// Package broker holds the NATS JetStream names the service uses, connects to
// NATS, and creates the streams and the consumer at startup when they are missing.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Names fixed by the brief, except EpisodeStream, which is ours.
const (
	EventStream   = "ANALYTICS_EVENTS"
	EventSubjects = "analytics.event.v1.>"
	Consumer      = "episoder"

	EpisodeStream     = "EPISODES"
	SettledSubjects   = "episode.settled.>"
	CorrectedSubjects = "episode.corrected.>"
)

// Consumer settings from the brief. Changing them must be noted in the README.
const (
	ackWait    = 30 * time.Second
	maxDeliver = 10
)

// dedupWindow is how long JetStream remembers a Nats-Msg-Id on the episode
// stream; an announcement published again within it is dropped (ADR-0004).
const dedupWindow = 2 * time.Minute

// Connect opens a NATS connection that keeps reconnecting for as long as the
// process runs, including when NATS is not up yet at startup.
func Connect(url string, log *slog.Logger) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.Name("panoptes"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "error", err.Error())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to nats: %w", err)
	}
	return nc, nil
}

// Provision makes sure the input stream, its consumer and a stream for the
// announcements exist, and returns the consumer to pull events from. Existing
// ones are used as they are, so a broker that someone else set up keeps its
// settings. It is safe to run from several instances at once, and a kill
// between its steps leaves no harm: the next start finds what was created and
// creates the rest.
func Provision(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	err := ensureStream(ctx, js, jetstream.StreamConfig{
		Name:      EventStream,
		Subjects:  []string{EventSubjects},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return nil, err
	}
	consumer, err := ensureConsumer(ctx, js, EventStream, jetstream.ConsumerConfig{
		Durable:    Consumer,
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    ackWait,
		MaxDeliver: maxDeliver,
	})
	if err != nil {
		return nil, err
	}
	if err := ensureEpisodeStream(ctx, js); err != nil {
		return nil, err
	}
	return consumer, nil
}

// ensureEpisodeStream gives each announcement subject pattern a stream. A
// pattern that some stream already covers in full is left to that stream; the
// rest go to EPISODES, which is ours, so it is created or extended. JetStream
// rejects overlapping streams, so a stream that covers only part of a pattern
// makes this fail, and startup keeps retrying with that error in the log.
func ensureEpisodeStream(ctx context.Context, js jetstream.JetStream) error {
	var subjects []string // every subject pattern of every stream
	streams := js.ListStreams(ctx)
	for info := range streams.Info() {
		subjects = append(subjects, info.Config.Subjects...)
	}
	if err := streams.Err(); err != nil {
		return fmt.Errorf("list streams: %w", err)
	}

	var missing []string
	for _, pattern := range []string{SettledSubjects, CorrectedSubjects} {
		if !slices.ContainsFunc(subjects, func(s string) bool { return covers(s, pattern) }) {
			missing = append(missing, pattern)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	s, err := js.Stream(ctx, EpisodeStream)
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       EpisodeStream,
			Subjects:   missing,
			Retention:  jetstream.LimitsPolicy,
			Storage:    jetstream.FileStorage,
			Duplicates: dedupWindow,
		})
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return errors.New("another instance created EPISODES at the same time; checking again")
		}
	case err == nil:
		cfg := s.CachedInfo().Config
		cfg.Subjects = append(cfg.Subjects, missing...)
		_, err = js.UpdateStream(ctx, cfg)
	}
	if err != nil {
		return fmt.Errorf("stream %s for %v: %w", EpisodeStream, missing, err)
	}
	return nil
}

// covers reports whether subject pattern p matches every subject that pattern
// t matches, for a t that ends in ">", such as "episode.settled.>". For that t,
// "episode.>", "*.settled.>" and ">" cover it; "episode.settled.site.>" does not.
func covers(p, t string) bool {
	pt, tt := strings.Split(p, "."), strings.Split(t, ".")
	for i, token := range pt {
		if token == ">" {
			return true
		}
		if i >= len(tt) || tt[i] == ">" {
			return false // t stays open here while p is narrower
		}
		if token != "*" && token != tt[i] {
			return false
		}
	}
	return false // without ">" p cannot cover t's open tail
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	_, err := js.Stream(ctx, cfg.Name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("look up stream %s: %w", cfg.Name, err)
	}
	_, err = js.CreateStream(ctx, cfg)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) { // in use: another instance won the race
		return fmt.Errorf("create stream %s: %w", cfg.Name, err)
	}
	return nil
}

func ensureConsumer(ctx context.Context, js jetstream.JetStream, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := js.Consumer(ctx, stream, cfg.Durable)
	if err == nil {
		return consumer, nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("look up consumer %s: %w", cfg.Durable, err)
	}
	consumer, err = js.CreateConsumer(ctx, stream, cfg)
	// A consumer created meanwhile with the same settings comes back without
	// an error; one with other settings is used as it is, like any existing one.
	if errors.Is(err, jetstream.ErrConsumerExists) {
		consumer, err = js.Consumer(ctx, stream, cfg.Durable)
	}
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", cfg.Durable, err)
	}
	return consumer, nil
}

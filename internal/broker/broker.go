// Package broker holds the NATS JetStream names the service uses, connects to
// NATS, and creates the streams and the consumer at startup when they are missing.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// duplicateWindow is how long JetStream remembers a Nats-Msg-Id on the episode
// stream; a republish within it is dropped (ADR-0004).
const duplicateWindow = 2 * time.Minute

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
// announcements exist. Existing ones are used as they are, so a broker that
// someone else set up keeps its settings. It is safe to run from several
// instances at once.
func Provision(ctx context.Context, js jetstream.JetStream) error {
	err := ensureStream(ctx, js, jetstream.StreamConfig{
		Name:      EventStream,
		Subjects:  []string{EventSubjects},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return err
	}
	err = ensureConsumer(ctx, js, EventStream, jetstream.ConsumerConfig{
		Durable:    Consumer,
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    ackWait,
		MaxDeliver: maxDeliver,
	})
	if err != nil {
		return err
	}

	// JetStream rejects a stream whose subjects overlap another stream's, so the
	// episode stream takes only the announcement subjects no stream covers yet.
	var missing []string
	for _, subjects := range []string{SettledSubjects, CorrectedSubjects} {
		covered, err := covered(ctx, js, subjects)
		if err != nil {
			return err
		}
		if !covered {
			missing = append(missing, subjects)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return ensureStream(ctx, js, jetstream.StreamConfig{
		Name:       EpisodeStream,
		Subjects:   missing,
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: duplicateWindow,
	})
}

// covered reports whether some stream already captures subjects such as
// "episode.settled.>", probed with one concrete subject.
func covered(ctx context.Context, js jetstream.JetStream, subjects string) (bool, error) {
	probe := subjects[:len(subjects)-1] + "site.camera"
	_, err := js.StreamNameBySubject(ctx, probe)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, jetstream.ErrStreamNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("look up the stream for %s: %w", probe, err)
	}
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

func ensureConsumer(ctx context.Context, js jetstream.JetStream, stream string, cfg jetstream.ConsumerConfig) error {
	_, err := js.Consumer(ctx, stream, cfg.Durable)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return fmt.Errorf("look up consumer %s: %w", cfg.Durable, err)
	}
	_, err = js.CreateConsumer(ctx, stream, cfg)
	if err != nil && !errors.Is(err, jetstream.ErrConsumerExists) { // exists: another instance won the race
		return fmt.Errorf("create consumer %s: %w", cfg.Durable, err)
	}
	return nil
}

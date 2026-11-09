package e2e

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStartupCreatesTheStreamsAndTheConsumer(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "ANALYTICS_EVENTS", "EPISODES")

	startReady(t)

	events := streamConfig(t, js, "ANALYTICS_EVENTS")
	if !slices.Equal(events.Subjects, []string{"analytics.event.v1.>"}) ||
		events.Retention != jetstream.WorkQueuePolicy || events.Storage != jetstream.FileStorage {
		t.Errorf("ANALYTICS_EVENTS: subjects %v, retention %v, storage %v; want analytics.event.v1.>, WorkQueue, File",
			events.Subjects, events.Retention, events.Storage)
	}
	consumer, err := js.Consumer(ctx(t), "ANALYTICS_EVENTS", "episoder")
	if err != nil {
		t.Fatalf("consumer episoder: %v", err)
	}
	c := consumer.CachedInfo().Config
	if c.Durable != "episoder" || c.AckPolicy != jetstream.AckExplicitPolicy ||
		c.AckWait != 30*time.Second || c.MaxDeliver != 10 {
		t.Errorf("episoder: durable %q, ack %v, AckWait %v, MaxDeliver %d; want episoder, explicit, 30s, 10",
			c.Durable, c.AckPolicy, c.AckWait, c.MaxDeliver)
	}
	episodes := streamConfig(t, js, "EPISODES")
	if !slices.Equal(episodes.Subjects, []string{"episode.settled.>", "episode.corrected.>"}) ||
		episodes.Retention != jetstream.LimitsPolicy || episodes.Storage != jetstream.FileStorage ||
		episodes.Duplicates != 2*time.Minute {
		t.Errorf("EPISODES: subjects %v, retention %v, storage %v, duplicate window %v; "+
			"want both announcement subjects, Limits, File, 2m",
			episodes.Subjects, episodes.Retention, episodes.Storage, episodes.Duplicates)
	}
}

func TestAnExistingEventStreamIsUsedAsItIs(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "ANALYTICS_EVENTS")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{
		Name:      "ANALYTICS_EVENTS",
		Subjects:  []string{"analytics.event.v1.>"},
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    time.Hour, // differs from what the service would create
	})
	if err != nil {
		t.Fatalf("create ANALYTICS_EVENTS: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "ANALYTICS_EVENTS") })

	startReady(t)

	if got := streamConfig(t, js, "ANALYTICS_EVENTS").MaxAge; got != time.Hour {
		t.Errorf("ANALYTICS_EVENTS MaxAge = %v, want the 1h it was created with", got)
	}
	if _, err := js.Consumer(ctx(t), "ANALYTICS_EVENTS", "episoder"); err != nil {
		t.Errorf("consumer episoder was not added to the existing stream: %v", err)
	}
}

func TestAnExistingConsumerIsUsedAsItIs(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "ANALYTICS_EVENTS")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{
		Name:      "ANALYTICS_EVENTS",
		Subjects:  []string{"analytics.event.v1.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("create ANALYTICS_EVENTS: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "ANALYTICS_EVENTS") })
	_, err = js.CreateConsumer(ctx(t), "ANALYTICS_EVENTS", jetstream.ConsumerConfig{
		Durable:    "episoder",
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    45 * time.Second, // both differ from what the service would create
		MaxDeliver: 3,
	})
	if err != nil {
		t.Fatalf("create consumer episoder: %v", err)
	}

	startReady(t)

	consumer, err := js.Consumer(ctx(t), "ANALYTICS_EVENTS", "episoder")
	if err != nil {
		t.Fatalf("consumer episoder: %v", err)
	}
	if c := consumer.CachedInfo().Config; c.AckWait != 45*time.Second || c.MaxDeliver != 3 {
		t.Errorf("episoder: AckWait %v, MaxDeliver %d; want the 45s and 3 it was created with", c.AckWait, c.MaxDeliver)
	}
}

func TestEPISODESGainsASubjectThatLostItsStream(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "EPISODES")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{Name: "EPISODES", Subjects: []string{"episode.corrected.>"}})
	if err != nil {
		t.Fatalf("create EPISODES: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "EPISODES") })

	startReady(t)

	got := streamConfig(t, js, "EPISODES").Subjects
	if !slices.Contains(got, "episode.settled.>") || !slices.Contains(got, "episode.corrected.>") {
		t.Errorf("EPISODES subjects = %v, want both announcement subjects", got)
	}
}

func TestTheEpisodeStreamTakesOnlyUncoveredSubjects(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "EPISODES")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{Name: "SUITE_SETTLED", Subjects: []string{"episode.settled.>"}})
	if err != nil {
		t.Fatalf("create SUITE_SETTLED: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "SUITE_SETTLED", "EPISODES") })

	startReady(t)

	if got := streamConfig(t, js, "EPISODES").Subjects; !slices.Equal(got, []string{"episode.corrected.>"}) {
		t.Errorf("EPISODES subjects = %v, want only episode.corrected.>", got)
	}
}

func TestNoEpisodeStreamWhenAnotherStreamCoversBothSubjects(t *testing.T) {
	js := jetStream(t)
	deleteStreams(t, js, "EPISODES")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{Name: "SUITE_EPISODES", Subjects: []string{"episode.>"}})
	if err != nil {
		t.Fatalf("create SUITE_EPISODES: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "SUITE_EPISODES") })

	startReady(t)

	if _, err := js.Stream(ctx(t), "EPISODES"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("EPISODES exists (err = %v), want it not created", err)
	}
}

// jetStream connects to the test NATS server.
func jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	requireStack(t)
	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		t.Fatalf("connect to nats: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func streamConfig(t *testing.T, js jetstream.JetStream, name string) jetstream.StreamConfig {
	t.Helper()
	s, err := js.Stream(ctx(t), name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	return s.CachedInfo().Config
}

func deleteStreams(t *testing.T, js jetstream.JetStream, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := js.DeleteStream(ctx(t), name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("delete stream %s: %v", name, err)
		}
	}
}

// ctx is a short deadline for one broker call.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

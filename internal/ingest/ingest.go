// Package ingest pulls events from the episoder consumer, stores each one and
// acknowledges it only after its transaction has committed.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"panoptes/internal/event"
	"panoptes/internal/store"
)

// workers is how many messages are processed at once. Events of one episode
// key wait for each other on the key's lock, so more workers only help when
// many keys are busy.
const workers = 4

// txTimeout bounds one ingest transaction. A transaction that has started
// runs to the end even when shutdown begins, so that its message is
// acknowledged rather than redelivered; the timeout keeps a stuck database
// from holding shutdown forever.
const txTimeout = 10 * time.Second

// retryDelay is how long a message whose transaction failed waits before it
// is delivered again.
const retryDelay = time.Second

// Run processes messages until ctx ends, then waits for the messages already
// in hand to finish.
func Run(ctx context.Context, consumer jetstream.Consumer, st *store.Store, log *slog.Logger) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { work(ctx, consumer, st, log) })
	}
	wg.Wait()
}

// work pulls one message at a time, so a worker never holds messages it has
// not started on: a message waiting in a local buffer would sit out its
// AckWait if the process died.
func work(ctx context.Context, consumer jetstream.Consumer, st *store.Store, log *slog.Logger) {
	for {
		msg, err := consumer.Next(jetstream.FetchContext(ctx))
		switch {
		case ctx.Err() != nil:
			return
		case errors.Is(err, nats.ErrTimeout): // nothing arrived during the pull
			continue
		case err != nil:
			log.Warn("fetch failed", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
			continue
		}
		handle(ctx, msg, st, log)
	}
}

func handle(ctx context.Context, msg jetstream.Msg, st *store.Store, log *slog.Logger) {
	ev, err := event.Decode(msg.Data())
	if err != nil {
		// It can never become an event, so redelivering it would change
		// nothing: acknowledge it and move on.
		log.Warn("invalid event", "reason", err.Error(), "subject", msg.Subject())
		ack(msg, log)
		return
	}

	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txTimeout)
	defer cancel()
	out, err := st.Ingest(txCtx, ev)
	if err != nil {
		// The transaction rolled back, so nothing was stored. Ask for the
		// message again; if even the nak is lost, AckWait brings it back.
		log.Error("ingest failed", "event_id", ev.ID, "error", err.Error())
		if err := msg.NakWithDelay(retryDelay); err != nil {
			log.Warn("nak failed", "event_id", ev.ID, "error", err.Error())
		}
		return
	}

	// The transaction has committed; only now is the message acknowledged. If
	// the process dies here or the ack is lost, the redelivery is a duplicate
	// and changes nothing.
	ack(msg, log)
	log.Debug("event ingested",
		"site_id", ev.Key.SiteID, "camera_id", ev.Key.CameraID, "kind", ev.Key.Kind,
		"event_id", ev.ID, "duplicate", out.Duplicate, "episode_id", out.EpisodeID.String())
}

func ack(msg jetstream.Msg, log *slog.Logger) {
	if err := msg.Ack(); err != nil {
		log.Warn("ack failed", "subject", msg.Subject(), "error", err.Error())
	}
}

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

	"panoptes/internal/crash"
	"panoptes/internal/event"
	"panoptes/internal/metrics"
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

// backoff is how long a worker waits after a failed fetch or a failed
// transaction before it pulls again.
const backoff = time.Second

// Run processes messages until ctx ends, then waits for the messages already
// in hand to finish.
func Run(ctx context.Context, consumer jetstream.Consumer, st *store.Store, log *slog.Logger) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { work(ctx, consumer, st, log) })
	}
	wg.Wait()
}

// pullWait is how long one pull request waits on the server for a message.
const pullWait = time.Second

// work pulls one message at a time, so a worker never holds messages it has
// not started on: a message waiting in a local buffer would sit out its
// AckWait if the process died.
//
// A pull is never cancelled midway. When shutdown begins, the worker lets its
// open pull run out, at most pullWait, and handles a message that arrives
// meanwhile. Cancelling the pull on the client would leave the request open
// on the server, which could then hand it a message nobody reads; that
// message would wait out AckWait before anyone else saw it.
func work(ctx context.Context, consumer jetstream.Consumer, st *store.Store, log *slog.Logger) {
	for ctx.Err() == nil {
		msg, err := consumer.Next(jetstream.FetchMaxWait(pullWait))
		switch {
		case err == nil:
			// Handled even when shutdown has just begun: the message is in
			// hand, and dropping it would cost it AckWait and a delivery.
			if !handle(ctx, msg, st, log) && !pause(ctx, st, log) {
				return
			}
		case errors.Is(err, nats.ErrTimeout): // nothing arrived during the pull
		default:
			log.Warn("fetch failed", "error", err.Error())
			if !sleep(ctx, backoff) {
				return
			}
		}
	}
}

// pause stops the worker from fetching until Postgres answers again, so that
// messages are not pulled only to fail: each delivery counts towards
// MaxDeliver, and a worker that kept fetching through an outage would use
// them all up. The message that failed comes back after AckWait. Meanwhile
// /readyz reports 503, because it pings Postgres too. pause reports false
// when ctx ended first.
func pause(ctx context.Context, st *store.Store, log *slog.Logger) bool {
	wait := backoff
	for paused := false; ; paused = true {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := st.Ping(pingCtx)
		cancel()
		if err == nil {
			if paused {
				log.Info("postgres answers again; ingest resumed")
			}
			return true
		}
		if !paused {
			log.Warn("ingest paused until postgres answers", "error", err.Error())
		}
		if !sleep(ctx, wait) {
			return false
		}
		wait = min(wait*2, 5*time.Second)
	}
}

// sleep waits for d and reports false when ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// handle processes one message and reports whether it is done with it.
func handle(ctx context.Context, msg jetstream.Msg, st *store.Store, log *slog.Logger) bool {
	ev, err := event.Decode(msg.Data())
	if err != nil {
		skipInvalid(msg, err, log)
		return true
	}
	if ev.Subject() != msg.Subject() {
		log.Warn("subject disagrees with payload; the payload wins",
			"subject", msg.Subject(), "event_id", ev.ID, "payload_subject", ev.Subject())
	}

	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txTimeout)
	defer cancel()
	started := time.Now()
	out, err := st.Ingest(txCtx, ev)
	if errors.Is(err, store.ErrRejected) {
		skipInvalid(msg, err, log)
		return true
	}
	if err != nil {
		// The transaction rolled back, so nothing was stored. The message is
		// left unacknowledged and comes back after AckWait. A nak would bring
		// it back sooner, but each delivery counts towards MaxDeliver, so a
		// short nak delay would use up all ten within seconds of a database
		// outage and lose the event.
		log.Error("ingest failed", "event_id", ev.ID, "error", err.Error())
		return false
	}

	// The transaction has committed; only now is the message acknowledged. If
	// the process dies here or the ack is lost, the redelivery is a duplicate
	// and changes nothing.
	crash.Point(crash.IngestAfterCommit)
	ack(msg, log)
	metrics.IngestDuration.Observe(time.Since(started).Seconds())
	if out.Duplicate {
		metrics.IngestedEvents.WithLabelValues("duplicate").Inc()
		log.Debug("duplicate event", "event_id", ev.ID)
		return true
	}
	metrics.IngestedEvents.WithLabelValues("new").Inc()
	if out.Revision > 0 {
		metrics.EpisodeCorrections.Inc()
		log.Info("episode corrected", "episode_id", out.EpisodeID.String(), "event_id", ev.ID,
			"site_id", ev.Key.SiteID, "camera_id", ev.Key.CameraID, "kind", ev.Key.Kind, "revision", out.Revision)
		return true
	}
	log.Debug("event ingested",
		"site_id", ev.Key.SiteID, "camera_id", ev.Key.CameraID, "kind", ev.Key.Kind,
		"event_id", ev.ID, "episode_id", out.EpisodeID.String())
	return true
}

// skipInvalid acknowledges, counts and logs a message that can never become
// an event. Redelivering it would change nothing, so it must not hold up the
// stream or use up its deliveries. Nothing is stored for it, so a kill at any
// point here loses nothing: before the ack the message comes back and is
// skipped again, and the counter, which is per process, counts it once more
// in whichever instance gets it.
func skipInvalid(msg jetstream.Msg, reason error, log *slog.Logger) {
	ack(msg, log)
	metrics.InvalidEvents.Inc()
	payload := msg.Data()
	if len(payload) > 256 {
		payload = payload[:256]
	}
	log.Warn("invalid event", "reason", reason.Error(), "subject", msg.Subject(), "payload", string(payload))
}

func ack(msg jetstream.Msg, log *slog.Logger) {
	if err := msg.Ack(); err != nil {
		log.Warn("ack failed", "subject", msg.Subject(), "error", err.Error())
	}
}

// Package relay publishes the outbox's announcements to JetStream.
package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"panoptes/internal/store"
)

const (
	// interval is how often the relay looks for new announcements when the
	// outbox was empty.
	interval = 100 * time.Millisecond
	// batch is how many announcements one transaction publishes.
	batch = 50
	// backoff is the wait after a failed publish or a database error.
	backoff = time.Second
	// txTimeout bounds one batch, which runs to the end even when shutdown
	// begins.
	txTimeout = 10 * time.Second
)

// Run publishes announcements until ctx ends. Every instance runs one; the
// outbox's row locks keep them from publishing the same row at once.
func Run(ctx context.Context, st *store.Store, js jetstream.JetStream, log *slog.Logger) {
	publish := func(ctx context.Context, a store.Announcement) error {
		// Publish waits for JetStream's acknowledgement, so a row is marked
		// published only once the stream holds it.
		_, err := js.Publish(ctx, a.Subject, a.Payload, jetstream.WithMsgID(a.MsgID))
		if err != nil {
			log.Warn("publish failed", "subject", a.Subject, "msg_id", a.MsgID, "error", err.Error())
		}
		return err
	}
	for {
		txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txTimeout)
		n, err := st.PublishOutbox(txCtx, batch, publish)
		cancel()
		wait := interval
		switch {
		case err != nil:
			wait = backoff
			log.Warn("relay failed", "published", n, "error", err.Error())
		case n == batch:
			wait = 0 // more may be waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

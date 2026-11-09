// Package app wires the modules together and runs them until the context ends.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"panoptes/internal/api"
	"panoptes/internal/broker"
	"panoptes/internal/config"
	"panoptes/internal/correlation"
	"panoptes/internal/ingest"
	"panoptes/internal/relay"
	"panoptes/internal/settle"
	"panoptes/internal/store"
)

// shutdownTimeout bounds the graceful shutdown. Compose gives the container 30
// seconds before it kills it, so this leaves room to spare.
const shutdownTimeout = 20 * time.Second

type app struct {
	store    *store.Store
	nc       *nats.Conn
	started  atomic.Bool // startup work is done
	stopping atomic.Bool // shutdown has begun
}

// Run starts the service and blocks until ctx is cancelled, then shuts down
// gracefully. It returns an error only when the service cannot run at all.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := store.Open(cfg.DatabaseURL, store.Rules{
		Settings:    correlation.Settings{QuietWindow: cfg.QuietWindow, MaxDuration: cfg.MaxDuration},
		SettleGrace: cfg.SettleGrace,
	})
	if err != nil {
		return err
	}
	defer st.Close()
	nc, err := broker.Connect(cfg.NATSURL, log)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	a := &app{store: st, nc: nc}

	// The HTTP server starts first, so /healthz answers while the rest starts up
	// and /readyz can say what the service is still waiting for.
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := &http.Server{Handler: api.NewHandler(st, a.ready, log), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("http server listening", "addr", ln.Addr().String())

	// The loops run on their own context, so that a failed HTTP server stops
	// them the same way a SIGTERM does.
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	var loops sync.WaitGroup

	// Startup work retries until it succeeds, because Postgres or NATS may still
	// be starting. A SIGTERM during startup ends the retries and shuts down cleanly.
	var consumer jetstream.Consumer
	provision := func(ctx context.Context) (err error) {
		consumer, err = broker.Provision(ctx, js)
		return err
	}
	if retry(loopCtx, log, "migrate the database", st.Migrate) == nil &&
		retry(loopCtx, log, "provision the streams", provision) == nil {
		loops.Go(func() { ingest.Run(loopCtx, consumer, st, log) })
		loops.Go(func() { settle.Run(loopCtx, st, cfg.QuietWindow, log) })
		loops.Go(func() { relay.Run(loopCtx, st, js, log) })
		a.started.Store(true)
		log.Info("ready")
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = fmt.Errorf("http server: %w", err)
	}

	// Shutdown, all within shutdownTimeout: /readyz turns 503, the loops stop
	// taking new work and finish what they hold (an ingest transaction and its
	// ack, a settle, a relay batch), then the HTTP server stops. The deferred
	// closes of NATS and the pool run last; closing NATS flushes the final
	// acks.
	a.stopping.Store(true)
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	stopLoops()
	finished := make(chan struct{})
	go func() {
		loops.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-shutdownCtx.Done():
		return fmt.Errorf("work in flight did not finish within %v", shutdownTimeout)
	}
	if runErr != nil {
		return runErr
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}

// ready is the readiness check behind /readyz.
func (a *app) ready(ctx context.Context) error {
	switch {
	case a.stopping.Load():
		return errors.New("shutting down")
	case !a.started.Load():
		return errors.New("starting up")
	}
	if err := a.store.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if status := a.nc.Status(); status != nats.CONNECTED {
		return fmt.Errorf("nats: %s", status)
	}
	return nil
}

// retry runs fn until it succeeds or ctx ends, waiting a little longer after
// each failure. It returns ctx.Err() when ctx ends first.
func retry(ctx context.Context, log *slog.Logger, what string, fn func(context.Context) error) error {
	wait := 200 * time.Millisecond
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("retrying", "step", what, "error", err.Error(), "in", wait.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}

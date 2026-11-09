// Package app wires the modules together and runs them until the context ends.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"panoptes/internal/api"
	"panoptes/internal/config"
	"panoptes/internal/store"
)

// shutdownTimeout bounds the graceful shutdown. Compose gives the container 30
// seconds before it kills it, so this leaves room to spare.
const shutdownTimeout = 20 * time.Second

type app struct {
	cfg      config.Config
	log      *slog.Logger
	store    *store.Store
	started  atomic.Bool // startup work is done
	stopping atomic.Bool // shutdown has begun
}

// Run starts the service and blocks until ctx is cancelled, then shuts down
// gracefully. It returns an error only when the service cannot run at all.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := store.Open(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	a := &app{cfg: cfg, log: log, store: st}

	// The HTTP server starts first, so /healthz answers while the rest starts up
	// and /readyz can say what the service is still waiting for.
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := &http.Server{Handler: api.NewHandler(a.ready), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("http server listening", "addr", ln.Addr().String())

	// Startup work retries until it succeeds, because Postgres may still be
	// starting. A SIGTERM during startup ends the retries and shuts down cleanly.
	if err := retry(ctx, log, "migrate the database", st.Migrate); err == nil {
		a.started.Store(true)
		log.Info("ready")
	}

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	}

	a.stopping.Store(true)
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
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

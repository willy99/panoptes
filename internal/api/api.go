// Package api serves the service's HTTP endpoints: the episode queries,
// liveness and readiness. Metrics join it in a later ticket.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"panoptes/internal/store"
)

// ReadinessCheck reports why the service cannot take traffic yet, or nil when it can.
type ReadinessCheck func(ctx context.Context) error

// readinessTimeout bounds one /readyz call, so a hanging dependency answers 503
// instead of holding the probe open.
const readinessTimeout = 2 * time.Second

// NewHandler returns the HTTP handler for every endpoint the service exposes.
func NewHandler(st *store.Store, ready ReadinessCheck, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /episodes", listEpisodes(st, log))
	mux.HandleFunc("GET /episodes/{id}", getEpisode(st, log))

	// Liveness: the process is running and serving HTTP. It never checks
	// dependencies, so a database outage does not get the process restarted.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Readiness: the service can do its job right now.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "reason": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	// Attributes go out as the device sent them; HTML escaping would turn
	// "<" into "\u003c". The API serves JSON, never HTML.
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// writeError sends the API's error shape: {"error": "<message>"}.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// internalError logs the cause and tells the client only that it failed.
func internalError(w http.ResponseWriter, log *slog.Logger, err error) {
	log.Error("request failed", "error", err.Error())
	writeError(w, http.StatusInternalServerError, "internal error")
}

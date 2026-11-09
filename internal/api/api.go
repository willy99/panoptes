// Package api serves the service's HTTP endpoints. For now that is liveness and
// readiness; the episode queries and metrics join it in later tickets.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ReadinessCheck reports why the service cannot take traffic yet, or nil when it can.
type ReadinessCheck func(ctx context.Context) error

// readinessTimeout bounds one /readyz call, so a hanging dependency answers 503
// instead of holding the probe open.
const readinessTimeout = 2 * time.Second

// NewHandler returns the HTTP handler for every endpoint the service exposes.
func NewHandler(ready ReadinessCheck) http.Handler {
	mux := http.NewServeMux()

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
	_ = json.NewEncoder(w).Encode(body)
}

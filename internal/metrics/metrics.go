// Package metrics holds the service's Prometheus metrics. They live in one
// registry of their own, which /metrics serves.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var registry = prometheus.NewRegistry()

var factory = promauto.With(registry)

// InvalidEvents counts messages that could not become an event. Its name and
// its lack of labels are the brief's contract.
var InvalidEvents = factory.NewCounter(prometheus.CounterOpts{
	Name: "episoder_invalid_events_total",
	Help: "Messages acknowledged and skipped because they could not become an event.",
})

// Handler serves the metrics in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

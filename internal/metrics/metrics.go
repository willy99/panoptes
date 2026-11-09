// Package metrics holds the service's Prometheus metrics. They live in one
// registry of their own, which /metrics serves together with the Go runtime
// and process metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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

// IngestedEvents counts valid events by whether they were new or duplicates.
var IngestedEvents = factory.NewCounterVec(prometheus.CounterOpts{
	Name: "panoptes_ingested_events_total",
	Help: "Valid events ingested, by result: new or duplicate.",
}, []string{"result"})

// IngestDuration measures one ingest transaction, lock wait included.
var IngestDuration = factory.NewHistogram(prometheus.HistogramOpts{
	Name:    "panoptes_ingest_duration_seconds",
	Help:    "Time to store one valid event and update its episodes, from begin to commit.",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to about 2s
})

// EpisodesSettled counts settles by close reason.
var EpisodesSettled = factory.NewCounterVec(prometheus.CounterOpts{
	Name: "panoptes_episodes_settled_total",
	Help: "Episodes settled, by close reason.",
}, []string{"close_reason"})

// EpisodeCorrections counts late events that joined settled episodes.
var EpisodeCorrections = factory.NewCounter(prometheus.CounterOpts{
	Name: "panoptes_episode_corrections_total",
	Help: "Late events that joined a settled episode and raised its revision.",
})

// OutboxPending is the number of announcements waiting to be published, as the
// relay last counted it.
var OutboxPending = factory.NewGauge(prometheus.GaugeOpts{
	Name: "panoptes_outbox_pending",
	Help: "Announcements in the outbox not yet published, sampled by the relay.",
})

// PublishFailures counts announcements JetStream did not acknowledge.
var PublishFailures = factory.NewCounter(prometheus.CounterOpts{
	Name: "panoptes_publish_failures_total",
	Help: "Announcements whose publish failed; they stay in the outbox and are retried.",
})

func init() {
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	// Every label value shows from the start, at 0, so that a rate over it
	// does not begin with a gap.
	for _, result := range []string{"new", "duplicate"} {
		IngestedEvents.WithLabelValues(result)
	}
	for _, reason := range []string{"quiet", "max_duration"} {
		EpisodesSettled.WithLabelValues(reason)
	}
}

// Handler serves the metrics in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

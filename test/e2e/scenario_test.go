package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// published counts the messages every scenario has published. Tests run one
// at a time, so the deliveries the consumer makes during a test answer to it.
var published uint64

// scenario is one test's own corner of the world: a site no other test uses,
// so a test sees only its own episodes, and event ids that no other test uses.
type scenario struct {
	t    *testing.T
	js   jetstream.JetStream
	site string
	// The consumer's delivery count and the published count when the
	// scenario began, so that it can tell when Panoptes has handled every
	// message published since.
	delivered, published uint64
}

// newScenario needs a running instance, which has created the consumer.
func newScenario(t *testing.T) *scenario {
	t.Helper()
	js := jetStream(t)
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return &scenario{
		t:         t,
		js:        js,
		site:      "site-" + hex.EncodeToString(b),
		delivered: consumerInfo(t, js).Delivered.Consumer,
		published: published,
	}
}

// testEvent is an event as a device publishes it.
type testEvent struct {
	ID         string          `json:"event_id"`
	Site       string          `json:"site_id"`
	Camera     string          `json:"camera_id"`
	Kind       string          `json:"kind"`
	Severity   string          `json:"severity"`
	OccurredAt string          `json:"occurred_at"`
	Attributes json.RawMessage `json:"attributes,omitempty"`
}

// ms makes device-time offsets read as timelines: 300 * ms.
const ms = time.Millisecond

// event returns event number n of the scenario: a person_detected at cam-1,
// at device time offset after 10:00:00. Tests change the fields they are about.
func (s *scenario) event(n int, severity string, offset time.Duration) testEvent {
	return testEvent{
		ID:         fmt.Sprintf("%s-%03d", s.site, n),
		Site:       s.site,
		Camera:     "cam-1",
		Kind:       "person_detected",
		Severity:   severity,
		OccurredAt: at(offset),
	}
}

// at renders the device time offset after 2026-08-04T10:00:00Z, in the format
// the API returns.
func at(offset time.Duration) string {
	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)
	return base.Add(offset).Format("2006-01-02T15:04:05.000Z")
}

// publish sends the events in order, each on the subject its own fields name.
func (s *scenario) publish(events ...testEvent) {
	s.t.Helper()
	for _, e := range events {
		// Without HTML escaping, which would turn "<" in the attributes into
		// "\u003c" before the service ever sees them.
		var payload bytes.Buffer
		enc := json.NewEncoder(&payload)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(e); err != nil {
			s.t.Fatalf("encode event: %v", err)
		}
		s.publishRaw("analytics.event.v1."+e.Site+"."+e.Camera+"."+e.Kind, payload.Bytes())
	}
}

// publishRaw sends one message exactly as given.
func (s *scenario) publishRaw(subject string, payload []byte) {
	s.t.Helper()
	if _, err := s.js.Publish(ctx(s.t), subject, payload); err != nil {
		s.t.Fatalf("publish on %s: %v", subject, err)
	}
	published++
}

// waitDrained waits until the episoder consumer has nothing left to deliver
// and nothing delivered but unacknowledged. Panoptes acknowledges an event
// only after its transaction commits, so by then every published event is
// visible through the API. It then checks that every message was delivered
// exactly once: a message whose transaction failed and came back would leave
// the end state looking right.
func (s *scenario) waitDrained() {
	s.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info := consumerInfo(s.t, s.js)
		if info.NumPending == 0 && info.NumAckPending == 0 {
			deliveries, messages := info.Delivered.Consumer-s.delivered, published-s.published
			if deliveries != messages {
				s.t.Fatalf("episoder made %d deliveries for %d published messages", deliveries, messages)
			}
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("episoder still has %d pending and %d unacknowledged messages after 15s",
				info.NumPending, info.NumAckPending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func consumerInfo(t *testing.T, js jetstream.JetStream) *jetstream.ConsumerInfo {
	t.Helper()
	consumer, err := js.Consumer(ctx(t), "ANALYTICS_EVENTS", "episoder")
	if err != nil {
		t.Fatalf("consumer episoder: %v", err)
	}
	info, err := consumer.Info(ctx(t))
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	return info
}

// episodeJSON is the brief's episode object.
type episodeJSON struct {
	ID           string  `json:"id"`
	SiteID       string  `json:"site_id"`
	CameraID     string  `json:"camera_id"`
	Kind         string  `json:"kind"`
	Status       string  `json:"status"`
	FirstSeen    string  `json:"first_seen"`
	LastSeen     string  `json:"last_seen"`
	EventCount   int     `json:"event_count"`
	PeakSeverity string  `json:"peak_severity"`
	CloseReason  *string `json:"close_reason"`
	SettledAt    *string `json:"settled_at"`
}

// eventJSON is one entry of the events list of GET /episodes/{id}.
type eventJSON struct {
	EventID    string          `json:"event_id"`
	Severity   string          `json:"severity"`
	OccurredAt string          `json:"occurred_at"`
	Attributes json.RawMessage `json:"attributes"`
}

// episodes returns the scenario's episodes from GET /episodes.
func (s *scenario) episodes(p *proc) []episodeJSON {
	s.t.Helper()
	var body struct {
		Episodes []episodeJSON `json:"episodes"`
	}
	p.getJSON("/episodes?site_id="+s.site, http.StatusOK, &body)
	return body.Episodes
}

// episode returns an episode and its events from GET /episodes/{id}.
func (s *scenario) episode(p *proc, id string) (episodeJSON, []eventJSON) {
	s.t.Helper()
	var body struct {
		Episode episodeJSON `json:"episode"`
		Events  []eventJSON `json:"events"`
	}
	p.getJSON("/episodes/"+id, http.StatusOK, &body)
	return body.Episode, body.Events
}

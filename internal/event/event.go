// Package event defines an event as the service stores it, and decodes one
// from the payload of a message on the input stream.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// EpisodeKey is the site, camera and kind that events must share to belong to
// the same episode.
type EpisodeKey struct {
	SiteID   string
	CameraID string
	Kind     string
}

// Severity is how important an event is. Its value is its rank, so severities
// compare as numbers, never as text: "warning" sorts after "critical" as a
// string, but Warning < Critical.
type Severity int

const (
	Info Severity = iota + 1
	Notice
	Warning
	Critical
)

var severityNames = map[Severity]string{
	Info:     "info",
	Notice:   "notice",
	Warning:  "warning",
	Critical: "critical",
}

func (s Severity) String() string { return severityNames[s] }

func parseSeverity(name string) (Severity, bool) {
	for s, n := range severityNames {
		if n == name {
			return s, true
		}
	}
	return 0, false
}

// kinds are the detection kinds the brief lists.
var kinds = map[string]bool{
	"person_detected": true,
	"loitering":       true,
	"line_crossed":    true,
	"tamper":          true,
}

// Event is one detection, decoded and validated.
type Event struct {
	ID         string
	Key        EpisodeKey
	Severity   Severity
	OccurredAt time.Time       // device time, in UTC, cut to the microseconds Postgres keeps
	Attributes json.RawMessage // exactly as the device sent it
}

// required are the payload fields that must be non-empty strings.
var required = []string{"event_id", "site_id", "camera_id", "kind", "severity", "occurred_at"}

// Decode turns a message payload into an event, or returns the reason it
// cannot be one.
func Decode(payload []byte) (Event, error) {
	// A map rather than a struct, because encoding/json matches struct fields
	// without regard to case: "Event_ID" would pass for "event_id".
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return Event{}, fmt.Errorf("not a JSON object: %w", err)
	}
	s := map[string]string{}
	for _, name := range required {
		raw, ok := fields[name]
		if !ok || string(raw) == "null" {
			return Event{}, fmt.Errorf("%s is missing", name)
		}
		var v string
		if err := json.Unmarshal(raw, &v); err != nil {
			return Event{}, fmt.Errorf("%s is not a string", name)
		}
		if v == "" {
			return Event{}, fmt.Errorf("%s is empty", name)
		}
		s[name] = v
	}

	if !kinds[s["kind"]] {
		return Event{}, fmt.Errorf("unknown kind %q", s["kind"])
	}
	severity, ok := parseSeverity(s["severity"])
	if !ok {
		return Event{}, fmt.Errorf("unknown severity %q", s["severity"])
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, s["occurred_at"])
	if err != nil {
		return Event{}, fmt.Errorf("occurred_at %q is not an RFC 3339 timestamp", s["occurred_at"])
	}

	attributes := fields["attributes"]
	if attributes == nil {
		attributes = json.RawMessage(`{}`)
	}
	return Event{
		ID:         s["event_id"],
		Key:        EpisodeKey{SiteID: s["site_id"], CameraID: s["camera_id"], Kind: s["kind"]},
		Severity:   severity,
		OccurredAt: occurredAt.UTC().Truncate(time.Microsecond),
		Attributes: attributes,
	}, nil
}

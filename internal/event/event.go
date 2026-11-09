// Package event defines an event as the service stores it, and decodes one
// from the payload of a message on the input stream.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Key is the episode key: the site, camera and kind that events must share to
// belong to the same episode.
type Key struct {
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

// ParseSeverity returns the severity with the given name.
func ParseSeverity(name string) (Severity, bool) {
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
	Key        Key
	Severity   Severity
	OccurredAt time.Time       // device time, in UTC, cut to the microseconds Postgres keeps
	Attributes json.RawMessage // exactly as the device sent it
}

// Decode turns a message payload into an event, or returns the reason it
// cannot be one.
func Decode(payload []byte) (Event, error) {
	// A map rather than a struct, because encoding/json matches struct fields
	// without regard to case: "Event_ID" would pass for "event_id".
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return Event{}, fmt.Errorf("not a JSON object: %w", err)
	}

	var problems []string
	str := func(name string) string {
		raw, ok := fields[name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s is missing", name))
			return ""
		}
		var s string
		switch {
		case string(raw) == "null":
			problems = append(problems, fmt.Sprintf("%s is null", name))
		case json.Unmarshal(raw, &s) != nil:
			problems = append(problems, fmt.Sprintf("%s is not a string", name))
		case s == "":
			problems = append(problems, fmt.Sprintf("%s is empty", name))
		}
		return s
	}
	ev := Event{
		ID:  str("event_id"),
		Key: Key{SiteID: str("site_id"), CameraID: str("camera_id"), Kind: str("kind")},
	}
	severity := str("severity")
	occurredAt := str("occurred_at")
	if len(problems) > 0 {
		return Event{}, errors.New(strings.Join(problems, "; "))
	}

	if !kinds[ev.Key.Kind] {
		problems = append(problems, fmt.Sprintf("kind %q is not one of person_detected, loitering, line_crossed, tamper", ev.Key.Kind))
	}
	var ok bool
	if ev.Severity, ok = ParseSeverity(severity); !ok {
		problems = append(problems, fmt.Sprintf("severity %q is not one of info, notice, warning, critical", severity))
	}
	t, err := time.Parse(time.RFC3339Nano, occurredAt)
	if err != nil {
		problems = append(problems, fmt.Sprintf("occurred_at %q is not an RFC 3339 timestamp", occurredAt))
	}
	ev.OccurredAt = t.UTC().Truncate(time.Microsecond)
	if len(problems) > 0 {
		return Event{}, errors.New(strings.Join(problems, "; "))
	}

	ev.Attributes = fields["attributes"]
	if ev.Attributes == nil {
		ev.Attributes = json.RawMessage(`{}`)
	}
	return ev, nil
}

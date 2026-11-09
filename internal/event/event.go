// Package event defines an event as the service stores it, and decodes one
// from the payload of a message on the input stream.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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

// Subject is the subject the event belongs on. Its tokens come from the
// payload, which wins when the subject it arrived on disagrees.
func (e Event) Subject() string {
	return "analytics.event.v1." + e.Key.SiteID + "." + e.Key.CameraID + "." + e.Key.Kind
}

// required are the payload fields that must be non-empty strings.
var required = []string{"event_id", "site_id", "camera_id", "kind", "severity", "occurred_at"}

// maxIDLength caps event_id, site_id and camera_id, so that they always fit an
// index entry; Postgres refuses entries of more than about 2.7 kB.
const maxIDLength = 255

// rfc3339 is the exact shape RFC 3339 allows. Go's parser alone is both
// stricter (it refuses a lowercase t or z) and looser (it takes a comma
// before the fraction).
var rfc3339 = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$`)

// Decode turns a message payload into an event, or returns the reason it
// cannot be one.
func Decode(payload []byte) (Event, error) {
	// Postgres stores only valid UTF-8, and encoding/json would quietly
	// replace bad bytes inside strings but keep them inside attributes.
	if !utf8.Valid(payload) {
		return Event{}, errors.New("not valid UTF-8")
	}
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
		if strings.ContainsRune(v, 0) {
			// Postgres text cannot hold U+0000.
			return Event{}, fmt.Errorf("%s contains U+0000", name)
		}
		s[name] = v
	}
	for _, name := range []string{"event_id", "site_id", "camera_id"} {
		if len(s[name]) > maxIDLength {
			return Event{}, fmt.Errorf("%s is longer than %d bytes", name, maxIDLength)
		}
	}
	// Every stored episode must be announceable on a subject built from these.
	for _, name := range []string{"site_id", "camera_id"} {
		if strings.ContainsFunc(s[name], notSubjectToken) {
			return Event{}, fmt.Errorf("%s %q cannot be a NATS subject token", name, s[name])
		}
	}

	if !kinds[s["kind"]] {
		return Event{}, fmt.Errorf("unknown kind %q", s["kind"])
	}
	severity, ok := parseSeverity(s["severity"])
	if !ok {
		return Event{}, fmt.Errorf("unknown severity %q", s["severity"])
	}
	occurredAt, err := ParseTime(s["occurred_at"])
	if err != nil {
		return Event{}, fmt.Errorf("occurred_at: %w", err)
	}

	attributes := fields["attributes"]
	if attributes == nil {
		attributes = json.RawMessage(`{}`)
	}
	return Event{
		ID:         s["event_id"],
		Key:        EpisodeKey{SiteID: s["site_id"], CameraID: s["camera_id"], Kind: s["kind"]},
		Severity:   severity,
		OccurredAt: occurredAt,
		Attributes: attributes,
	}, nil
}

// ParseTime parses an RFC 3339 timestamp into UTC, cut to the microseconds
// Postgres keeps.
func ParseTime(s string) (time.Time, error) {
	if !rfc3339.MatchString(s) {
		return time.Time{}, fmt.Errorf("%q is not an RFC 3339 timestamp", s)
	}
	t, err := time.Parse(time.RFC3339Nano, strings.ToUpper(s))
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not a valid time: %w", s, err)
	}
	return t.UTC().Truncate(time.Microsecond), nil
}

// IsKind reports whether kind is one of the brief's detection kinds.
func IsKind(kind string) bool { return kinds[kind] }

// notSubjectToken reports the runes a NATS subject token cannot contain.
func notSubjectToken(r rune) bool {
	return r == '.' || r == '*' || r == '>' || unicode.IsSpace(r)
}

// Package contract holds the JSON shapes the brief fixes, because the
// grading suite reads them: the episode object of the HTTP API, the same
// object in the announcements, and the time format they share.
package contract

import (
	"bytes"
	"encoding/json"
	"time"
)

// Fields are the episode object's fields other than its id. The API and the
// announcements name the id differently, and embed these.
type Fields struct {
	SiteID       string  `json:"site_id"`
	CameraID     string  `json:"camera_id"`
	Kind         string  `json:"kind"`
	Status       string  `json:"status"`
	FirstSeen    string  `json:"first_seen"`
	LastSeen     string  `json:"last_seen"`
	EventCount   int     `json:"event_count"`
	PeakSeverity string  `json:"peak_severity"`
	CloseReason  *string `json:"close_reason"` // null while open
	SettledAt    *string `json:"settled_at"`   // null while open
}

// Episode is the episode object of GET /episodes and GET /episodes/{id}.
type Episode struct {
	ID string `json:"id"`
	Fields
}

// Announcement is the episode object as episode.settled and
// episode.corrected carry it: episode_id in place of id. A correction adds
// the revision; a settled announcement has no other fields.
type Announcement struct {
	EpisodeID string `json:"episode_id"`
	Fields
	Revision *int `json:"revision,omitempty"`
}

// FormatTime renders a time in UTC with a Z suffix and milliseconds, as the
// brief's examples do, or with microseconds when it has digits below the
// millisecond, so that nothing Postgres stores is lost.
func FormatTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()%int(time.Millisecond) == 0 {
		return t.Format("2006-01-02T15:04:05.000Z")
	}
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// Marshal encodes v as JSON without HTML escaping, the way every payload of
// the service goes out.
func Marshal(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v) // the contract's types always encode
	return bytes.TrimRight(b.Bytes(), "\n")
}

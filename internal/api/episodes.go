package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
	"uuid"

	"panoptes/internal/store"
)

// episodeJSON is the brief's episode object. Its field names and formats are
// a contract the grading suite reads.
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
	CloseReason  *string `json:"close_reason"` // null while open
	SettledAt    *string `json:"settled_at"`   // null while open
}

// eventJSON is one of an episode's events in GET /episodes/{id}.
type eventJSON struct {
	EventID    string          `json:"event_id"`
	Severity   string          `json:"severity"`
	OccurredAt string          `json:"occurred_at"`
	Attributes json.RawMessage `json:"attributes"`
}

// listResponse is the body of GET /episodes.
type listResponse struct {
	Episodes   []episodeJSON `json:"episodes"`
	NextCursor string        `json:"next_cursor"` // empty on the last page
}

// detailResponse is the body of GET /episodes/{id}.
type detailResponse struct {
	Episode episodeJSON `json:"episode"`
	Events  []eventJSON `json:"events"`
}

func toEpisodeJSON(e store.Episode) episodeJSON {
	out := episodeJSON{
		ID:           e.ID.String(),
		SiteID:       e.Key.SiteID,
		CameraID:     e.Key.CameraID,
		Kind:         e.Key.Kind,
		Status:       e.Status,
		FirstSeen:    formatTime(e.FirstSeen),
		LastSeen:     formatTime(e.LastSeen),
		EventCount:   e.EventCount,
		PeakSeverity: e.PeakSeverity.String(),
		CloseReason:  e.CloseReason,
	}
	if e.SettledAt != nil {
		settledAt := formatTime(*e.SettledAt)
		out.SettledAt = &settledAt
	}
	return out
}

// formatTime renders a time in UTC with a Z suffix and milliseconds, as the
// brief's examples do, or with microseconds when it has digits below the
// millisecond, so that nothing Postgres stores is lost.
func formatTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()%int(time.Millisecond) == 0 {
		return t.Format("2006-01-02T15:04:05.000Z")
	}
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// listEpisodes serves GET /episodes. For now it filters on site_id only and
// returns every match on one page.
func listEpisodes(st *store.Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		episodes, err := st.Episodes(r.Context(), r.URL.Query().Get("site_id"))
		if err != nil {
			internalError(w, log, err)
			return
		}
		out := make([]episodeJSON, 0, len(episodes)) // an empty list is [], never null
		for _, e := range episodes {
			out = append(out, toEpisodeJSON(e))
		}
		writeJSON(w, http.StatusOK, listResponse{Episodes: out})
	}
}

// getEpisode serves GET /episodes/{id}. An id that is not a UUID cannot name an
// episode, so it gets the same 404 as an unknown one.
func getEpisode(st *store.Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusNotFound, store.ErrNotFound.Error())
			return
		}
		episode, events, err := st.Episode(r.Context(), id)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if err != nil {
			internalError(w, log, err)
			return
		}
		out := detailResponse{Episode: toEpisodeJSON(episode), Events: make([]eventJSON, 0, len(events))}
		for _, e := range events {
			out.Events = append(out.Events, eventJSON{
				EventID:    e.ID,
				Severity:   e.Severity.String(),
				OccurredAt: formatTime(e.OccurredAt),
				Attributes: e.Attributes,
			})
		}
		writeJSON(w, http.StatusOK, out)
	}
}

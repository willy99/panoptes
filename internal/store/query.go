package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"panoptes/internal/contract"
	"panoptes/internal/event"
)

// ErrNotFound means no episode has the requested id.
var ErrNotFound = errors.New("episode not found")

// Episode is an episode as stored.
type Episode struct {
	ID           uuid.UUID
	Key          event.EpisodeKey
	Status       string // "open" or "settled"
	FirstSeen    time.Time
	LastSeen     time.Time
	EventCount   int
	PeakSeverity event.Severity
	CloseReason  *string    // nil while open
	SettledAt    *time.Time // nil while open
}

// Fields renders the episode in the brief's shape, without its id.
func (e Episode) Fields() contract.Fields {
	f := contract.Fields{
		SiteID:       e.Key.SiteID,
		CameraID:     e.Key.CameraID,
		Kind:         e.Key.Kind,
		Status:       e.Status,
		FirstSeen:    contract.FormatTime(e.FirstSeen),
		LastSeen:     contract.FormatTime(e.LastSeen),
		EventCount:   e.EventCount,
		PeakSeverity: e.PeakSeverity.String(),
		CloseReason:  e.CloseReason,
	}
	if e.SettledAt != nil {
		settledAt := contract.FormatTime(*e.SettledAt)
		f.SettledAt = &settledAt
	}
	return f
}

const episodeColumns = `id, site_id, camera_id, kind, status, first_seen, last_seen,
	event_count, peak_severity, close_reason, settled_at`

func scanEpisode(row pgx.Row) (Episode, error) {
	var e Episode
	err := row.Scan(&e.ID, &e.Key.SiteID, &e.Key.CameraID, &e.Key.Kind, &e.Status, &e.FirstSeen, &e.LastSeen,
		&e.EventCount, &e.PeakSeverity, &e.CloseReason, &e.SettledAt)
	return e, err
}

// Filter selects episodes for the list. A zero field does not filter.
type Filter struct {
	SiteID, CameraID, Kind, Status string
	From, To                       time.Time // first seen: From inclusive, To exclusive
	After                          *Position // only episodes after this place in the order
	Limit                          int
}

// Position is a place in the list's order: first seen, then id, both
// descending.
type Position struct {
	FirstSeen time.Time
	ID        uuid.UUID
}

// Episodes lists up to f.Limit episodes that match f, newest first: by first
// seen, then by id, both descending, so that the order is total and a page
// can start exactly after the last row of the one before.
func (s *Store) Episodes(ctx context.Context, f Filter) ([]Episode, error) {
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	for _, c := range []struct{ column, value string }{
		{"site_id", f.SiteID}, {"camera_id", f.CameraID}, {"kind", f.Kind}, {"status", f.Status},
	} {
		if c.value != "" {
			where = append(where, c.column+" = "+arg(c.value))
		}
	}
	if !f.From.IsZero() {
		where = append(where, "first_seen >= "+arg(f.From))
	}
	if !f.To.IsZero() {
		where = append(where, "first_seen < "+arg(f.To))
	}
	if f.After != nil {
		where = append(where, "(first_seen, id) < ("+arg(f.After.FirstSeen)+", "+arg(f.After.ID)+")")
	}
	query := `SELECT ` + episodeColumns + ` FROM episodes`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY first_seen DESC, id DESC LIMIT " + arg(f.Limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list episodes: %w", err)
	}
	episodes, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Episode, error) { return scanEpisode(row) })
	if err != nil {
		return nil, fmt.Errorf("list episodes: %w", err)
	}
	return episodes, nil
}

// Episode returns an episode and its events in device-time order, ties by
// event_id. Both are read in one snapshot, so the list of events always
// matches the event count, even while an ingest changes the episode.
func (s *Store) Episode(ctx context.Context, id uuid.UUID) (Episode, []event.Event, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Episode{}, nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	ep, err := scanEpisode(tx.QueryRow(ctx, `SELECT `+episodeColumns+` FROM episodes WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Episode{}, nil, ErrNotFound
	}
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read episode %s: %w", id, err)
	}

	rows, err := tx.Query(ctx, `
		SELECT event_id, site_id, camera_id, kind, severity, occurred_at, attributes FROM events
		WHERE episode_id = $1
		ORDER BY occurred_at, event_id`, id)
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read events of %s: %w", id, err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (event.Event, error) {
		var e event.Event
		err := row.Scan(&e.ID, &e.Key.SiteID, &e.Key.CameraID, &e.Key.Kind, &e.Severity, &e.OccurredAt, &e.Attributes)
		return e, err
	})
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read events of %s: %w", id, err)
	}
	return ep, events, tx.Commit(ctx)
}

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"panoptes/internal/event"
)

// ErrNotFound means no episode has the requested id.
var ErrNotFound = errors.New("episode not found")

// Episode is an episode as the API shows it.
type Episode struct {
	ID           uuid.UUID
	Key          event.Key
	Status       string // "open" or "settled"
	FirstSeen    time.Time
	LastSeen     time.Time
	EventCount   int
	PeakSeverity event.Severity
	CloseReason  *string    // nil while open
	SettledAt    *time.Time // nil while open
}

// Member is one of an episode's events as the API shows it.
type Member struct {
	EventID    string
	Severity   event.Severity
	OccurredAt time.Time
	Attributes json.RawMessage
}

const episodeColumns = `id, site_id, camera_id, kind, status, first_seen, last_seen,
	event_count, peak_severity, close_reason, settled_at`

func scanEpisode(row pgx.Row) (Episode, error) {
	var e Episode
	err := row.Scan(&e.ID, &e.Key.SiteID, &e.Key.CameraID, &e.Key.Kind, &e.Status, &e.FirstSeen, &e.LastSeen,
		&e.EventCount, &e.PeakSeverity, &e.CloseReason, &e.SettledAt)
	return e, err
}

// Episodes lists episodes newest first: by first seen, then by id, both
// descending. An empty siteID lists every site.
func (s *Store) Episodes(ctx context.Context, siteID string) ([]Episode, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+episodeColumns+` FROM episodes
		WHERE $1 = '' OR site_id = $1
		ORDER BY first_seen DESC, id DESC`, siteID)
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
func (s *Store) Episode(ctx context.Context, id uuid.UUID) (Episode, []Member, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Episode{}, nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	ep, err := scanEpisode(tx.QueryRow(ctx, `SELECT `+episodeColumns+` FROM episodes WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Episode{}, nil, ErrNotFound
	}
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read episode %s: %w", id, err)
	}

	rows, err := tx.Query(ctx, `
		SELECT event_id, severity, occurred_at, attributes FROM events
		WHERE episode_id = $1
		ORDER BY occurred_at, event_id`, id)
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read events of %s: %w", id, err)
	}
	members, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Member, error) {
		var m Member
		err := row.Scan(&m.EventID, &m.Severity, &m.OccurredAt, &m.Attributes)
		return m, err
	})
	if err != nil {
		return Episode{}, nil, fmt.Errorf("read events of %s: %w", id, err)
	}
	return ep, members, tx.Commit(ctx)
}

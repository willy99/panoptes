package store

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"panoptes/internal/correlation"
	"panoptes/internal/event"
)

// ErrRejected wraps an insert that Postgres refused because of the event's
// own data. The codec should catch every such event first; this is the
// backstop, so that one it missed is skipped instead of failing on every
// delivery.
var ErrRejected = errors.New("postgres rejected the event")

// Outcome is what Ingest did with an event.
type Outcome struct {
	Duplicate bool      // the event_id was already stored, so nothing changed
	EpisodeID uuid.UUID // the episode the event joined; zero for a duplicate
}

// Ingest stores one event and rebuilds its key's open episodes, in one
// transaction:
//
//  1. Take the episode key's transaction lock. Everything that changes a key's
//     episodes holds it, so two instances never interleave on one key.
//  2. Insert the event with its arrival time. If the event_id is already
//     stored, nothing is inserted: the event is a duplicate, and Ingest
//     commits and returns. The first stored copy wins, even when a later copy
//     carries a different payload.
//  3. Load the key's open episodes with their events, and the settled
//     episodes among and just after them.
//  4. Rebuild the open episodes with the correlation rules, then write them:
//     upsert each episode, move events whose episode changed, and delete the
//     open episodes that no longer exist.
//  5. Commit.
//
// A kill before the commit rolls all of it back, and the redelivered message
// does it again. The caller acknowledges the message only after Ingest
// returns, so a kill between the commit and the ack, or a lost ack, makes the
// redelivery a duplicate that stops at step 2.
func (s *Store) Ingest(ctx context.Context, ev event.Event) (Outcome, error) {
	// Read committed, whatever DATABASE_URL sets as the default: each statement
	// must see what the previous holder of the key's lock committed while this
	// transaction waited for the lock.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Outcome{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // does nothing after a commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", keyLock(ev.Key)); err != nil {
		return Outcome{}, fmt.Errorf("lock episode key: %w", err)
	}

	var arrivedAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO events (event_id, site_id, camera_id, kind, severity, occurred_at, attributes, arrived_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (event_id) DO NOTHING
		RETURNING arrived_at`,
		ev.ID, ev.Key.SiteID, ev.Key.CameraID, ev.Key.Kind, ev.Severity, ev.OccurredAt, ev.Attributes).Scan(&arrivedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Outcome{Duplicate: true}, tx.Commit(ctx)
	}
	if err != nil {
		var pgErr *pgconn.PgError
		// Class 22 is data exceptions, class 54 program limits such as an
		// index entry that is too long.
		if errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "54")) {
			return Outcome{}, fmt.Errorf("%w: %v", ErrRejected, err)
		}
		return Outcome{}, fmt.Errorf("insert event: %w", err)
	}

	open, err := openEvents(ctx, tx, ev.Key)
	if err != nil {
		return Outcome{}, err
	}
	from, to := ev.OccurredAt, ev.OccurredAt
	for _, e := range open {
		from, to = minTime(from, e.OccurredAt), maxTime(to, e.OccurredAt)
	}
	settled, err := settledSpans(ctx, tx, ev.Key, from, to)
	if err != nil {
		return Outcome{}, err
	}

	added := correlation.Event{ID: ev.ID, OccurredAt: ev.OccurredAt, Severity: ev.Severity, ArrivedAt: arrivedAt}
	episodes, gone := correlation.Rebuild(s.rules, append(open, added), settled, uuid.NewV4)

	var joined uuid.UUID
	for _, ep := range episodes {
		if err := writeEpisode(ctx, tx, ev.Key, ep); err != nil {
			return Outcome{}, err
		}
		for _, e := range ep.Events {
			if e.ID == ev.ID {
				joined = ep.ID
			}
		}
	}
	if len(gone) > 0 {
		if _, err := tx.Exec(ctx, "DELETE FROM episodes WHERE id = ANY($1) AND status = 'open'", gone); err != nil {
			return Outcome{}, fmt.Errorf("delete merged episodes: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, fmt.Errorf("commit: %w", err)
	}
	return Outcome{EpisodeID: joined}, nil
}

// openEvents loads the events of the key's open episodes, each with the
// episode that holds it.
func openEvents(ctx context.Context, tx pgx.Tx, key event.EpisodeKey) ([]correlation.Event, error) {
	rows, err := tx.Query(ctx, `
		SELECT e.event_id, e.occurred_at, e.severity, e.arrived_at, e.episode_id
		FROM episodes ep JOIN events e ON e.episode_id = ep.id
		WHERE ep.site_id = $1 AND ep.camera_id = $2 AND ep.kind = $3 AND ep.status = 'open'`,
		key.SiteID, key.CameraID, key.Kind)
	if err != nil {
		return nil, fmt.Errorf("load open episodes: %w", err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (correlation.Event, error) {
		var e correlation.Event
		err := row.Scan(&e.ID, &e.OccurredAt, &e.Severity, &e.ArrivedAt, &e.EpisodeID)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("load open episodes: %w", err)
	}
	return events, nil
}

// settledSpans loads the key's settled episodes that overlap the device times
// from..to of the open events, which act as dividers, and the first one that
// starts after them, which can end the last open episode.
func settledSpans(ctx context.Context, tx pgx.Tx, key event.EpisodeKey, from, to time.Time) ([]correlation.Span, error) {
	rows, err := tx.Query(ctx, `
		(SELECT first_seen, last_seen FROM episodes
		 WHERE site_id = $1 AND camera_id = $2 AND kind = $3 AND status = 'settled'
		   AND last_seen >= $4 AND first_seen <= $5)
		UNION ALL
		(SELECT first_seen, last_seen FROM episodes
		 WHERE site_id = $1 AND camera_id = $2 AND kind = $3 AND status = 'settled'
		   AND first_seen > $5
		 ORDER BY first_seen LIMIT 1)`,
		key.SiteID, key.CameraID, key.Kind, from, to)
	if err != nil {
		return nil, fmt.Errorf("load settled episodes: %w", err)
	}
	spans, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (correlation.Span, error) {
		var sp correlation.Span
		err := row.Scan(&sp.FirstSeen, &sp.LastSeen)
		return sp, err
	})
	if err != nil {
		return nil, fmt.Errorf("load settled episodes: %w", err)
	}
	return spans, nil
}

// writeEpisode upserts one rebuilt open episode and points its events at it.
func writeEpisode(ctx context.Context, tx pgx.Tx, key event.EpisodeKey, ep correlation.Episode) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO episodes (id, site_id, camera_id, kind, status, first_seen, last_seen,
		                      event_count, peak_severity, last_arrival, pending_close_reason)
		VALUES ($1, $2, $3, $4, 'open', $5, $6, $7, $8, $9, NULLIF($10, ''))
		ON CONFLICT (id) DO UPDATE SET
			first_seen           = EXCLUDED.first_seen,
			last_seen            = EXCLUDED.last_seen,
			event_count          = EXCLUDED.event_count,
			peak_severity        = EXCLUDED.peak_severity,
			last_arrival         = EXCLUDED.last_arrival,
			pending_close_reason = EXCLUDED.pending_close_reason`,
		ep.ID, key.SiteID, key.CameraID, key.Kind, ep.FirstSeen, ep.LastSeen,
		len(ep.Events), ep.PeakSeverity, ep.LastArrival, string(ep.Pending))
	if err != nil {
		return fmt.Errorf("write episode %s: %w", ep.ID, err)
	}
	var moved []string
	for _, e := range ep.Events {
		if e.EpisodeID != ep.ID {
			moved = append(moved, e.ID)
		}
	}
	if len(moved) > 0 {
		if _, err := tx.Exec(ctx, "UPDATE events SET episode_id = $1 WHERE event_id = ANY($2)", ep.ID, moved); err != nil {
			return fmt.Errorf("move events to episode %s: %w", ep.ID, err)
		}
	}
	return nil
}

// keyLock maps an episode key to the number of its transaction lock. Two keys
// whose numbers collide only wait for each other.
func keyLock(k event.EpisodeKey) int64 {
	h := fnv.New64a()
	for _, part := range []string{k.SiteID, k.CameraID, k.Kind} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return int64(h.Sum64())
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

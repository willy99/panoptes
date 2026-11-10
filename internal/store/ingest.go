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
	"panoptes/internal/crash"
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
	Revision  int       // set when the event joined a settled episode as a correction
}

// Ingest stores one event and updates its key's episodes, in one
// transaction:
//
//  1. Take the episode key's transaction lock. Everything that changes a key's
//     episodes holds it, so two instances never interleave on one key.
//  2. Insert the event with its arrival time. If the event_id is already
//     stored, nothing is inserted: the event is a duplicate, and Ingest
//     commits and returns. The first stored copy wins, even when a later copy
//     carries a different payload.
//  3. Find the key's episodes just before and just after the event, and let
//     the placement rule pick one. If it picks a settled episode, the event
//     joins it as a correction: its aggregates are recomputed, its revision
//     rises by one, and a corrected announcement goes into the outbox.
//  4. Load the key's open episodes with their events, and the settled
//     episodes among and just after them; rebuild the open episodes, with the
//     new event unless it became a correction, and write them: upsert each,
//     move events whose episode changed, and delete the open episodes a merge
//     removed. After a correction this still matters: the settled episode may
//     now start earlier, which can change why the open one before it ends.
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

	added := correlation.Event{ID: ev.ID, OccurredAt: ev.OccurredAt, Severity: ev.Severity, ArrivedAt: arrivedAt}
	before, after, err := neighbours(ctx, tx, ev.Key, ev.OccurredAt)
	if err != nil {
		return Outcome{}, err
	}
	var out Outcome
	rebuild := []correlation.Event{added}
	if target := correlation.Place(s.rules.Settings, ev.OccurredAt, before, after); target != nil && target.Settled {
		if out, err = correct(ctx, tx, target.ID, added); err != nil {
			return Outcome{}, err
		}
		rebuild = nil
	}

	open, err := openEvents(ctx, tx, ev.Key)
	if err != nil {
		return Outcome{}, err
	}
	rebuild = append(rebuild, open...)
	if len(rebuild) > 0 {
		from, to := rebuild[0].OccurredAt, rebuild[0].OccurredAt
		for _, e := range rebuild {
			from, to = minTime(from, e.OccurredAt), maxTime(to, e.OccurredAt)
		}
		settled, err := settledSpans(ctx, tx, ev.Key, from, to)
		if err != nil {
			return Outcome{}, err
		}
		episodes, gone := correlation.Rebuild(s.rules.Settings, rebuild, settled, uuid.NewV4)
		for _, ep := range episodes {
			if err := writeEpisode(ctx, tx, ev.Key, ep); err != nil {
				return Outcome{}, err
			}
			for _, e := range ep.Events {
				if e.ID == ev.ID {
					out.EpisodeID = ep.ID
				}
			}
		}
		if len(gone) > 0 {
			if _, err := tx.Exec(ctx, "DELETE FROM episodes WHERE id = ANY($1) AND status = 'open'", gone); err != nil {
				return Outcome{}, fmt.Errorf("delete merged episodes: %w", err)
			}
		}
	}

	crash.Point(crash.IngestBeforeCommit)
	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

// neighbours loads the key's episode with the greatest first seen at or
// before the device time at, and the one with the smallest first seen after
// it. Either is nil when there is none.
func neighbours(ctx context.Context, tx pgx.Tx, key event.EpisodeKey, at time.Time) (before, after *correlation.Neighbour, err error) {
	rows, err := tx.Query(ctx, `
		(SELECT id, first_seen, last_seen, status = 'settled', true FROM episodes
		 WHERE site_id = $1 AND camera_id = $2 AND kind = $3 AND first_seen <= $4
		 ORDER BY first_seen DESC LIMIT 1)
		UNION ALL
		(SELECT id, first_seen, last_seen, status = 'settled', false FROM episodes
		 WHERE site_id = $1 AND camera_id = $2 AND kind = $3 AND first_seen > $4
		 ORDER BY first_seen LIMIT 1)`,
		key.SiteID, key.CameraID, key.Kind, at)
	if err != nil {
		return nil, nil, fmt.Errorf("load neighbours: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n correlation.Neighbour
		var isBefore bool
		if err := rows.Scan(&n.ID, &n.FirstSeen, &n.LastSeen, &n.Settled, &isBefore); err != nil {
			return nil, nil, fmt.Errorf("load neighbours: %w", err)
		}
		if isBefore {
			before = &n
		} else {
			after = &n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("load neighbours: %w", err)
	}
	return before, after, nil
}

// correct adds a late event to a settled episode: it assigns the event,
// recomputes the episode's aggregates from all its events, raises its
// revision and queues one corrected announcement with the new state.
// settled_at and the close reason stay as they were.
func correct(ctx context.Context, tx pgx.Tx, id uuid.UUID, late correlation.Event) (Outcome, error) {
	events, err := collectEvents(tx.Query(ctx, `
		SELECT event_id, occurred_at, severity, arrived_at, episode_id FROM events WHERE episode_id = $1`, id))
	if err != nil {
		return Outcome{}, fmt.Errorf("load episode %s: %w", id, err)
	}
	corrected := correlation.Correct(id, events, late)

	if _, err := tx.Exec(ctx, "UPDATE events SET episode_id = $1 WHERE event_id = $2", id, late.ID); err != nil {
		return Outcome{}, fmt.Errorf("assign late event: %w", err)
	}
	ep, err := scanEpisode(tx.QueryRow(ctx, `
		UPDATE episodes SET
			first_seen    = $2,
			last_seen     = $3,
			event_count   = $4,
			peak_severity = $5,
			last_arrival  = $6,
			revision      = revision + 1
		WHERE id = $1 AND status = 'settled'
		RETURNING `+episodeColumns,
		id, corrected.FirstSeen, corrected.LastSeen, len(corrected.Events), corrected.PeakSeverity, corrected.LastArrival))
	if err != nil {
		return Outcome{}, fmt.Errorf("correct episode %s: %w", id, err)
	}

	if err := enqueue(ctx, tx, ep); err != nil {
		return Outcome{}, err
	}
	return Outcome{EpisodeID: id, Revision: ep.Revision}, nil
}

// openEvents loads the events of the key's open episodes, each with the
// episode that holds it.
func openEvents(ctx context.Context, tx pgx.Tx, key event.EpisodeKey) ([]correlation.Event, error) {
	events, err := collectEvents(tx.Query(ctx, `
		SELECT e.event_id, e.occurred_at, e.severity, e.arrived_at, e.episode_id
		FROM episodes ep JOIN events e ON e.episode_id = ep.id
		WHERE ep.site_id = $1 AND ep.camera_id = $2 AND ep.kind = $3 AND ep.status = 'open'`,
		key.SiteID, key.CameraID, key.Kind))
	if err != nil {
		return nil, fmt.Errorf("load open episodes: %w", err)
	}
	return events, nil
}

// collectEvents reads rows of event_id, occurred_at, severity, arrived_at and
// episode_id into events as the correlation rules see them.
func collectEvents(rows pgx.Rows, err error) ([]correlation.Event, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (correlation.Event, error) {
		var e correlation.Event
		err := row.Scan(&e.ID, &e.OccurredAt, &e.Severity, &e.ArrivedAt, &e.EpisodeID)
		return e, err
	})
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

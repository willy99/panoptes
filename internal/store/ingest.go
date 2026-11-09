package store

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"uuid"

	"github.com/jackc/pgx/v5"

	"panoptes/internal/event"
)

// Outcome is what Ingest did with an event.
type Outcome struct {
	Duplicate bool      // the event_id was already stored, so nothing changed
	EpisodeID uuid.UUID // the episode the event joined; zero for a duplicate
}

// Ingest stores one event and brings its episode up to date, in one
// transaction:
//
//  1. Take the episode key's transaction lock. Everything that changes a key's
//     episodes holds it, so two instances never interleave on one key.
//  2. Insert the event with its arrival time. If the event_id is already
//     stored, nothing is inserted: the event is a duplicate, and Ingest
//     commits and returns. The first stored copy wins, even when a later copy
//     carries a different payload.
//  3. Find the key's open episode, or create one.
//  4. Assign the event to it and recompute its aggregates from its events.
//  5. Commit.
//
// A kill before the commit rolls all of it back, and the redelivered message
// does it again. The caller acknowledges the message only after Ingest
// returns, so a kill between the commit and the ack, or a lost ack, makes the
// redelivery a duplicate that stops at step 2.
func (s *Store) Ingest(ctx context.Context, ev event.Event) (Outcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) // does nothing after a commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", keyLock(ev.Key)); err != nil {
		return Outcome{}, fmt.Errorf("lock episode key: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO events (event_id, site_id, camera_id, kind, severity, occurred_at, attributes, arrived_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (event_id) DO NOTHING`,
		ev.ID, ev.Key.SiteID, ev.Key.CameraID, ev.Key.Kind, ev.Severity, ev.OccurredAt, ev.Attributes)
	if err != nil {
		return Outcome{}, fmt.Errorf("insert event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return Outcome{Duplicate: true}, tx.Commit(ctx)
	}

	// A stand-in until correlation by device time: each key has at most one
	// open episode, and it takes every event of the key.
	var id uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT id FROM episodes
		WHERE site_id = $1 AND camera_id = $2 AND kind = $3 AND status = 'open'`,
		ev.Key.SiteID, ev.Key.CameraID, ev.Key.Kind).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// The aggregates are the event's own; step 4 recomputes them anyway.
		id = uuid.NewV4()
		_, err = tx.Exec(ctx, `
			INSERT INTO episodes (id, site_id, camera_id, kind, status,
			                      first_seen, last_seen, event_count, peak_severity, last_arrival)
			VALUES ($1, $2, $3, $4, 'open', $5, $5, 1, $6, now())`,
			id, ev.Key.SiteID, ev.Key.CameraID, ev.Key.Kind, ev.OccurredAt, ev.Severity)
		if err != nil {
			return Outcome{}, fmt.Errorf("create episode: %w", err)
		}
	case err != nil:
		return Outcome{}, fmt.Errorf("find open episode: %w", err)
	}

	if _, err := tx.Exec(ctx, "UPDATE events SET episode_id = $1 WHERE event_id = $2", id, ev.ID); err != nil {
		return Outcome{}, fmt.Errorf("assign event: %w", err)
	}
	if err := recompute(ctx, tx, id); err != nil {
		return Outcome{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, fmt.Errorf("commit: %w", err)
	}
	return Outcome{EpisodeID: id}, nil
}

// recompute sets an episode's aggregates from the events that belong to it.
func recompute(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE episodes SET
			first_seen    = agg.first_seen,
			last_seen     = agg.last_seen,
			event_count   = agg.event_count,
			peak_severity = agg.peak_severity,
			last_arrival  = agg.last_arrival
		FROM (SELECT min(occurred_at) AS first_seen,
		             max(occurred_at) AS last_seen,
		             count(*)         AS event_count,
		             max(severity)    AS peak_severity,
		             max(arrived_at)  AS last_arrival
		      FROM events WHERE episode_id = $1) AS agg
		WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("recompute episode %s: %w", id, err)
	}
	return nil
}

// keyLock maps an episode key to the number of its transaction lock. Two keys
// whose numbers collide only wait for each other.
func keyLock(k event.Key) int64 {
	h := fnv.New64a()
	for _, part := range []string{k.SiteID, k.CameraID, k.Kind} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return int64(h.Sum64())
}

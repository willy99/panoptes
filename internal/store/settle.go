package store

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"

	"panoptes/internal/contract"
	"panoptes/internal/event"
)

// dueCondition is true for an open episode whose silence, measured on the
// Postgres clock from the arrival of its latest event, has reached its wait:
// the settle grace ($2) when it has ended by device time, the quiet window
// ($1) otherwise.
const dueCondition = `status = 'open' AND last_arrival <= now() -
	CASE WHEN pending_close_reason IS NULL THEN $1::interval ELSE $2::interval END`

// DueEpisodes lists up to limit open episodes that are due to settle, the
// longest silent first.
func (s *Store) DueEpisodes(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM episodes WHERE `+dueCondition+` ORDER BY last_arrival LIMIT $3`,
		s.rules.QuietWindow, s.rules.SettleGrace, limit)
	if err != nil {
		return nil, fmt.Errorf("find due episodes: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("find due episodes: %w", err)
	}
	return ids, nil
}

// Settle settles one episode, in one transaction:
//
//  1. Read the episode's key; if the episode is gone, a merge removed it and
//     there is nothing to do.
//  2. Take the key's lock, so no ingest changes the episode meanwhile.
//  3. Check that it is still open and still due, since an event may have
//     arrived after DueEpisodes looked. If so, set status settled, settled_at
//     from the Postgres clock, and the close reason: the pending one, or quiet.
//  4. Write the announcement to the outbox: the episode object as of this
//     transaction, with message id settled:<id>.
//  5. Commit.
//
// A kill before the commit leaves the episode open and due, and the next
// sweep settles it. After the commit the outbox row exists, so the
// announcement goes out however soon the process dies. Settle reports false
// when the episode was not settled.
func (s *Store) Settle(ctx context.Context, id uuid.UUID) (Episode, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Episode{}, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var key event.EpisodeKey
	err = tx.QueryRow(ctx, "SELECT site_id, camera_id, kind FROM episodes WHERE id = $1", id).
		Scan(&key.SiteID, &key.CameraID, &key.Kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return Episode{}, false, nil
	}
	if err != nil {
		return Episode{}, false, fmt.Errorf("read episode %s: %w", id, err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", keyLock(key)); err != nil {
		return Episode{}, false, fmt.Errorf("lock episode key: %w", err)
	}

	ep, err := scanEpisode(tx.QueryRow(ctx, `
		UPDATE episodes SET
			status               = 'settled',
			settled_at           = now(),
			close_reason         = COALESCE(pending_close_reason, 'quiet'),
			pending_close_reason = NULL
		WHERE id = $3 AND `+dueCondition+`
		RETURNING `+episodeColumns,
		s.rules.QuietWindow, s.rules.SettleGrace, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Episode{}, false, tx.Commit(ctx) // no longer open, or no longer due
	}
	if err != nil {
		return Episode{}, false, fmt.Errorf("settle episode %s: %w", id, err)
	}

	payload := contract.Marshal(contract.Announcement{EpisodeID: ep.ID.String(), Fields: ep.Fields()})
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (episode_id, subject, msg_id, payload) VALUES ($1, $2, $3, $4)`,
		ep.ID, "episode.settled."+key.SiteID+"."+key.CameraID, "settled:"+ep.ID.String(), string(payload))
	if err != nil {
		return Episode{}, false, fmt.Errorf("queue announcement of %s: %w", id, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Episode{}, false, fmt.Errorf("commit: %w", err)
	}
	return ep, true, nil
}

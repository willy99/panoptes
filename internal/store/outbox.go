package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"panoptes/internal/contract"
)

// Announcement is one outbox row to publish.
type Announcement struct {
	ID      int64
	Subject string
	MsgID   string // the Nats-Msg-Id, so JetStream drops a second copy
	Payload []byte
}

// enqueue writes the episode's announcement into the outbox, as of the
// calling transaction: the settled announcement at revision 0, a corrected
// one with its revision after that. The message id makes a second publish of
// the same row a copy that JetStream drops.
func enqueue(ctx context.Context, tx pgx.Tx, ep Episode) error {
	a := contract.Announcement{EpisodeID: ep.ID.String(), Fields: ep.Fields()}
	subject, msgID := "episode.settled.", "settled:"+ep.ID.String()
	if ep.Revision > 0 {
		a.Revision = &ep.Revision
		subject, msgID = "episode.corrected.", fmt.Sprintf("corrected:%s:%d", ep.ID, ep.Revision)
	}
	_, err := tx.Exec(ctx, `INSERT INTO outbox (episode_id, subject, msg_id, payload) VALUES ($1, $2, $3, $4)`,
		ep.ID, subject+ep.Key.SiteID+"."+ep.Key.CameraID, msgID, string(contract.Marshal(a)))
	if err != nil {
		return fmt.Errorf("queue announcement %s: %w", msgID, err)
	}
	return nil
}

// PublishOutbox publishes up to limit unpublished announcements, in one
// transaction:
//
//  1. Claim rows in id order with FOR UPDATE SKIP LOCKED, so relays in other
//     instances take other rows. A row is claimed only when no earlier
//     unpublished row exists for its episode, so a correction never reaches
//     the stream before its episode's settled announcement or an earlier
//     correction, even with several relays.
//  2. Hand each row to publish, which returns once JetStream has acknowledged
//     it.
//  3. Mark the published rows and commit.
//
// A kill before a row is published leaves it unpublished, and it is claimed
// again. A kill after the publish but before the commit publishes it again
// with the same message id, and JetStream drops the copy within its
// duplicate window. When publish fails, the rows published before it are
// still marked, and the error is returned so the relay can back off.
func (s *Store) PublishOutbox(ctx context.Context, limit int, publish func(context.Context, Announcement) error) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, subject, msg_id, payload::text FROM outbox o
		WHERE published_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM outbox earlier
		                  WHERE earlier.episode_id = o.episode_id
		                    AND earlier.published_at IS NULL
		                    AND earlier.id < o.id)
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("claim announcements: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Announcement, error) {
		var a Announcement
		var payload string
		err := row.Scan(&a.ID, &a.Subject, &a.MsgID, &payload)
		a.Payload = []byte(payload)
		return a, err
	})
	if err != nil {
		return 0, fmt.Errorf("claim announcements: %w", err)
	}

	var published []int64
	var publishErr error
	for _, a := range claimed {
		if publishErr = publish(ctx, a); publishErr != nil {
			break
		}
		published = append(published, a.ID)
	}
	if len(published) > 0 {
		if _, err := tx.Exec(ctx, "UPDATE outbox SET published_at = now() WHERE id = ANY($1)", published); err != nil {
			return 0, fmt.Errorf("mark announcements published: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	if publishErr != nil {
		return len(published), fmt.Errorf("publish: %w", publishErr)
	}
	return len(published), nil
}

// PendingAnnouncements counts the outbox rows not yet published.
func (s *Store) PendingAnnouncements(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n)
	return n, err
}

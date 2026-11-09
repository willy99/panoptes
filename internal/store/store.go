// Package store owns every Postgres access of the service: the connection pool,
// the schema migrations and, in later tickets, the ingest, settle and outbox
// transactions and the queries behind the API. Callers never see SQL.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the service's handle on Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// Open creates the connection pool. It does not connect yet: the first query
// does, so Open succeeds even while Postgres is still starting.
func Open(databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Ping checks that Postgres answers.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close waits for borrowed connections to come back and closes the pool.
func (s *Store) Close() {
	s.pool.Close()
}

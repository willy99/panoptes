// Package store owns every Postgres access of the service: the connection
// pool, the schema migrations, the ingest and settle transactions, the
// outbox and the queries behind the API. Callers never see SQL.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"panoptes/internal/correlation"
)

// Store is the service's handle on Postgres.
type Store struct {
	pool  *pgxpool.Pool
	rules Rules
}

// Rules are the windows the store's transactions apply.
type Rules struct {
	correlation.Settings               // how the ingest transaction groups events
	SettleGrace          time.Duration // the silence that settles an episode that has ended by device time
}

// minPoolSize is the smallest pool the service runs with: one connection for
// each of the four ingest workers, the settle loop and the relay, plus room
// for the API's reads. A larger pool_max_conns in DATABASE_URL is kept.
const minPoolSize = 10

// Open creates the connection pool. It does not connect yet: the first query
// does, so Open succeeds even while Postgres is still starting.
func Open(databaseURL string, rules Rules) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = max(cfg.MaxConns, minPoolSize)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	return &Store{pool: pool, rules: rules}, nil
}

// Ping checks that Postgres answers.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close waits for borrowed connections to come back and closes the pool.
func (s *Store) Close() {
	s.pool.Close()
}

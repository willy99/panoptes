package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestTwoInstancesMigrateTheSameDatabaseAtOnce(t *testing.T) {
	dbURL := freshDatabase(t)

	a := start(t, "DATABASE_URL="+dbURL)
	b := start(t, "DATABASE_URL="+dbURL)

	a.waitReady()
	b.waitReady()

	// Postgres was up the whole time, so any retry means the two migrations collided.
	for _, p := range []*proc{a, b} {
		if p.output.contains(`"msg":"retrying"`) {
			t.Fatalf("an instance retried its startup:\n%s", p.output.String())
		}
	}
}

// freshDatabase creates an empty database next to the test database and
// returns its URL; the database is dropped when the test ends. It is test
// setup only: the tests never read the service's tables.
func freshDatabase(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer admin.Close(context.Background())

	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	name := "panoptes_e2e_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

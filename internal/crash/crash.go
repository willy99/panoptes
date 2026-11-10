// Package crash lets tests kill the process at a named step, so that every
// "what if it dies here" answer is backed by a test. Without
// PANOPTES_CRASH_AT, every point does nothing.
package crash

import (
	"fmt"
	"os"
	"slices"
)

// The named points, each on the ingest, settle or publish path.
const (
	IngestBeforeCommit = "ingest.before_commit"
	IngestAfterCommit  = "ingest.after_commit" // committed, not yet acknowledged
	SettleBeforeCommit = "settle.before_commit"
	RelayBeforePublish = "relay.before_publish"
	RelayAfterPublish  = "relay.after_publish" // published, not yet marked
)

// points lists every named point.
var points = []string{IngestBeforeCommit, IngestAfterCommit, SettleBeforeCommit, RelayBeforePublish, RelayAfterPublish}

// armed is the point to exit at, set once at startup.
var armed string

// Arm makes the process exit at the named point. An empty name disarms.
func Arm(point string) error {
	if point != "" && !slices.Contains(points, point) {
		return fmt.Errorf("unknown crash point %q", point)
	}
	armed = point
	return nil
}

// Point exits the process at once when it is the armed point. No deferred
// cleanup and no graceful drain run, which from the outside looks like a
// kill: open transactions roll back when Postgres loses the connection, and
// unacknowledged messages come back after AckWait.
func Point(name string) {
	if name == armed {
		fmt.Fprintf(os.Stderr, `{"level":"ERROR","msg":"crash point reached","point":%q}`+"\n", name)
		os.Exit(3)
	}
}

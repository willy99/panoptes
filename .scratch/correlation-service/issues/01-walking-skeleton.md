# 01: Walking skeleton

**What to build:** A reviewer runs `docker compose up` on a machine with only Docker and gets NATS with JetStream, Postgres and one Panoptes instance. Panoptes reads its configuration from the environment as the spec's table lists it and fails fast on an invalid value. It applies its embedded migrations under a session advisory lock, creates `ANALYTICS_EVENTS`, the durable consumer `episoder` and `EPISODES` when they are missing and uses existing ones as they are, serves `/healthz` and `/readyz`, and exits cleanly on SIGTERM. `make test` runs every test in a Go container inside a separate compose project with its own NATS and Postgres, and tears that project down afterwards. The end-to-end harness builds the binary once, starts instances as child processes with test settings, can stop them with SIGTERM or SIGKILL, and skips itself when the broker and database addresses are not set. No domain behaviour yet. Spec stories 40, 44, 45, 46, 47.

**Blocked by:** None (can start immediately)

**Status:** done

- [x] On a clean clone, `docker compose up` brings all three services up and `/readyz` returns 200.
- [x] `make up && make test` passes, and `make test` leaves the stack started by `make up` untouched.
- [x] `go test ./...` on a host without the addresses set passes and skips the end-to-end package.
- [x] End-to-end: after start, `/healthz` and `/readyz` return 200, and the two streams and the consumer exist with the spec's settings.
- [x] End-to-end: an `ANALYTICS_EVENTS` stream created beforehand with different settings is left unchanged.
- [x] End-to-end: an invalid duration in `EPISODE_QUIET_WINDOW` makes the process exit non-zero with a message that names the variable.
- [x] End-to-end: SIGTERM makes the process exit with code 0.
- [x] Two instances started at the same moment both apply migrations without error.

## Comments

Done in 9954c62 (config, health, e2e harness), 61be893 (Postgres and migrations), 5404f8a (provisioning), baa4245 (Dockerfile and compose) and 74dd189 (fixes from the two-axis review). Checked on a fresh clone: `docker compose up` gives `/readyz` 200, and `make test` passes next to the running stack.

What these tests do not prove:
- The two-instance migration test is a smoke test. With the advisory lock removed, it still passed 30 runs out of 30: the collision is rare, and when it happens the loser's transaction rolls back and startup retries.
- The harness picks a free port and closes it before the child binds it, so another process could take it in between. This is acceptable while tests run one at a time.

Beyond the ticket: the `EPISODES` stream takes only the announcement patterns no other stream covers, and it is extended when a pattern loses its stream. The spec and ADR-0004 were updated to match.

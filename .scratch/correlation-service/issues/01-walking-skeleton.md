# 01: Walking skeleton

**What to build:** A reviewer runs `docker compose up` on a machine with only Docker and gets NATS with JetStream, Postgres and one Panoptes instance. Panoptes reads its configuration from the environment as the spec's table lists it and fails fast on an invalid value. It applies its embedded migrations under a session advisory lock, creates `ANALYTICS_EVENTS`, the durable consumer `episoder` and `EPISODES` when they are missing and uses existing ones as they are, serves `/healthz` and `/readyz`, and exits cleanly on SIGTERM. `make test` runs every test in a Go container inside a separate compose project with its own NATS and Postgres, and tears that project down afterwards. The end-to-end harness builds the binary once, starts instances as child processes with test settings, can stop them with SIGTERM or SIGKILL, and skips itself when the broker and database addresses are not set. No domain behaviour yet. Spec stories 40, 44, 45, 46, 47.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] On a clean clone, `docker compose up` brings all three services up and `/readyz` returns 200.
- [ ] `make up && make test` passes, and `make test` leaves the stack started by `make up` untouched.
- [ ] `go test ./...` on a host without the addresses set passes and skips the end-to-end package.
- [ ] End-to-end: after start, `/healthz` and `/readyz` return 200, and the two streams and the consumer exist with the spec's settings.
- [ ] End-to-end: an `ANALYTICS_EVENTS` stream created beforehand with different settings is left unchanged.
- [ ] End-to-end: an invalid duration in `EPISODE_QUIET_WINDOW` makes the process exit non-zero with a message that names the variable.
- [ ] End-to-end: SIGTERM makes the process exit with code 0.
- [ ] Two instances started at the same moment both apply migrations without error.

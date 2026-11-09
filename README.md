# Panoptes

Panoptes turns camera detection events into episodes: one operator-facing unit per thing that happened, instead of dozens of alerts. It consumes `ANALYTICS_EVENTS` from NATS JetStream through the shared durable consumer `episoder`. It groups events by device time into episodes and stores them in Postgres, serves them over HTTP, and announces each settled episode exactly once on `episode.settled.<site_id>.<camera_id>`. Any number of identical instances can run side by side, and a kill at any step loses nothing.

The vocabulary is defined in [GLOSSARY.md](GLOSSARY.md), and the decisions behind the design are in [docs/adr/](docs/adr/).

## Run it

Only Docker is needed.

| Command | What it does |
|---|---|
| `docker compose up` | NATS with JetStream, Postgres and one Panoptes instance; the API is on `localhost:8080` |
| `make up` / `make down` / `make logs` | the same stack in the background; `down` also removes its data |
| `make test` | every test, in a Go container, against a separate compose project with its own NATS and Postgres; the stack from `make up` is left alone |
| `make demo` | publishes a scenario to the running stack and prints each announcement as it arrives; see `cmd/demo` |
| `docker compose --profile multi up` | adds a second, identical instance on `localhost:8081` |

`go test ./...` on a host with Go runs the correlation tables and skips the end-to-end tests, which need `NATS_URL` and `DATABASE_URL`. The soak test takes `PANOPTES_SOAK_ROUNDS` (default 4) and `PANOPTES_SOAK_SEED`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `EPISODE_QUIET_WINDOW` | `60s` | longest device-time gap inside one episode, and the silence that settles it |
| `EPISODE_MAX_DURATION` | `10m` | an episode spans `[first_seen, first_seen + max)` |
| `EPISODE_SETTLE_GRACE` | `1s` | silence that settles an episode that already has a successor (ours) |
| `NATS_URL` | `nats://nats:4222` | broker |
| `DATABASE_URL` | required | Postgres |
| `HTTP_ADDR` | `:8080` | API, health and metrics |
| `PANOPTES_CRASH_AT` | empty | test only: exit at a named crash point |

## Interfaces

- `GET /episodes`: optional `site_id`, `camera_id`, `kind`, `status`, `from` (inclusive), `to` (exclusive), `limit` (default 50, clamped to 200) and `cursor`. Results are ordered by `first_seen`, then `id`, both descending.
- `GET /episodes/{id}`: the episode and its events, ordered by `occurred_at`, then `event_id`.
- Errors are `{"error": "..."}`: 400 for malformed parameters or cursors, 404 for unknown or malformed ids.
- `/healthz` reports that the process is alive. `/readyz` reports that migrations are applied, Postgres and NATS answer, and shutdown has not begun.
- `/metrics` serves `episoder_invalid_events_total` and the `panoptes_*` counters for ingest, settles, corrections, the outbox, publish failures and ingest latency.
- `episode.settled.<site>.<camera>`: the episode object with `episode_id` in place of `id`, and `Nats-Msg-Id: settled:<id>`.
- `episode.corrected.<site>.<camera>`: the same object plus `revision`, and `Nats-Msg-Id: corrected:<id>:<revision>`.

## How it is built

One binary, coordinated only through Postgres (ADR-0001). Each delivery runs one transaction:
1. Take an advisory lock on the episode key.
2. Insert the event with `ON CONFLICT DO NOTHING`.
3. Either rebuild the key's open episodes, or add the event to a settled episode as a correction (ADR-0002, ADR-0005).
4. Commit, and only then acknowledge.

A settle loop closes silent episodes and writes the announcement to an outbox in the same transaction. A relay in every instance publishes the outbox with `Nats-Msg-Id` (ADR-0003, ADR-0004).

| Package | Role |
|---|---|
| `internal/correlation` | the rules, pure, with table tests |
| `internal/event` | the codec |
| `internal/store` | all SQL |
| `internal/ingest`, `settle`, `relay` | the three loops |
| `internal/api`, `contract` | HTTP and the brief's shapes |

Tests go through two seams only: the process from outside (NATS, HTTP, `/metrics`) and the pure correlation module. Five crash points, a soak test with random kills and an invariant check back every claim about kills.

## Assumptions and Trade-offs

**Where the brief is silent or ambiguous, and what I decided**
- *No skeleton arrived*, so the service provisions `ANALYTICS_EVENTS` and `episoder` itself when they are missing, with the brief's settings: AckWait 30s and MaxDeliver 10, unchanged. Existing ones are used as they are.
- *Closing a silent camera.* Membership uses `occurred_at` only. Settling measures silence on the Postgres clock, from the arrival of the episode's own latest event, because device time cannot tell that a camera went quiet. An episode that already has a successor settles after `EPISODE_SETTLE_GRACE`, so a chapter cut at the max duration is announced while the camera stays busy. A chapter is cut only by an event at or past its boundary; there is no timer.
- *Late events* go where a device-time walk puts them. If that episode has settled, the event becomes a correction: aggregates change, `revision` rises, one `episode.corrected.*` message. `episode.settled.*` is never repeated. Settled episodes never merge or split, so a correction can leave two episodes closer than the quiet window.
- *Exactly once* holds for JetStream readers within the 2-minute duplicate window of `EPISODES`. A core NATS subscriber can see a copy after a kill between publish and commit. `EPISODES` is created only for subjects no other stream covers.
- *Duplicates:* the first stored copy of an `event_id` wins, even with a different payload.
- *Payload vs subject:* the payload wins, with a warning in the log. A `site_id` or `camera_id` that cannot be a NATS token is invalid.
- *Invalid events* are counted and logged, not stored. Invalid also covers invalid UTF-8, U+0000, ids over 255 bytes and non-RFC 3339 times. Leap seconds are refused.
- *Formats:* times are stored to the microsecond and returned in UTC with milliseconds, or microseconds when present. `attributes` come back byte for byte, apart from whitespace.
- *API rules:* empty parameters count as absent, `limit` above 200 is clamped, and bad values get 400. The cursor is opaque. Pages are stable only for episodes that do not change meanwhile; an open episode merged into another returns 404.
- *Restart guard:* an instance settles nothing for one quiet window after it starts, so that it reads the events that queued up while it was down. A backlog that takes longer than that to read can still settle an episode early; its late events then become corrections.
- *Postgres outage:* ingest pauses and `/readyz` says 503. Checked by hand with Postgres down for 57s: nothing was lost, and no message used more than two deliveries.

**Deliberately not done:** nothing graded as secondary was dropped. Out of scope:
- persisting invalid events;
- `revision` in the HTTP object;
- cleaning up published outbox rows;
- sharding keys across instances.

**With a week instead of a day:**
- Restart guard: replace the fixed window with one based on the consumer's backlog.
- Relay: wake it on commit (`LISTEN/NOTIFY`) instead of polling.
- Scale: partition the input by key with a subject transform, and measure throughput.
- Outbox: add a retention job.
- Monitoring: add consumer lag and settle-latency metrics.
- API: ask whether `revision` may join the HTTP object.

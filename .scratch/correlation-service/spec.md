# Spec: Panoptes correlation service

Status: done

Vocabulary follows `GLOSSARY.md`. The decisions behind this spec are ADR-0001 to ADR-0005 in `docs/adr/`; this spec does not repeat their reasoning.

## Problem Statement

Edge devices at customer sites run detection models on camera streams and publish every detection to NATS JetStream. A busy camera produces dozens of events a minute, and operators do not want dozens of alerts: they want one alert per thing that happened. The stream is delivered at least once, so duplicates, redeliveries, out-of-order events and late events are normal. The service that groups events may run as several instances and may be killed at any moment. Today nothing turns this stream into operator-facing units, stores them, lets anyone query them, or tells the rest of the platform when one has ended.

## Solution

Panoptes consumes the event stream and groups events into episodes by episode key, using device time for every correlation rule: a gap longer than the quiet window starts a new episode, and the max duration cuts long activity into chapters. It stores events and episodes in Postgres, serves them over an HTTP query API, settles each episode once it has been silent long enough, and announces it exactly once on `episode.settled.<site_id>.<camera_id>`. A late event that belongs to a settled episode becomes a correction, announced on `episode.corrected.<site_id>.<camera_id>` with a revision. Any number of identical instances can run side by side, and a kill at any step loses nothing and announces nothing twice to a JetStream reader.

## User Stories

### Episodes

1. As an operator, I want all events of one kind at one camera that follow each other closely to form one episode, so that I get one alert per thing that happened.
2. As an operator, I want a new episode to start when the device-time gap to the previous event exceeds the quiet window, so that two separate incidents are not merged.
3. As an operator, I want an event whose gap equals the quiet window exactly to stay in the current episode, so that only a gap that exceeds the window splits.
4. As an operator, I want long activity cut into chapters at the max duration, so that I hear about a continuing incident at least once per max duration.
5. As an operator, I want an event exactly on a chapter boundary to start the next chapter, and no event to fall between chapters, so that chapters add up to the whole activity.
6. As an operator, I want each episode's peak severity to be the highest severity by rank, so that a `critical` event is never hidden behind a `warning` because of string comparison.
7. As an operator, I want first seen, last seen and event count to describe the episode's own events, so that I can judge how long and how intense the activity was.
8. As an operator, I want grouping to follow device time rather than arrival time, so that network delays do not change which events belong together.
9. As an operator, I want an open episode to come out the same whatever order its events arrived in, so that delivery order never changes what I see.
10. As an operator, I want different kinds at the same camera to form separate episodes, so that a `tamper` is never folded into a `person_detected` episode.

### Settling and announcements

11. As a platform service, I want exactly one announcement on `episode.settled.<site_id>.<camera_id>` per episode, so that I raise exactly one alert.
12. As a platform service, I want the announcement to carry the episode object from the brief with `episode_id` in place of `id`, so that I can act on it without calling the API.
13. As a platform service, I want an episode to settle about one quiet window after its last event arrived when the camera falls silent, so that alerts are timely.
14. As a platform service, I want a chapter ended at the max duration to settle within the settle grace even while the camera stays busy, so that long activity does not delay the earlier chapter's alert.
15. As a platform service, I want the close reason to be `max_duration` when the next chapter started and `quiet` otherwise, so that I can tell a cut chapter from an ended incident.
16. As a platform service, I want to learn about a late event that joined a settled episode through one message on `episode.corrected.<site_id>.<camera_id>`, so that I can update my record without raising a second alert.
17. As a platform service, I want every correction to carry the episode's revision, so that I can keep the latest state when corrections arrive out of order.
18. As a platform service, I want a late event to land in the episode a device-time walk puts it in, so that chapter boundaries stay where they were (an event at 10:09:45 joins the chapter that started at 10:00:00, not the one that started at 10:10:00).

### Ingest

19. As a platform engineer, I want each `event_id` stored exactly once however many times the stream delivers it, so that counts are true.
20. As a platform engineer, I want a duplicate to change no episode and delay no settle, so that a redelivery storm is harmless.
21. As a platform engineer, I want a message that is not JSON, lacks a required field, or has an unknown kind or severity to be acknowledged and counted in `episoder_invalid_events_total`, so that one bad device never stalls the stream.
22. As a platform engineer, I want each invalid event logged with its reason, its subject and the start of its payload, so that I can tell the device team what is wrong.
23. As a platform engineer, I want an event whose `site_id` or `camera_id` cannot be a NATS subject token to be treated as invalid, so that every stored episode can be announced.
24. As a platform engineer, I want the payload to win when the subject tokens disagree with it, with a warning in the log, so that the stored event matches what the device reported.
25. As a platform engineer, I want `attributes` stored and returned verbatim, so that device-specific data survives without the service interpreting it.

### Query API

26. As an API client, I want to list episodes filtered by `site_id`, `camera_id`, `kind` and `status`, so that I can see one camera or one kind of activity.
27. As an API client, I want `from` (inclusive) and `to` (exclusive) to filter on first seen, so that consecutive time ranges never overlap.
28. As an API client, I want episodes ordered by first seen descending with id descending as a tiebreak, so that the newest activity comes first and the order is total.
29. As an API client, I want a default page of 50 and a maximum of 200, with larger limits clamped rather than rejected, so that I cannot overload the service by accident.
30. As an API client, I want an opaque cursor returned as `next_cursor`, empty on the last page, so that I can walk all pages without skipping or repeating an unchanged episode.
31. As an API client, I want 400 for a cursor that does not decode and for malformed parameters, with a readable message, so that I find my own mistakes quickly.
32. As an API client, I want `GET /episodes/{id}` to return the episode with its events ordered by device time ascending, so that I can replay what happened.
33. As an API client, I want 404 for an unknown or malformed id, so that a vanished open episode and a typo look the same.
34. As an API client, I want `close_reason` and `settled_at` to be null while an episode is open, so that I can tell open from settled at a glance.
35. As an API client, I want an episode's event list and its event count read in one snapshot, so that they never disagree.

### Operating the service

36. As an on-call engineer, I want to run several identical instances against the same stream and database with no extra configuration, so that I can scale and survive a node loss.
37. As an on-call engineer, I want a kill at any step to lose no event and announce no episode twice to a JetStream reader, so that a crash is a non-event.
38. As an on-call engineer, I want a restarted service not to settle episodes whose events are still waiting in the stream, so that downtime does not produce early alerts and corrections.
39. As an on-call engineer, I want the service to stop pulling events while Postgres is unavailable, so that events do not use up their delivery attempts.
40. As an on-call engineer, I want `/healthz` to report that the process is alive and `/readyz` to report whether Postgres and NATS are reachable and the schema is current, so that an orchestrator can route and restart correctly.
41. As an on-call engineer, I want SIGTERM to finish events, settles and announcements already in flight before exit, so that a deploy causes no redelivery burst.
42. As an on-call engineer, I want Prometheus metrics for ingest outcomes, settles by close reason, corrections, outbox backlog, publish failures and ingest latency, so that I can see the pipeline's health.
43. As an on-call engineer, I want structured JSON logs that carry the episode key, `event_id` and episode id, so that I can follow one event through the pipeline.
44. As an on-call engineer, I want the quiet window, max duration, settle grace and addresses read from the environment with the brief's defaults, so that nothing is hard-coded.
45. As an on-call engineer, I want the service to create the input stream, the `episoder` consumer and the episode stream when they are missing and to leave existing ones alone, so that it works on an empty broker and respects a broker someone else set up.

### Reviewing and extending

46. As a reviewer, I want `docker compose up` to run the whole system on a machine with only Docker, so that I can try it in minutes.
47. As a reviewer, I want `make up && make test` to run every test in containers, so that I need no Go toolchain and the tests do not disturb the running stack.
48. As a reviewer, I want a fixture generator that publishes a realistic mix of events, duplicates, shuffled and invalid messages, so that I can watch episodes form during the demo.
49. As a developer, I want the correlation rules in one pure module with table tests written as timelines, so that I can change a rule live and see exactly which cases move.
50. As a developer, I want named crash points that kill the process at a chosen step, so that every "what if it dies here" answer is backed by a test.

## Implementation Decisions

### Modules

- **Event codec.** Turns a raw message (subject and payload) into an event or an invalid-event reason. Small interface, all validation inside.
- **Correlation.** Pure, no IO: given a new event and the episode key's current state, decides whether the event joins a settled episode as a correction or rebuilds the key's open episodes, and returns the resulting episodes with their aggregates. This is the deep module of the service and one of the two test seams.
- **Store.** All Postgres access: migrations, the ingest transaction, the settle transaction, outbox claiming and the two queries behind the API. It calls the correlation module inside the ingest transaction; callers never see SQL.
- **Ingest loop.** Pulls from the `episoder` consumer, runs each message through the codec and the store with a small fixed pool of workers, and acknowledges only after the commit.
- **Settle loop.** Polls a few times per second for open episodes that are due and settles each in its own transaction.
- **Relay.** Publishes unpublished outbox rows to JetStream and marks them published.
- **HTTP API.** `GET /episodes`, `GET /episodes/{id}`, `/healthz`, `/readyz`, `/metrics`.
- **Config.** Reads and validates the environment once at startup, and fails fast with a clear message.
- **Crash points.** Named points on the ingest, settle and publish paths that exit the process when the test environment names them. Without that variable, each one does nothing.
- **App.** Wires the modules, provisions the broker, runs the four loops and orchestrates shutdown.

### Event codec rules

- Required fields: `event_id`, `site_id`, `camera_id`, `kind`, `severity`, `occurred_at`. A field that is missing, null, an empty string or of the wrong JSON type makes the event invalid.
- `kind` must be one of `person_detected`, `loitering`, `line_crossed`, `tamper`. `severity` must be one of `info`, `notice`, `warning`, `critical`, ranked 1 to 4 in that order.
- `occurred_at` must be RFC 3339. Any offset is accepted and normalised to UTC. Precision is kept to the microsecond, which is what Postgres stores.
- `site_id` and `camera_id` must be valid NATS subject tokens: no `.`, `*`, `>` or whitespace.
- `event_id` must be non-empty. It is not checked to be a ULID, since the brief lists only missing fields and unknown sets as invalid.
- The payload must be valid UTF-8, and no string field may contain U+0000. Postgres text cannot store either, so such an event would fail its insert on every delivery and never be counted.
- `event_id`, `site_id` and `camera_id` are at most 255 bytes, so that they always fit a btree index entry.
- `occurred_at` follows RFC 3339 exactly: `T` and `Z` in either case, a fraction only after a dot.
- As a backstop, an insert that Postgres rejects for the data itself (SQLSTATE classes 22 and 54) is treated as an invalid event: acknowledged and counted, not retried.
- `attributes` is stored as given, whatever JSON value it is. When missing it is stored as an empty object.
- Unknown extra fields are ignored. Subject tokens are not used for decoding. When they disagree with the payload, the payload wins and a warning is logged.
- Invalid events are acknowledged, counted and logged with the reason, the subject and the first 256 bytes of the payload. They are not persisted.

### Correlation rules

These rules restate ADR-0002, ADR-0003 and ADR-0005 precisely enough to implement.

- **Walk.** Sort events by device time, ties by `event_id`. An event joins the current group when its gap to the group's last seen is at most the quiet window and it lies before the group's first seen plus the max duration. Otherwise it starts a new group.
- **Ended by device time.** An open group that is followed by another episode of the same key, open or settled, has ended by device time. Its pending close reason is `max_duration` when the next episode starts at or after the group's chapter boundary and within one quiet window of its last seen; otherwise it is `quiet`.
- **Placement of a new event.** Let P be the key's episode with the greatest first seen at or before the event's device time, and N the episode with the smallest first seen after it. P is within reach when the event is no more than one quiet window after P's last seen and before P's chapter boundary. N is within reach when the event is no more than one quiet window before N's first seen and N's last seen minus the event's device time is less than the max duration. The target is P if it is within reach, otherwise N if it is within reach, otherwise none.
- **Correction.** When the target is settled, the event joins it. Its aggregates are recomputed from its events and its revision rises by one. `settled_at` and the close reason stay as they were, and one corrected announcement is queued. The key's open episodes are then rebuilt without the event, because the corrected episode may now start earlier and so change the pending close reason of the open episode before it.
- **Rebuild.** When the target is open or there is none, all open episodes of the key are rebuilt by the walk. Settled episodes act as fixed dividers, so no open group spans one.
- **Identity on rebuild.** Groups are visited in device-time order. Each one keeps the id of the old open episode that owned its earliest previously stored event, unless an earlier group already took that id; otherwise it gets a new UUID v4. Old open episodes whose ids no group keeps are deleted.
- **Aggregates.** First seen, last seen, event count and peak severity by rank are always recomputed from the episode's events, never patched.

### Settling

- An open episode is due when its silence, measured on the Postgres clock from the arrival of the latest of its own events, reaches the settle grace if it has ended by device time, or the quiet window if it has not.
- A settle takes the episode key's lock, re-reads the episode and checks that it is still open and still due. It then sets status `settled`, `settled_at` from the Postgres clock and the close reason (the pending one, or `quiet`), and writes the announcement into the outbox, all in one transaction.
- Restart guard: an instance settles nothing until it has run for one quiet window.
- There is no max-duration timer. A chapter is cut only by an event at or past its boundary.

### Ingest transaction

One transaction per delivery, in this order:
1. Take `pg_advisory_xact_lock` on a 64-bit hash of the episode key. A hash collision only serialises two unrelated keys.
2. Insert the event with its arrival time from the Postgres clock, `ON CONFLICT DO NOTHING`. If nothing was inserted it is a duplicate: commit and return. First write wins, even when a later copy carries a different payload.
3. Load the correlation input: the key's open episodes with their events, the settled episodes among and next to them (bounds only), and P and N for the new event.
4. Apply the correlation result: write episodes, reassign events, delete vanished open episodes and queue a corrected announcement if there was a correction.
5. Commit, then acknowledge the message.

What a kill does at each step: a kill before the commit rolls everything back and the redelivery does all of it again. A kill after the commit but before the ack makes the redelivery a duplicate, which returns at step 2. A lost ack has the same effect. While Postgres is unavailable the loop stops fetching, marks itself not ready and retries the connection with backoff, so in-flight messages spend at most one delivery attempt per outage.

### Announcements and the outbox

- The settle or correction transaction writes an outbox row: the subject, a message id (`settled:<episode_id>` or `corrected:<episode_id>:<revision>`) and a snapshot of the payload as of that transaction.
- The settled payload is the brief's episode object with `episode_id` in place of `id` and no other fields. The corrected payload is the same object plus `revision`.
- Every instance runs a relay. It claims up to a batch of unpublished rows in id order with `FOR UPDATE SKIP LOCKED`, publishes each to JetStream with `Nats-Msg-Id` and waits for the publish acknowledgement, marks the rows published, and commits.
- A row can be claimed only when no earlier unpublished row exists for the same episode. A correction therefore never reaches the stream before its episode's settled announcement or an earlier correction, even with several relays.
- What a kill does at each step: before the publish, the rows stay unpublished and are claimed again. After the publish but before the commit, they are published again with the same message id, and JetStream drops the copy within its duplicate window. When a publish fails, the rows published before it in the batch are still marked, the rest stay unpublished, and the relay backs off.
- Published rows are kept, marked with the publish time. Cleaning them up is out of scope.

### Schema

- **events:** `event_id` primary key, episode key fields, severity rank, device time, attributes as `json` (which keeps the device's text; `jsonb` reorders keys and rejects `\u0000`), arrival time, and the id of the episode it belongs to. Indexed by episode and device time; no query reads events by key, so there is no index for that.
- **episodes:** UUID id, episode key fields, status, first seen, last seen, event count, peak severity rank, close reason, `settled_at`, revision, the latest arrival time among its events, and, for open episodes, the pending close reason when it has ended by device time. Indexed for the list query (first seen and id, descending, with the filters) and with a partial index on open episodes by latest arrival for the settle sweep.
- **outbox:** serial id, episode id, subject, unique message id, payload, created and published times, with a partial index on unpublished rows.
- Migrations are plain SQL embedded in the binary and applied at startup under a session advisory lock, each in its own transaction. Concurrent instances therefore never race.

### HTTP API

- `GET /episodes`: optional `site_id`, `camera_id`, `kind`, `status`, `from`, `to`, `limit`, `cursor`. An empty parameter counts as absent. A `kind` or `status` outside its set, a `from` or `to` that is not RFC 3339, or a `limit` that is not a positive integer returns 400. A limit above 200 is clamped to 200. When `from` is not before `to` the result is empty.
- Keyset pagination: fetch one row more than the limit. When it exists, `next_cursor` encodes the last returned row's first seen and id. The next page selects rows strictly after that position in the ordering. The cursor is base64url of a small versioned structure. Anything that fails to decode or parse returns 400.
- The list is always an array, never null. All timestamps are UTC with a `Z` suffix and millisecond precision, or microseconds when sub-millisecond digits are present. `close_reason` and `settled_at` are null while open.
- `GET /episodes/{id}`: the episode and its events in one read-only snapshot, events ordered by device time ascending, ties by `event_id`. Each event carries `event_id`, `severity`, `occurred_at` and `attributes`. An unknown id or one that is not a UUID returns 404.
- Errors are JSON: `{"error": "<message>"}`.
- An open episode can still change its first seen or vanish into a merge, so pagination is stable only for episodes that do not change while a client is paging.

### Broker provisioning

- At startup, create `ANALYTICS_EVENTS` (subjects `analytics.event.v1.>`, WorkQueue retention, file storage) if it is missing, and the durable pull consumer `episoder` (explicit ack, AckWait 30s, MaxDeliver 10) if it is missing. Existing ones are used as they are.
- Create the stream `EPISODES` (limits retention, file storage, duplicate window 2 minutes) for whichever of `episode.settled.>` and `episode.corrected.>` no existing stream covers yet, and skip it when both are covered. JetStream rejects overlapping streams, so a stream on `episode.>` would fail next to a suite stream that covers only one of them.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `EPISODE_QUIET_WINDOW` | `60s` | Quiet window, and the silence that settles an episode |
| `EPISODE_MAX_DURATION` | `10m` | Max duration of one chapter |
| `EPISODE_SETTLE_GRACE` | `1s` | Silence that settles an episode that has ended by device time (ours) |
| `NATS_URL` | `nats://nats:4222` | Broker |
| `DATABASE_URL` | none, required | Postgres |
| `HTTP_ADDR` | `:8080` | API, health and metrics listener |
| `PANOPTES_CRASH_AT` | empty | Test only: the crash point at which to exit |

Durations use Go duration syntax. The quiet window and max duration must be positive, and the grace must be zero or more.

### Operability

- `/healthz` returns 200 while the process runs. `/readyz` returns 200 only when migrations are applied, Postgres answers a ping, NATS is connected and shutdown has not begun; otherwise it returns 503.
- Metrics: `episoder_invalid_events_total` (counter, no labels, the brief's contract) and our own:
  - `panoptes_ingested_events_total{result="new|duplicate"}`
  - `panoptes_episodes_settled_total{close_reason}`
  - `panoptes_episode_corrections_total`
  - `panoptes_outbox_pending`, a gauge sampled by the relay
  - `panoptes_publish_failures_total`
  - `panoptes_ingest_duration_seconds`, a histogram
- Logs: `log/slog` JSON to stdout, one line per invalid event, settle, correction and publish failure. Normal ingest is logged at debug level.
- Shutdown on SIGTERM or SIGINT: `/readyz` turns 503, fetching stops, in-flight ingest transactions finish and are acknowledged, the settle loop and the relay finish their current unit, the HTTP server shuts down, then the NATS connection closes, which flushes the final acks, and the pool closes. The overall deadline is 20 seconds, and compose gives the container 30.

### Crash points

The crash points are `ingest.before_commit`, `ingest.after_commit` (before the ack), `settle.before_commit`, `relay.before_publish` and `relay.after_publish` (before the mark). When `PANOPTES_CRASH_AT` names the point being passed, the process logs one line and exits at once with a non-zero code. No deferred cleanup and no graceful drain run, which from the outside looks like a kill.

### Packaging

- Multi-stage Dockerfile that produces a static binary in a minimal image.
- `docker-compose.yml`: NATS with JetStream, Postgres and Panoptes, with health checks and `depends_on` on healthy services. A second Panoptes instance runs under a profile for the multi-instance demo.
- Makefile targets: `up`, `down`, `logs`, `test` and `demo`. `test` runs `go test ./...` in a Go container, in a separate compose project with its own NATS and Postgres, and tears the project down afterwards.
- Fixture generator: a small command that publishes a scenario (busy camera across a chapter boundary, duplicates, shuffled order, a late event, invalid messages) for the demo.
- Libraries: `nats.go` with its `jetstream` package, `pgx/v5` with plain SQL, `net/http` with method and path patterns, `prometheus/client_golang`, `log/slog`. No ORM and no web framework.

## Testing Decisions

### What makes a good test here

- It observes behaviour through one of the two seams and never inspects SQL, tables or internal types.
- It states its scenario as a device-time timeline with hand-written expectations. Expected episodes are never computed with the module under test.
- It would fail if the rule it names were broken. Boundaries are tested exactly on the line and one millisecond to either side.
- No mocks of our own modules. Seam A uses real NATS and Postgres; seam B needs none.

### Seam A: the process, from the outside

The grading suite uses this seam: publish to `analytics.event.v1.*`, read `GET /episodes`, `GET /episodes/{id}` and `/metrics`, and read announcements from the episode stream through a JetStream consumer filtered to the test's site.

- The harness builds the binary once, starts one or more instances as child processes with short windows (quiet window 1s, max duration 5s, settle grace 200ms; device times in payloads are arbitrary), and can stop them with SIGTERM or SIGKILL or set a crash point.
- The package skips itself when the broker and database addresses are not set, so `go test ./...` on a host with Go runs only seam B.
- Each test uses its own random `site_id`. Tests run one after another, because all instances share the one `episoder` consumer, and a test that relies on a crash point must have no other instance consuming its events.
- Scenarios:
  - Happy path: events form one episode, it settles `quiet`, one announcement goes out, and the API shows the episode with its events.
  - Duplicates: every event published three times, including a copy with a different payload. Each is stored once, counts are right and first write wins.
  - Out-of-order arrival: a shuffled timeline gives the same episodes as the sorted one.
  - Quiet window: a gap equal to the window stays in one episode, a gap one millisecond longer splits.
  - Chaptering: an event exactly on the boundary starts the next chapter, close reasons are `max_duration` then `quiet`, and event counts add up.
  - Peak severity by rank: `critical` with `warning` gives `critical`.
  - Invalid input, one case per class (not JSON, each required field missing, unknown kind, unknown severity, bad timestamp, bad subject token): the counter rises by one each time, nothing is stored, and a valid event published after them is processed.
  - Late event: it joins the settled episode, one corrected message goes out with revision 1 after the settled one, and no second settled message. Includes the 10:09:45 boundary timeline.
  - API: every filter, `from` inclusive and `to` exclusive, ordering with equal first seen, walking all pages without repeats or gaps, default and clamped limit, 400 for a bad cursor and bad parameters, 404 for unknown and malformed ids, and null `close_reason` and `settled_at` while open.
  - Restart guard: after a kill, an episode whose next events wait in the stream is not settled early.
  - Crash points: one test per point, each followed by a clean restart and the invariants below.
  - Two instances: interleaved keys with duplicates, consumed by both; checked against the invariants.
  - Soak: two instances under load, killed at random moments and restarted, then checked against the invariants.
  - Graceful shutdown: SIGTERM under load, the process exits zero within the deadline, and everything it acknowledged is stored.
- Invariants checked after every kill: every published valid `event_id` is stored once and belongs to exactly one episode; the episode stream holds exactly one settled message per settled episode; and each episode's aggregates match its event list.

### Seam B: the correlation module

- Table-driven timelines: a new event, the key's current episodes (open and settled), the expected decision and the expected resulting episodes with their aggregates and pending close reasons.
- Cases:
  - gap equal to the quiet window, and one millisecond longer;
  - an event exactly on the chapter boundary, and one millisecond before it;
  - an early event that moves first seen back and splits an open episode into chapters;
  - a bridging event that merges two open episodes, and which id survives;
  - placement into P, into N, and into neither, in the gap between settled episodes;
  - the 10:09:45 late event;
  - N rejected because joining it would reach the max duration;
  - peak severity by rank.
- A property test with random timelines and random arrival orders, and no settled episodes: the final episodes equal one walk over the sorted events. Every event is in exactly one episode, no gap inside an episode exceeds the quiet window, and no span reaches the max duration.

### Prior art

The repository has no code yet. Tests follow standard Go table-driven style with the standard `testing` package only.

## Out of Scope

- Authentication, authorisation, multi-tenancy, a user interface, Kubernetes or Terraform, CI, coverage targets, performance tuning and load testing (all excluded by the brief).
- Persisting invalid events. They are logged and counted only.
- A revision field in the HTTP response or in `episode.settled.*`, which the brief's fixed shapes leave out.
- Deduplicating announcements beyond JetStream's duplicate window, and for core NATS subscribers.
- Re-merging neighbouring settled episodes that a correction brought within one quiet window.
- A max-duration timer that settles a chapter without an event past its boundary.
- Cleaning up published outbox rows.
- Sharding by episode key or splitting the binary into roles (see ADR-0001).

## Further Notes

- The questionnaire to District Cyber is still unanswered. The spec follows its stated defaults (server-clock settling, corrections, payload wins, the service creates the episode stream, exact shapes), and each one goes into the README's "Assumptions and Trade-offs".
- The README must also record: the limit, timestamp and parameter rules above; first write wins on a duplicate `event_id`; microsecond precision; the restart guard and what a backlog longer than the quiet window does; the 2-minute duplicate window; and which secondary items were dropped, if any.
- Corrections can bring two neighbouring episodes closer than one quiet window. This is accepted in ADR-0005.
- The tickets under `issues/` are implemented one at a time, in number order, even where their blocking edges would allow parallel work. Each step then builds on the one before it and can be explained in that order. The numbering already respects every blocking edge.

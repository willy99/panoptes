# 02: Events in, episodes out

**What to build:** A valid event published to `analytics.event.v1.<site_id>.<camera_id>.<kind>` is stored exactly once and is acknowledged only after its transaction commits. For now each episode key has a single open episode that holds all of its events; this is a deliberate stand-in that ticket 03 replaces with the correlation rules. The episode's aggregates (first seen, last seen, event count, peak severity by rank) are recomputed from its events on every change. `GET /episodes?site_id=` lists episodes newest first without pagination, and `GET /episodes/{id}` returns the episode with its events. The ingest transaction follows the spec's steps (key lock, insert with `ON CONFLICT DO NOTHING`, write the episode, commit, ack), and the code on that path states what a kill between its steps does. Messages the codec cannot decode are acknowledged and logged; counting them is ticket 04. Spec stories 6, 7, 10, 19, 20, 25, 32, 33, 34, 35.

**Blocked by:** 01 (Walking skeleton)

**Status:** done

- [x] End-to-end: three events of one key form one open episode with the right first seen, last seen, event count and peak severity.
- [x] End-to-end: `GET /episodes/{id}` returns the events ordered by device time ascending, each with `event_id`, `severity`, `occurred_at` and `attributes` exactly as published.
- [x] End-to-end: every event published three times, one copy with a different payload, is stored once, and the first stored copy wins.
- [x] End-to-end: two kinds at one camera form two episodes.
- [x] End-to-end: `critical` together with `warning` gives peak severity `critical`.
- [x] End-to-end: an open episode has status `open` and null `close_reason` and `settled_at`, and every field name and timestamp format matches the spec.
- [x] End-to-end: an unknown id and a malformed id both return 404 with a JSON error.
- [x] The episode and its events are read in one snapshot.

## Comments

Done in e30780d (codec), ec4bc47 (schema), 4c31318 (ingest path), 2c02d14 (API and end-to-end tests) and 865cdf6 (fixes from the two-axis review).

Decisions taken here:
- `attributes` is a `json` column, not `jsonb`: `json` keeps the device's text, while `jsonb` reorders keys and rejects `\u0000`, which would fail such an event on every delivery. The spec's schema section now says so.
- A failed ingest transaction leaves its message unacknowledged instead of naking it, so AckWait paces the retries. A short nak delay would use up MaxDeliver within seconds of a database outage.
- The first stored copy of an event wins, not the first published: workers run concurrently, so two copies published back to back can be stored in either order.

Checked by breaking the code on purpose: last-write-wins, peak severity by `min`, and a `jsonb` column each make a test fail. The `jsonb` check needs a fresh test database, because an applied migration is not read again.

Found here and handed to ticket 04: an `event_id` containing `\u0000`, invalid UTF-8, and ids longer than a btree entry pass the codec but fail the insert on every delivery. Lowercase `t`/`z` in `occurred_at` is rejected by Go although RFC 3339 allows it, and a comma fraction is accepted although it does not.

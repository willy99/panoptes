# 02: Events in, episodes out

**What to build:** A valid event published to `analytics.event.v1.<site_id>.<camera_id>.<kind>` is stored exactly once and is acknowledged only after its transaction commits. For now each episode key has a single open episode that holds all of its events; this is a deliberate stand-in that ticket 03 replaces with the correlation rules. The episode's aggregates (first seen, last seen, event count, peak severity by rank) are recomputed from its events on every change. `GET /episodes?site_id=` lists episodes newest first without pagination, and `GET /episodes/{id}` returns the episode with its events. The ingest transaction follows the spec's steps (key lock, insert with `ON CONFLICT DO NOTHING`, write the episode, commit, ack), and the code on that path states what a kill between its steps does. Messages the codec cannot decode are acknowledged and logged; counting them is ticket 04. Spec stories 6, 7, 10, 19, 20, 25, 32, 33, 34, 35.

**Blocked by:** 01 (Walking skeleton)

**Status:** ready-for-agent

- [ ] End-to-end: three events of one key form one open episode with the right first seen, last seen, event count and peak severity.
- [ ] End-to-end: `GET /episodes/{id}` returns the events ordered by device time ascending, each with `event_id`, `severity`, `occurred_at` and `attributes` exactly as published.
- [ ] End-to-end: every event published three times, one copy with a different payload, is stored once, and the first stored copy wins.
- [ ] End-to-end: two kinds at one camera form two episodes.
- [ ] End-to-end: `critical` together with `warning` gives peak severity `critical`.
- [ ] End-to-end: an open episode has status `open` and null `close_reason` and `settled_at`, and every field name and timestamp format matches the spec.
- [ ] End-to-end: an unknown id and a malformed id both return 404 with a JSON error.
- [ ] The episode and its events are read in one snapshot.

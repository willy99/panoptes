# 04: Invalid events are counted and skipped

**What to build:** A message that is not JSON, lacks a required field, has a `kind` or `severity` outside its set, has an `occurred_at` that is not RFC 3339, or has a `site_id` or `camera_id` that cannot be a NATS subject token never stalls the stream. It is acknowledged, counted in `episoder_invalid_events_total` on `/metrics`, logged with its reason, its subject and the first 256 bytes of its payload, and not persisted. When subject tokens disagree with the payload, the payload wins and a warning is logged. The codec follows every rule in the spec's "Event codec rules". Spec stories 21, 22, 23, 24.

**Blocked by:** 02 (Events in, episodes out)

**Status:** ready-for-agent

- [ ] `/metrics` serves Prometheus text and shows `episoder_invalid_events_total`, without labels, at 0 before any invalid event arrives.
- [ ] End-to-end, one case per class (not JSON; each required field missing, null, empty or of the wrong type; unknown kind; unknown severity; bad timestamp; a dot in `site_id`; a dot in `camera_id`): the counter rises by exactly one and nothing is stored.
- [ ] End-to-end: a valid event published after a batch of invalid ones is stored, and after a further wait the counter has not grown, so nothing was redelivered.
- [ ] End-to-end: an event whose subject names a different camera than its payload is stored under the payload's ids.
- [ ] End-to-end: missing `attributes` is returned as an empty object, and an `attributes` value that is not an object is returned as given.

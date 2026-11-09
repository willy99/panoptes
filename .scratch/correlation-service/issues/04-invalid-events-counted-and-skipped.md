# 04: Invalid events are counted and skipped

**What to build:** A message that is not JSON, lacks a required field, has a `kind` or `severity` outside its set, has an `occurred_at` that is not RFC 3339, or has a `site_id` or `camera_id` that cannot be a NATS subject token never stalls the stream. It is acknowledged, counted in `episoder_invalid_events_total` on `/metrics`, logged with its reason, its subject and the first 256 bytes of its payload, and not persisted. When subject tokens disagree with the payload, the payload wins and a warning is logged. The codec follows every rule in the spec's "Event codec rules". Spec stories 21, 22, 23, 24.

**Blocked by:** 02 (Events in, episodes out)

**Status:** done

- [x] `/metrics` serves Prometheus text and shows `episoder_invalid_events_total`, without labels, at 0 before any invalid event arrives.
- [x] End-to-end, one case per class (not JSON; each required field missing, null, empty or of the wrong type; unknown kind; unknown severity; bad timestamp; a dot in `site_id`; a dot in `camera_id`): the counter rises by exactly one and nothing is stored.
- [x] End-to-end: a valid event published after a batch of invalid ones is stored, and after a further wait the counter has not grown, so nothing was redelivered.
- [x] End-to-end: an event whose subject names a different camera than its payload is stored under the payload's ids.
- [x] End-to-end: a payload that is not valid UTF-8, an `event_id` containing `\u0000`, and an `event_id` longer than 255 bytes are counted as invalid. Before this ticket, the second failed its insert on every delivery (`SQLSTATE 22021`) until MaxDeliver ran out.
- [x] End-to-end: `occurred_at` with a lowercase `t` and `z` is valid, and one with a comma before the fraction is not.
- [x] End-to-end: missing `attributes` is returned as an empty object, and an `attributes` value that is not an object is returned as given.

## Comments

Done in the commit after 8c26383. Beyond the original list, the codec also refuses invalid UTF-8, U+0000 in a string field and ids over 255 bytes, which ticket 02's review found could fail an insert on every delivery. An insert that Postgres still refuses for the data (SQLSTATE class 22 or 54) is counted as invalid rather than retried; the codec catches every known case first, so no test reaches that backstop on its own, but it is what makes a missed case harmless.

Leap seconds (`:60`) are refused, because Go's time parser does not accept them. The README should say so.

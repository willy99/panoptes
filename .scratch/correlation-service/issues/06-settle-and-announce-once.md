# 06: Settle and announce once

**What to build:** An open episode settles once it is due by the spec's rule. Silence, measured on the Postgres clock from the arrival of the latest of its own events, must reach the quiet window, or only the settle grace when the episode has ended by device time. The settle transaction takes the key's lock, re-checks that the episode is still open and due, sets status `settled`, `settled_at` and the close reason, and writes an outbox row with a snapshot of the payload. A relay in every instance claims unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes each to `episode.settled.<site_id>.<camera_id>` with `Nats-Msg-Id: settled:<episode_id>`, waits for the publish acknowledgement and marks the row published. The code on the settle and relay paths states what a kill between their steps does. The restart guard comes in ticket 08. Spec stories 11, 12, 13, 14, 15.

**Blocked by:** 03 (Correlation by device time)

**Status:** done

- [x] End-to-end: a camera that falls silent has its episode settled as `quiet` about one quiet window after its last event arrived, with `settled_at` set and status `settled`.
- [x] End-to-end: exactly one message arrives on `episode.settled.<site_id>.<camera_id>`. It is the episode object with `episode_id` in place of `id` and no other fields.
- [x] End-to-end: for a busy camera across a chapter boundary, the first chapter settles as `max_duration` within the settle grace while events keep arriving, and the second settles as `quiet` after the camera falls silent.
- [x] End-to-end: `GET /episodes?status=settled` and `?status=open` split the episodes correctly.
- [x] End-to-end: after a further wait, the `EPISODES` stream holds exactly one settled message per settled episode.
- [x] End-to-end: re-publishing an already stored event after its episode settled changes nothing and announces nothing.

## Comments

Done in two commits after 30ab3fe: the contract package (the brief's shapes, shared by the API and the outbox), then the settle loop, the outbox and the relay with their tests. The end-to-end tests passed twice in a row.

- `settled_at` is the settle transaction's `now()`, on the Postgres clock like the arrival times.
- When a publish fails partway through a batch, the rows published before it are still marked, so fewer duplicates reach the stream's dedup window.
- On shutdown the relay stops with the other loops, so an episode settled in the last moments is announced by the next instance to start. Ticket 10 can let the relay drain first.

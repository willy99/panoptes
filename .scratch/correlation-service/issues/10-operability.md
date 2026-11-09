# 10: Operability

**What to build:** An on-call engineer can see the pipeline's health and can deploy without causing a redelivery burst. This covers the spec's `panoptes_*` metrics; JSON logs carrying the episode key, `event_id` and episode id; a graceful shutdown that drains work in flight within 20 seconds; `/readyz` turning 503 during shutdown and while Postgres or NATS is unreachable; and ingest pausing with backoff while Postgres is unavailable, so events do not use up their delivery attempts. The brief grades these items as secondary. If time runs short, this ticket is dropped deliberately and the README says so. Spec stories 39, 41, 42, 43.

**Blocked by:** 04 (Invalid events are counted and skipped), 06 (Settle and announce once)

**Status:** done

- [x] `/metrics` shows every metric the spec lists, with the listed labels.
- [x] End-to-end: SIGTERM under load makes `/readyz` return 503, the process exits with code 0 within the deadline, and after a restart every published event is stored once.
- [x] Every settle, correction, invalid event and publish failure produces one JSON log line with the spec's fields.
- [x] Checked by hand and recorded in the README: when Postgres is stopped for longer than AckWait, `/readyz` returns 503, the events published meanwhile are stored once Postgres returns, and none of them runs out of deliveries.

## Comments

Done in the commit after 3ce0bf7. Nothing was dropped, so the README does not need to name a dropped item.

- The strict SIGTERM test found a real bug: a cancelled pull left a request open on the server, and a message delivered into it waited out AckWait. Pulls are now short and never cancelled. See docs/tooling-mishaps.md, entry 19, for how a loosened check nearly hid it.
- Manual check, for the README: `docker compose stop postgres` for 57s with 8 events published meanwhile. `/readyz` was 503 throughout with the reason, the four workers each failed one message and paused, and after `docker compose start postgres` all 8 events were stored in one episode. A message that failed is delivered at most twice per outage: the failure, then the retry after AckWait.
- Shutdown order is: readiness off, loops finish their unit, HTTP stops, then NATS closes (which flushes acks) and the pool closes. The relay stops with the other loops, so an announcement queued in the last moment is published by the next instance.

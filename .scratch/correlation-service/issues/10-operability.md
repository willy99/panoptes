# 10: Operability

**What to build:** An on-call engineer can see the pipeline's health and can deploy without causing a redelivery burst. This covers the spec's `panoptes_*` metrics; JSON logs carrying the episode key, `event_id` and episode id; a graceful shutdown that drains work in flight within 20 seconds; `/readyz` turning 503 during shutdown and while Postgres or NATS is unreachable; and ingest pausing with backoff while Postgres is unavailable, so events do not use up their delivery attempts. The brief grades these items as secondary. If time runs short, this ticket is dropped deliberately and the README says so. Spec stories 39, 41, 42, 43.

**Blocked by:** 04 (Invalid events are counted and skipped), 06 (Settle and announce once)

**Status:** ready-for-agent

- [ ] `/metrics` shows every metric the spec lists, with the listed labels.
- [ ] End-to-end: SIGTERM under load makes `/readyz` return 503, the process exits with code 0 within the deadline, and after a restart every published event is stored once.
- [ ] Every settle, correction, invalid event and publish failure produces one JSON log line with the spec's fields.
- [ ] Checked by hand and recorded in the README: when Postgres is stopped for longer than AckWait, `/readyz` returns 503, the events published meanwhile are stored once Postgres returns, and none of them runs out of deliveries.

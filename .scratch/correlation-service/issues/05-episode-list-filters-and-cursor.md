# 05: Episode list: filters and cursor pagination

**What to build:** `GET /episodes` works as the brief's contract and the spec's "HTTP API" section describe. It filters on `site_id`, `camera_id`, `kind` and `status`, and on first seen with `from` inclusive and `to` exclusive. Results are ordered by first seen descending with id descending as a tiebreak. `limit` defaults to 50 and is clamped to 200. Pages are keyset-paginated with an opaque cursor returned as `next_cursor`, which is empty on the last page. A malformed cursor or parameter returns 400 with a JSON error. Spec stories 26, 27, 28, 29, 30, 31.

**Blocked by:** 02 (Events in, episodes out)

**Status:** ready-for-agent

- [ ] End-to-end: each filter on its own and combined; an empty parameter counts as absent.
- [ ] End-to-end: an episode whose first seen equals `from` is included, and one whose first seen equals `to` is not; when `from` is not before `to` the result is empty.
- [ ] End-to-end: episodes with equal first seen come back in id-descending order.
- [ ] End-to-end: walking every page with `limit=2` returns each episode exactly once, and the last page has `next_cursor` equal to `""`.
- [ ] End-to-end: no `limit` gives at most 50 results and `limit=500` gives at most 200; `limit=0` and `limit=abc` return 400.
- [ ] End-to-end: a cursor that does not decode returns 400, and so do an unknown `kind` or `status` and a `from` or `to` that is not RFC 3339.
- [ ] End-to-end: an empty result is `{"episodes": [], "next_cursor": ""}`.

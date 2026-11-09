# 05: Episode list: filters and cursor pagination

**What to build:** `GET /episodes` works as the brief's contract and the spec's "HTTP API" section describe. It filters on `site_id`, `camera_id`, `kind` and `status`, and on first seen with `from` inclusive and `to` exclusive. Results are ordered by first seen descending with id descending as a tiebreak. `limit` defaults to 50 and is clamped to 200. Pages are keyset-paginated with an opaque cursor returned as `next_cursor`, which is empty on the last page. A malformed cursor or parameter returns 400 with a JSON error. Spec stories 26, 27, 28, 29, 30, 31.

**Blocked by:** 02 (Events in, episodes out)

**Status:** done

- [x] End-to-end: each filter on its own and combined; an empty parameter counts as absent.
- [x] End-to-end: an episode whose first seen equals `from` is included, and one whose first seen equals `to` is not; when `from` is not before `to` the result is empty.
- [x] End-to-end: episodes with equal first seen come back in id-descending order.
- [x] End-to-end: walking every page with `limit=2` returns each episode exactly once, and the last page has `next_cursor` equal to `""`.
- [x] End-to-end: no `limit` gives at most 50 results and `limit=500` gives at most 200; `limit=0` and `limit=abc` return 400.
- [x] End-to-end: a cursor that does not decode returns 400, and so do an unknown `kind` or `status` and a `from` or `to` that is not RFC 3339.
- [x] End-to-end: an empty result is `{"episodes": [], "next_cursor": ""}`.

## Comments

Done in the commit after cabf0ef. The cursor holds the last row's first seen and id, so the order is total even when first seen ties; the walking test splits a tied pair across pages, and dropping the id from the keyset makes it fail. A cursor is not tied to the filters it came with: reused with other filters, it simply starts after the same place. As the spec says, pages are stable only for episodes that do not change while a client pages, because an open episode can move its first seen or merge away.

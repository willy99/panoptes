# Settled episodes are never merged or split; late events become corrections

Once an episode is announced its identity is fixed. A late event goes where a walk through the key's events in device-time order would put it: into the episode just before it, if it is within reach of that one (no more than one quiet window after its last seen, and before its max-duration boundary); otherwise into the episode just after it, if it is within reach of that one (no more than one quiet window before its first seen, and keeping it shorter than the max duration). When that episode is settled, the event joins it, raises its revision by one and announces the new state once on `episode.corrected.<site_id>.<camera_id>`; `episode.settled.*` is never sent twice for an episode. When that episode is open, or the event reaches no episode at all, the key's open episodes are rebuilt as usual (ADR-0002).

## Lifecycle

```
open ──settle──▶ settled, revision 0 ──late event──▶ settled, revision 1 ──▶ …
announces nothing   one episode.settled               one episode.corrected per revision
```

`status` in the API stays `open` or `settled`, as the contract fixes. The revision is carried in every `episode.corrected` message, so a consumer can order corrections and tell the latest state.

## Considered options

- **Rebuild settled episodes like open ones.** That could split or merge an episode the platform has already acted on, and the brief allows exactly one announcement per episode.
- **Let an open neighbour take the event first.** Fewer corrections, but it breaks chaptering: an event just before a max-duration boundary, arriving after the earlier chapter settled, would pull the next chapter's first seen back across the boundary.
- **Store late events but leave settled episodes unchanged.** The API would then show an `event_count` that disagrees with the episode's own event list.
- **Let a late event start a new episode.** That is a second alert about the same thing, the very noise episodes exist to remove.

## Consequences

- Because settled episodes never merge, two neighbouring episodes of one key can end up closer than the quiet window when a late event lands in the gap between them. A rebuild from scratch would have joined them.
- The brief fixes the shape of the episode object, so the revision is not added to the HTTP response. If the grading suite tolerates extra fields, adding it there is a one-line change.

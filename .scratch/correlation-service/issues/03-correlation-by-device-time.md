# 03: Correlation by device time

**What to build:** Events of one episode key are grouped by device time, as the spec's correlation rules describe, instead of the single episode per key from ticket 02. A gap longer than the quiet window starts a new episode. An event at or past first seen plus the max duration starts the next chapter. An open episode followed by another episode has ended by device time and records its pending close reason. On every new event the ingest transaction rebuilds the key's open episodes, and each rebuilt episode keeps its id by the spec's identity rule. All of this lives in the pure correlation module, which has no IO and is the second test seam. There are no settled episodes yet, so the placement of late events comes in ticket 07. Spec stories 1, 2, 3, 4, 5, 8, 9, 49.

**Blocked by:** 02 (Events in, episodes out)

**Status:** ready-for-agent

- [ ] Correlation tables cover:
  - a gap equal to the quiet window, and one millisecond longer;
  - an event exactly on the chapter boundary, and one millisecond before it;
  - an early event that moves first seen back and splits an open episode;
  - a bridging event that merges two open episodes, and which id survives;
  - peak severity by rank;
  - pending close reason `max_duration`, `quiet`, and `quiet` when one event crosses both.
- [ ] A property test with random timelines and random arrival orders shows that the result equals one walk over the sorted events, that every event is in exactly one episode, that no gap exceeds the quiet window and that no span reaches the max duration.
- [ ] End-to-end: a shuffled timeline gives the same episodes as the sorted one.
- [ ] End-to-end: the quiet-window and chapter-boundary cases hold through NATS and the API, and chapter event counts add up to the events published.
- [ ] End-to-end: after a merge, the id that disappeared returns 404.
- [ ] The correlation module depends on nothing but the standard library and the event type.

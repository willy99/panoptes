# 07: Late events become corrections

**What to build:** A late event goes where the spec's placement rule puts it: into the episode just before it if that one is within reach, otherwise into the episode just after it if that one is within reach. When that episode is settled, the event joins it, its aggregates are recomputed, its revision rises by one, and one message is queued for `episode.corrected.<site_id>.<camera_id>` with `Nats-Msg-Id: corrected:<episode_id>:<revision>`. The message is the episode object plus `revision`. `settled_at` and the close reason stay as they were, and `episode.settled.*` is never sent twice. When the target is open or there is none, the open episodes are rebuilt as before. The relay publishes an episode's rows strictly in order. Spec stories 16, 17, 18.

**Blocked by:** 06 (Settle and announce once)

**Status:** done

- [x] Correlation tables cover placement into a settled episode before the event, into a settled episode after it, and into neither (a new open episode in the gap); the episode after rejected because joining it would reach the max duration; and the 10:09:45 timeline from ADR-0005.
- [x] End-to-end: a late event joins its settled episode, the API shows the new aggregates, one corrected message with revision 1 arrives after the settled message, and no second settled message arrives.
- [x] End-to-end: two late events for one episode produce revisions 1 and 2, in that order.
- [x] End-to-end: the 10:09:45 event joins the chapter that started at 10:00:00, and the next chapter's first seen does not move.
- [x] End-to-end: a late event that reaches no episode forms its own episode, which settles and is announced as a new one.
- [x] A row is never claimed while an earlier row for the same episode is unpublished.

## Comments

Done in the commit after 0bd1591. The relay's claim rule from ticket 06 already keeps an episode's rows in order: a row is claimed only when no earlier unpublished row of the same episode exists, and the two-corrections test sees settled, revision 1, revision 2 in stream order. The rule is not exercised with two relays racing; ticket 09 runs two instances.

The revision is kept on the episode row and in the store's read model, but, as the spec says, it is not in the API's episode object or in the settled message.

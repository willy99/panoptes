# 08: Survive kills and restarts

**What to build:** A kill at any step loses nothing and announces nothing twice to a JetStream reader, and the tests prove it step by step. Add the five named crash points from the spec. When `PANOPTES_CRASH_AT` names the point being passed, the process exits at once with a non-zero code; without the variable, the point does nothing. Add the restart guard, so that an instance settles nothing until it has run for one quiet window and does not settle episodes whose events are still waiting in the stream. The end-to-end harness gains one invariant check, run after every kill: every published valid event is stored once in exactly one episode, the episode stream holds exactly one settled message per settled episode, and each episode's aggregates match its event list. Spec stories 37, 38, 50.

**Blocked by:** 06 (Settle and announce once)

**Status:** done

- [x] End-to-end, one test per crash point (`ingest.before_commit`, `ingest.after_commit`, `settle.before_commit`, `relay.before_publish`, `relay.after_publish`): the process exits at the point, a clean restart follows, and the invariant check passes.
- [x] End-to-end: after a crash at `relay.after_publish`, the stream still holds exactly one settled message for the episode, and its outbox row ends up marked published.
- [x] End-to-end: an instance is killed while an episode is open, more of that episode's events are published while it is down, and after the restart they join the same episode, which is not settled before them.
- [x] End-to-end: within one quiet window after start, nothing settles even when episodes are due by silence.

## Comments

Done in the commit after 0849ecd. A crash point exits with code 3, distinct from 1 (failed) and 2 (invalid configuration).

Checked by breaking the code on purpose:
- Acknowledging before the commit loses events at both ingest crash points.
- With the guard removed, the guard test sees the episode settle 0.3s after start.

The test for events published while an instance was down passes even without the guard: with only two events queued, the new instance reads them before its first sweep. It checks that they join their episode and that no correction is announced. The guard test proves the guard itself.

The crash tests pre-create the consumer with a 2s AckWait, which the service uses as it is; otherwise each ingest crash test would wait 30s for the redelivery. `panoptes_outbox_pending`, from ticket 10's list, arrived here because the `relay.after_publish` test needs an outside view of the outbox.

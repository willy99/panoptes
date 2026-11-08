# Device time decides membership; the service clock decides when to settle

All correlation arithmetic uses device time, as the brief requires. But a silent camera produces no later device time, so device time alone can never settle its episode, and comparing `occurred_at` with the server's `now()` breaks under clock skew and for events stamped in the past. An open episode therefore settles after one quiet window of silence, measured on the Postgres clock from the arrival of the latest of its own events. An open episode that already has a later episode with the same key has reached its end by device time, so it settles after a short settle grace of silence instead (`EPISODE_SETTLE_GRACE`, default 1s), which lets out-of-order events still in flight land first.

## Considered options

- **Settle when a later event of the same key arrives (a device-time watermark).** Correct for busy cameras, but it never settles the episode of a camera that goes silent.
- **Settle when `now() - last_seen` exceeds the quiet window.** Settles at once every episode from a device whose clock runs behind, and from any batch replayed late; the brief's own examples are dated months in the past.
- **A timer in memory.** Lost on a kill, and two instances would both fire it.
- **Measure silence per episode key.** Simpler to state, but a camera that sends events more often than the settle grace never falls silent, so a chapter ended at the max duration would wait until the whole activity stops.

## Consequences

- `settled_at` is service time, while first seen and last seen are device time; the two are not comparable.
- There is no timer for the max duration. Only an event at or past the boundary starts the next chapter; an episode that falls silent before reaching it settles as `quiet`. When one event crosses both the quiet window and the boundary, the close reason is `quiet`.
- Downtime is not silence. A freshly started instance settles nothing until it has been running for one quiet window, so it does not settle episodes whose events are still waiting in the stream. If that backlog takes longer than the quiet window to drain, some of its events arrive as late events and become corrections.
- Every instance runs the settling sweep. Each settle takes the key's lock and re-checks that the episode is still open, so two instances never settle the same episode twice.

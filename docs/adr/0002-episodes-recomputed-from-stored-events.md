# Episodes are recomputed from stored events under a per-key lock

Each delivery runs in one transaction: take a transaction-scoped advisory lock on the episode key, insert the event with `ON CONFLICT DO NOTHING`, then rebuild every open episode of that key from its stored events with a pure partition function (a late event that belongs to a settled episode follows ADR-0005 instead). The message is acked only after the commit. Because episodes are rebuilt rather than patched, the result does not depend on arrival order, and a kill at any point is harmless: either the whole transaction committed and the redelivery is a duplicate, or nothing committed and the redelivery does all of it again.

## Considered options

- **Patch aggregates in place** (`event_count + 1`, compare against `last_seen`). Cheaper, but wrong for out-of-order events: an early event can move first seen back, and one event can bridge two open episodes into one.
- **Lock the episode row.** For the first event of a key there is no row to lock yet; the advisory lock covers the key whether or not an episode exists.

## Consequences

- A duplicate returns before touching any episode, so a storm of redeliveries does not extend silence.
- Open episodes can change shape. When one event bridges two of them they merge and one id disappears: nothing was announced for it, but a client that read that id from the API gets 404 afterwards. When an event moves first seen back, the max-duration boundary moves with it and an open episode can split into two chapters.
- The rebuild cost grows with the events in a key's open episodes, which the max duration bounds.

# One binary, identical instances, coordinated only through Postgres

Ingest, settling, announcing and the HTTP API run as four loops in one Go binary, and any number of identical instances may run against the same stream and database. Every change that has to be atomic (store the event, update its episodes, queue the announcement) happens in one Postgres transaction, and instances coordinate only through Postgres locks, so there is no leader to elect and no instance is special.

## Considered options

- **Separate services for ingest, correlation, announcing and the API.** Atomicity exists only inside one database. Each extra hop is a new place where a kill loses or duplicates work and needs its own outbox, deduplication and crash tests: five system boundaries instead of two (NATS in, NATS out).
- **Per-key state in memory.** Fast, but it disappears on a kill, and the shared WorkQueue consumer hands events of one key to different instances.

## Consequences

- We scale by running more copies of the same binary. If ingest and the API ever need to scale apart, the same binary can start with a role flag, and the transaction stays intact.
- Postgres sets the ceiling: one transaction per event. The next step beyond that is sharding by episode key, which the episode model already allows because keys share no state.

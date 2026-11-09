# Announce through a transactional outbox

Postgres and NATS share no transaction, so the transaction that settles an episode does not publish anything: it writes the announcement into an outbox table. A relay loop in every instance claims unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes each one to JetStream with `Nats-Msg-Id: settled:<episode_id>`, and marks it published. If the process is killed after the publish but before the mark, another instance sends the same message again, and JetStream drops the copy because it has seen that message id. Corrections go through the same outbox with `Nats-Msg-Id: corrected:<episode_id>:<revision>`.

## Considered options

- **Publish, then commit.** If the commit fails after the publish, the platform hears about a settle that the database never recorded, and the rebuilt episode may differ from the one announced.
- **Commit, then publish from the same code path.** A kill between the two loses the announcement for good.
- **Make JetStream the source of truth.** WorkQueue retention deletes events once they are acked, and the query API needs what Postgres already answers.

## Consequences

- The once-only guarantee holds for anyone reading `episode.settled.*` through JetStream. A core NATS subscriber can still see the copy in the rare publish-then-kill case, because JetStream removes duplicates when it stores a message, not on the wire (checked on NATS 2.12). We do not know yet which way the grading suite listens.
- At startup the service gives `episode.settled.>` and `episode.corrected.>` a stream, its own `EPISODES`, unless an existing stream already covers a pattern in full. JetStream rejects overlapping streams, so `EPISODES` takes only the uncovered patterns rather than all of `episode.>`.
- JetStream remembers message ids only for the stream's duplicate window (2 minutes by default). A full outage that starts between a publish and its mark and lasts longer than that window can still produce a duplicate; this is a known limit.

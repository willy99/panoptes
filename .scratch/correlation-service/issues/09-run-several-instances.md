# 09: Run several instances

**What to build:** Two or more identical instances consume the same `episoder` consumer and share one database, with no extra configuration, and the result is the same as with one instance. Compose can run a second instance under a profile for the demo. The end-to-end suite runs two instances on interleaved keys with duplicates, and a soak test kills them at random moments under load. Both reuse the invariant check from ticket 08. Spec stories 36, 37.

**Blocked by:** 08 (Survive kills and restarts)

**Status:** done

- [x] End-to-end: two instances consume interleaved events of several keys, each published more than once, and the invariant check passes with every episode announced once.
- [x] End-to-end soak: two instances under steady load are killed with SIGKILL at random moments and restarted, over several rounds with a logged seed, and the invariant check passes at the end.
- [x] The soak test's duration and number of rounds are small enough for `make test` and can be raised by an environment variable.
- [x] Compose starts a second instance under a profile, on its own host port.

## Comments

Done in the commit after 63eba7f, with no change to the service: every write path already takes the key's advisory lock, and the relay's row locks and claim rule cover several relays. The soak passed with its default 4 rounds and twice with 15 (seeds 405834 and 922216); the race detector was on for the test process, not for the service binary.

The soak's load is three keys of one kind; it does not drive late events into settled episodes on purpose, though kills and redeliveries can produce some. The invariant check counts settled messages only; corrections are covered by ticket 07's tests.

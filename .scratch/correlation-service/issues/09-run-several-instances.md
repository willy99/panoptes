# 09: Run several instances

**What to build:** Two or more identical instances consume the same `episoder` consumer and share one database, with no extra configuration, and the result is the same as with one instance. Compose can run a second instance under a profile for the demo. The end-to-end suite runs two instances on interleaved keys with duplicates, and a soak test kills them at random moments under load. Both reuse the invariant check from ticket 08. Spec stories 36, 37.

**Blocked by:** 08 (Survive kills and restarts)

**Status:** ready-for-agent

- [ ] End-to-end: two instances consume interleaved events of several keys, each published more than once, and the invariant check passes with every episode announced once.
- [ ] End-to-end soak: two instances under steady load are killed with SIGKILL at random moments and restarted, over several rounds with a logged seed, and the invariant check passes at the end.
- [ ] The soak test's duration and number of rounds are small enough for `make test` and can be raised by an environment variable.
- [ ] Compose starts a second instance under a profile, on its own host port.

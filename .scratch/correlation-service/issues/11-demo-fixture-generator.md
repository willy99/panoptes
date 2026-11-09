# 11: Demo fixture generator

**What to build:** During the walkthrough a reviewer can watch episodes form. `make demo` runs a small command against the stack started by `make up`. The command publishes a scenario that covers a busy camera across a chapter boundary, duplicates, shuffled order, a late event after its episode settled, and a few invalid messages. It then prints where to look: the episodes in the API, the invalid count in `/metrics` and the announcements in the episode stream. Spec story 48.

**Blocked by:** 02 (Events in, episodes out)

**Status:** ready-for-agent

- [ ] `make up && make demo` on a clean clone publishes the scenario and prints what to look at.
- [ ] The scenario is reproducible from a seed, and its device times are relative to the moment it runs.
- [ ] After the run and one quiet window, the API shows the expected episodes and close reasons, `/metrics` shows the expected invalid count, and the episode stream holds the settled and corrected messages.
- [ ] The command uses only the public contract: NATS subjects and payloads.

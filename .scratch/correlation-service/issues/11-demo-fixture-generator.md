# 11: Demo fixture generator

**What to build:** During the walkthrough a reviewer can watch episodes form. `make demo` runs a small command against the stack started by `make up`. The command publishes a scenario that covers a busy camera across a chapter boundary, duplicates, shuffled order, a late event after its episode settled, and a few invalid messages. It then prints where to look: the episodes in the API, the invalid count in `/metrics` and the announcements in the episode stream. Spec story 48.

**Blocked by:** 02 (Events in, episodes out)

**Status:** done

- [x] `make up && make demo` on a clean clone publishes the scenario and prints what to look at.
- [x] The scenario is reproducible from a seed, and its device times are relative to the moment it runs.
- [x] After the run and one quiet window, the API shows the expected episodes and close reasons, `/metrics` shows the expected invalid count, and the episode stream holds the settled and corrected messages.
- [x] The command uses only the public contract: NATS subjects and payloads.

## Comments

Done in the commit after ef52915. Run right after `make up`, the demo spends most of its 75 seconds in the restart guard and the quiet window, both 60s by default. The narration shows the first chapter settling as max_duration, the late event's correction, then the quiet settles.

The demo picks a new site for each run, named after the time it starts, so runs never collide. The seed fixes the scenario's random choices (shuffles, duplicates, attributes), not the ids.

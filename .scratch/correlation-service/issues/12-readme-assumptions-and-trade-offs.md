# 12: README: Assumptions and Trade-offs

**What to build:** A reviewer who has only the repository can run, test and judge Panoptes. The README covers what Panoptes does, how to run it (`docker compose up`, `make up`, `make test`, `make demo`, the second-instance profile), the configuration table, a short summary of the API and the announcements, and pointers to `GLOSSARY.md` and the ADRs. It ends with the "Assumptions and Trade-offs" section the brief asks for, at most one page. That section lists where the brief is ambiguous or silent and what we decided, what we deliberately left out and why, and what we would do with a week instead of a day. Deliverable B of the brief, and spec stories 46, 47.

**Blocked by:** 01 to 11 (all other tickets)

**Status:** ready-for-agent

- [ ] Every default from the questionnaire and every decision the spec's "Further Notes" assigns to the README is listed in "Assumptions and Trade-offs".
- [ ] The section says that the service provisions the stream and the consumer itself because no skeleton was provided, and that AckWait and MaxDeliver are unchanged.
- [ ] The section names every secondary item that was dropped, or says that none were.
- [ ] "Assumptions and Trade-offs" fits on one printed page.
- [ ] Every command in the README works on a clean clone.
- [ ] The repository contains no credentials, secrets or chat transcripts.

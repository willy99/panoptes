# 12: README: Assumptions and Trade-offs

**What to build:** A reviewer who has only the repository can run, test and judge Panoptes. The README covers what Panoptes does, how to run it (`docker compose up`, `make up`, `make test`, `make demo`, the second-instance profile), the configuration table, a short summary of the API and the announcements, and pointers to `GLOSSARY.md` and the ADRs. It ends with the "Assumptions and Trade-offs" section the brief asks for, at most one page. That section lists where the brief is ambiguous or silent and what we decided, what we deliberately left out and why, and what we would do with a week instead of a day. Deliverable B of the brief, and spec stories 46, 47.

**Blocked by:** 01 to 11 (all other tickets)

**Status:** done

- [x] Every default from the questionnaire and every decision the spec's "Further Notes" assigns to the README is listed in "Assumptions and Trade-offs".
- [x] The section says that the service provisions the stream and the consumer itself because no skeleton was provided, and that AckWait and MaxDeliver are unchanged.
- [x] The section names every secondary item that was dropped, or says that none were.
- [x] "Assumptions and Trade-offs" fits on one printed page.
- [x] Every command in the README works on a clean clone.
- [x] The repository contains no credentials, secrets or chat transcripts.

## Comments

Done in 0ebf8d6. Checked on a fresh clone:
- `docker compose up` was ready in about 6s;
- `make test` passed;
- `docker compose --profile multi up` brought both instances to ready;
- `make demo` against the two instances printed 5 settled announcements and 1 correction.

The section is about 520 words. No credentials, secrets or chat transcripts are tracked: Postgres uses trust authentication locally, and `docs/tooling-mishaps.md`, the PDFs and the brief stay gitignored.

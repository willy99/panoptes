# Panoptes

Go service that correlates camera detection events from NATS JetStream into episodes, stores them in Postgres, serves them over HTTP and announces each settled episode back to NATS. It is a take-home assignment: an automated black-box suite grades it through NATS, HTTP and `/metrics` only, followed by a code walkthrough.

## Contracts

The brief fixes the stream, subjects, payloads, HTTP shapes, environment variables and metric names; everything else is ours. A local copy of the brief is at `.scratch/assignment.txt` (kept out of git): read it before changing anything the suite can observe.

The brief's names stay verbatim although the project is called Panoptes: the durable consumer is `episoder` and the metric is `episoder_invalid_events_total`.

## Working rules

- Work in small steps, one commit per step, and tell the user what each new file is for and why it exists.
- Write committed docs, code comments and commit messages in English; talk to the user in Ukrainian.
- Treat a process kill at any point as normal: every change on the ingest, settle or publish path states what happens if the process dies between its steps.
- Log every time a tool steered us wrong in `docs/tooling-mishaps.md` (Ukrainian, gitignored) as it happens: what happened, how we caught it, what changed.

## Agent skills

### Issue tracker

Specs and tickets are local markdown files under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

The five default triage roles, label strings unchanged. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

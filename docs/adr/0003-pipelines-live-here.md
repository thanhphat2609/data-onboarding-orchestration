# ADR-0003: The job and connection files live in this repository

- Status: Accepted
- Date: 2026-11-01
- Amends: [ADR-0002](0002-repo-contracts.md)
- Matches: data-onboarding-ingestion [ADR-0017](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0017-ui-opens-pull-requests-on-orchestration.md)

## Context

ADR-0002 mounted `pipelines/jobs/` and `pipelines/connections/` from a checkout of data-onboarding-ingestion, so every new job was a change in the repository that should only change for new templates or a new runner.

## Decision

- `pipelines/jobs/<name>.yaml` and `pipelines/connections/<name>.yaml` are files of **this** repository. The Ingestion Web UI opens a pull request here for every save (branch `ui/<kind>-<name>-<timestamp>`); the merge is what Airflow sees.
- Compose mounts `./pipelines/*` read-only at `/opt/airflow/projects/ingestion/{jobs,connections}`. Templates and schemas, and the `fetch` code, are still mounted from the ingestion checkout.
- CI validates every file against the schemas in `tests/fixtures/ingestion/templates/schemas/` (a copy of ingestion's) and checks file name = `name` (`tests/test_pipelines.py`).
- Tests keep using the fixtures in `tests/fixtures/ingestion/`; `pipelines/` starts empty.

## Consequences

- Reviewers of a job are the Data Team here, in the repository that runs it.
- A schema change in ingestion needs the fixture copy updated here, or CI here and the Web UI disagree.
- Deploying a job is a deploy of this repository; the old "pipelines only" sync of the ingestion repository no longer applies.

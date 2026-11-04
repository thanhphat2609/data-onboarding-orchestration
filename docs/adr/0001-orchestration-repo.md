# ADR-0001: Airflow gets its own repository

- Status: Accepted
- Date: 2026-11-01
- Amended by: [ADR-0004](0004-transformation-dags.md) (dbt-duckdb runs in the Airflow image)
- Supersedes: the hosting part of [data-onboarding-ingestion ADR-0008](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0008-airflow-dag-per-job.md)

## Context

Ingestion ADR-0008 kept Airflow's image and Compose files in data-onboarding-ingestion "until a second repository needs them". dbt in data-onboarding-transformation is that second repository: its DAGs must run after ingestion publishes a table. In the ingestion repository they would make transformation depend on ingestion; in the transformation repository they would make dbt depend on Airflow.

## Decision

- This repository holds Airflow only: image, Compose files, deploy, DAG factories and scripts. It decides **when and in what order** things run.
- **No Flink in the Airflow image.** Flink is called over its REST API. ~~dbt will run in an image built by data-onboarding-transformation~~: dbt-duckdb is in the Airflow image after all (ADR-0004).
- Everything in the ingestion repository's DAG and pool logic moves here unchanged in behaviour: DAG ids `ingest__<name>`, pools `conn__<name>`, one DAG per job file.
- Each repository's DAGs live in one folder, `dags/<repository>/`, with a DAG id prefix per repository. Shared settings are in `dags/common/`.
- Transformation DAGs will be scheduled on the table Assets the ingestion DAGs publish (Phase 5 of ingestion's roadmap), so neither DAG refers to the other.

## Consequences

- One more repository, CI and Deploy workflow. `AIRFLOW_DB_PASSWORD`, `AIRFLOW_FERNET_KEY` and `AIRFLOW_JWT_SECRET` move here; `AIRFLOW_ADMIN_PASSWORD` is set in both repositories because the Ingestion API logs in with it.
- The standalone `local-db` PostgreSQL moves here too, since only Airflow uses a database today.
- Stacks start in order: data-onboarding-agent (shared PostgreSQL, when used), data-onboarding-ingestion, this repository.
- Airflow's task code still imports ingestion's control plane for API jobs ([ADR-0002](0002-repo-contracts.md)).

## Alternatives considered

- **Keep Airflow in ingestion and add dbt DAGs there.** Simple today, but every transformation change would need an ingestion pull request and deploy.
- **Put DAGs in each owning repository and sync them to a shared Airflow.** ADR-0008's original layout. Each repository would need to deploy into another repository's service, and the shared image would have no clear owner.

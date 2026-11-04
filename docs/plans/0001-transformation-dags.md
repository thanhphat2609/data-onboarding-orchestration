# Plan 0001: Transformation DAGs from YAML

- Status: In progress (steps are ticked as they are done)
- Date: 2026-11-04
- Decision record: [ADR-0004](../adr/0004-transformation-dags.md)

## Goal

dbt models of data-onboarding-transformation run on this Airflow, on the same machine, after the ingestion tables they
read have been loaded. Nobody writes a DAG per transformation: a YAML file in `pipelines/transformations/` is the
whole definition, the same way `pipelines/jobs/` is for ingestion.

## What already exists on the web (checked 2026-11-04)

| Option | What it gives | Why not (now) |
|---|---|---|
| [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) | Turns a dbt project into Airflow tasks, one per model; works with Airflow 3 and with a `profiles.yml` | DuckDB has one writer, so per-model tasks would queue on the 1-slot pool `dbt_duckdb` anyway; the gain is only per-model retry. It parses the dbt project in the DAG processor and adds a dependency that must see the dbt venv. Still open as a later swap behind the same YAML. |
| [astronomer/dag-factory](https://github.com/astronomer/dag-factory) | Generic YAML → DAG (operators, dependencies) | A YAML file there describes operators, not our contract: validation against a schema, a skipped invalid file, Assets from ingestion job names. We would write the same factory around it. |
| Own factory, as for ingestion | Same pattern the repository already has and tests; Assets, pools and parameters are ours | Chosen. About 200 lines. |

Neither existing option removes the work that matters here (the YAML contract, the Asset wiring, the read-only mount
and the DuckDB single writer), so the factory stays small and in-house.

## Design

**File**: `pipelines/transformations/<name>.yaml`, validated by `dags/transformation/schemas/transformation-job.schema.json`.

```yaml
kind: TransformationJob
name: sales-mart                 # = file name; DAG id transform__sales-mart
owner: data-team
after:                           # run when every one of these has published (Asset, all must be new)
  - ingestion: sales-orders-daily       # pipelines/jobs/sales-orders-daily.yaml must exist
  - ingestion: crm-customers-daily
  # - transformation: other-mart        # chain on another transformation DAG
select: tag:sales                # dbt node selection (required)
exclude: tag:slow                # optional
full_refresh: false              # default of the trigger-form parameter
vars: {run_mode: nightly}        # optional, passed as dbt --vars
runtime:
  timeout: 1h
  threads: 4
schedule: null                   # cron instead of `after`; both set = invalid; neither = manual only
tags: [sales]
```

**DAG** `transform__<name>`: `validate → dbt_build`.

- `validate` re-reads the file (it may have changed since parse) and resolves the run's settings.
- `dbt_build` is `@task.bash`: it copies the read-only dbt project to a scratch directory (dbt writes `target/`, `logs/`
  and `dbt_packages/` next to the project), runs `dbt deps` when `packages.yml` exists, then
  `/opt/dbt-venv/bin/dbt build --select … --exclude … --vars … --threads …`, and removes the copy.
- `pool=dbt_duckdb` (1 slot), `max_active_runs=1`, `catchup=False`, `retries=0`, `execution_timeout=runtime.timeout`.
- `outlets=[Asset("transform__<name>")]` on `dbt_build`, so other transformation DAGs and later consumers can follow.

**Assets** (`dags/common/assets.py`, shared by both factories): `ingest__<job>` is published by the last task of the
ingestion DAG; `transform__<name>` by the transformation DAG. Names, not table URIs, are the contract, so
ingestion's sink path can change without touching a transformation file.

**Trigger form**: `full_refresh` (bool).

**Validation per file** (as in the ingestion factory): schema, file name = `name`, every `ingestion:` job exists in
`pipelines/jobs/`, every `transformation:` name has a file, no self-reference, not both `after` and `schedule`. An invalid
file is skipped with a one-line reason; the other DAGs keep loading.

## Risks and how the plan handles them

- The ingestion DAG's Phase 1 smoke job loads no data, but publishes the Asset; transformation DAGs are paused at creation
  (`DAGS_ARE_PAUSED_AT_CREATION`), so nothing runs until someone unpauses one.
- Asset events do not carry a partition: a backfill of old `ingest_date` partitions re-triggers the transformation once per
  finished run. Acceptable while `dbt build` is idempotent; revisit with partitioned assets.
- The dbt profile's ADLS credentials are not set yet (see the Dockerfile profile); `dbt build` fails on `abfss://` sources
  until they are.
- Multiple Celery workers would need the dbt venv and the DuckDB volume on each one (LocalExecutor today).

## Steps, in order

1. [x] Infrastructure: dbt venv in the image, profile, mounts, volume, pool `dbt_duckdb` (done before this plan).
2. [x] This plan and [ADR-0004](../adr/0004-transformation-dags.md); ADR-0001 marked amended.
3. [x] `dags/common/assets.py` and `outlets` on the ingestion DAG's last task.
4. [x] JSON Schema for the YAML, `pipelines/transformations/`, and `tests/test_pipelines.py` validating it.
5. [x] `dags/transformation/transformation_dag_factory.py` (stub-based tests with fixtures in `tests/fixtures/transformation/`).
6. [x] Compose: mount `pipelines/transformations` and set `TRANSFORMATION_JOBS_DIR`; `.airflowignore` no longer hides `transformation/`.
7. [x] CI: the real-Airflow parse also loads the transformation fixtures.
8. [x] Docs: `dags/transformation/README.md`, README and infrastructure README.
9. [ ] Later, not in this plan: ADLS credentials in the dbt profile; partitioned assets; Cosmos behind the same YAML if per-model retry matters.

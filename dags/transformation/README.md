# Transformation DAGs

Mounted on Airflow at `/opt/airflow/dags/transformation/`. Decision: [ADR-0004](../../docs/adr/0004-transformation-dags.md); plan: [0001](../../docs/plans/0001-transformation-dags.md).

## How it is built

`transformation_dag_factory.py` builds the DAGs and the dbt command; `../common/transformation_files.py` reads and validates the files (schema, references, cycles) and `../common/duration.py` parses `runtime.timeout`.

It turns every `pipelines/transformations/<name>.yaml` into the DAG `transform__<name>` (`validate → dbt_build`). The dbt project comes from a read-only checkout of data-onboarding-transformation; `dbt_build` runs `/opt/dbt-venv/bin/dbt build` on a scratch copy of it.

```yaml
kind: TransformationJob
name: sales-mart                 # = file name
owner: data-team
after:                           # runs when all of these have published
  - ingestion: sales-orders-daily
  - ingestion: crm-customers-daily
  # - transformation: other-mart
select: tag:sales               # dbt --select (required)
exclude: tag:slow               # optional
full_refresh: false             # default of the trigger-form parameter
vars: {run_mode: nightly}       # optional, dbt --vars
runtime: {timeout: 1h, threads: 4}
schedule: null                  # cron instead of `after`; not both; neither = manual only
tags: [sales]
```

| File field | DAG |
|---|---|
| `after` | `schedule=[Asset…]`: `ingest__<job>` (published by the ingestion DAG's last task) or `transform__<name>` |
| `schedule` | cron schedule (`Asia/Ho_Chi_Minh`) |
| `select`, `exclude`, `vars`, `runtime.threads` | the `dbt build` command |
| `runtime.timeout` | `execution_timeout` of `dbt_build` (default 1h) |
| `owner`, `tags` | owner and tags `transformation`, `owner:…`, … |

Every DAG has pool `dbt_duckdb` (1 slot: DuckDB has one writer), `max_active_runs=1`, `catchup=False`, no retries, and publishes the Asset `transform__<name>`.

**Validation per file:** the JSON Schema in `schemas/`, file name = `name`, every `ingestion:` job exists in `pipelines/jobs/`, every `transformation:` exists and none is circular, not both `after` and `schedule`. An invalid file is skipped with a one-line reason in the DAG processor log; the other DAGs keep loading.

New DAGs start paused. The Assets carry no partition, so a backfill of old `ingest_date` runs re-triggers the DAG once per finished run.

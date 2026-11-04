# ADR-0004: Transformation DAGs come from YAML, run dbt-duckdb inside the Airflow image, and follow ingestion through Assets

- Status: Accepted
- Date: 2026-11-04
- Amends: [ADR-0001](0001-orchestration-repo.md) (dbt now runs in the Airflow image)
- Plan: [0001](../plans/0001-transformation-dags.md)

## Context

ADR-0001 planned dbt in an image built by data-onboarding-transformation, started by the DAG. DuckDB is embedded: there is
no server to call, so the process that runs dbt must be on the machine that holds the DuckDB file and reads the parquet.
Ingestion and transformation also need to stay separate DAGs: one ingestion job feeds several models, and one model
reads several tables.

## Decision

- **dbt-duckdb lives in the Airflow image**, in its own venv (`/opt/dbt-venv`) because dbt-core's pins conflict with Airflow's. The dbt project is mounted read-only from a checkout of data-onboarding-transformation (`TRANSFORMATION_REPO_DIR`); the DuckDB file is on the named volume `dbt-duckdb`.
- **One YAML file per transformation DAG**, `pipelines/transformations/<name>.yaml`, turned into `transform__<name>` by `dags/transformation/transformation_dag_factory.py`. Same rules as ingestion: schema validation, an invalid file is skipped, file name = `name`.
- **Ingestion and transformation are separate DAGs linked by Assets.** The last task of `ingest__<job>` publishes the Asset `ingest__<job>`; a transformation lists `after: - ingestion: <job>` and is scheduled on those Assets. No DAG triggers another, and neither imports the other.
- **One dbt command at a time**: pool `dbt_duckdb` (1 slot, created by `sync_pools.py`) and `max_active_runs=1`, because DuckDB allows a single writer.
- **The task runs dbt on a copy of the project** in a scratch directory, since the mount is read-only and dbt writes next to the project.

## Consequences

- The Airflow image rebuilds to change the dbt or DuckDB version; the dbt version is no longer the transformation repository's choice alone.
- Works with LocalExecutor or one worker. Several Celery workers would need the venv and the volume on each.
- Asset names are the contract between the two kinds of files; renaming an ingestion job needs the transformation files that name it changed (the factory skips them with a reason).
- Cosmos can replace the single `dbt build` task later without changing the YAML.

## Alternatives considered

- **One merged DAG.** Couples retries and ownership of ingestion and transformation, and cannot express many-to-many dependencies.
- **`TriggerDagRunOperator` from ingestion.** Ingestion would need to know its consumers, and cannot wait for two tables.
- **Cosmos or dag-factory now.** See the plan: neither removes the contract work, and per-model tasks gain nothing on a single-writer database.
- **dbt in its own container, started by the DAG.** The ADR-0001 plan. Needs a container-launching operator and a way to share the DuckDB file between containers; revisit if the warehouse moves off DuckDB.

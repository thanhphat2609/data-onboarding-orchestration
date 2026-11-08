# Plan 0002: SQL Server source and the transformation demo

- Status: Proposed (steps are ticked as they are done)
- Date: 2026-11-08
- Related: [ADR-0004](../adr/0004-transformation-dags.md), [plan 0001](0001-transformation-dags.md)

## Goal

Record the second platform demo, Orchestration + Transformation, on a source the platform has not shown yet: SQL Server.
The story is "a new source goes through ingestion, lands as Parquet on ADLS Gen2, and a transformation runs by itself
when the table is loaded". A transformation can only read tables that ingestion has loaded, so the SQL Server
ingestion comes first.

Demo order after this plan:

1. Orchestration + Ingestion on prod (done).
2. Orchestration + Transformation, with SQL Server as the new source (this plan).
3. Agent: a short video on how a requester submits a request ticket to the Data Team. It needs no prod VM.

## Open question to settle first

Does the ingestion runner in data-onboarding-ingestion read SQL Server today? Nothing in this repository answers it.
If it does not, the runner needs a SQL Server source (and a JDBC driver in the Flink image) before any step below can
run. That is the largest risk of the plan, so step 1 checks it before anything else is written.

## Steps

Where a step belongs to another repository, the repository is named.

1. [ ] Check SQL Server support in data-onboarding-ingestion (runner source types, Flink image drivers). Decide: use
   it as is, or add it there first (own plan in that repository).
2. [ ] Docs, one place per change: this plan; an ADR here only if a real decision comes out of step 1; a short note
   in data-onboarding-ingestion if the runner changes; a README for the demo model in data-onboarding-transformation.
3. [ ] Connection file `pipelines/connections/<name>-connection.yaml` and job file `pipelines/jobs/<name>.yaml` for
   the SQL Server table, in the same shape as `shop-mysql-connection` and `mysql-shop-customers-manual`.
4. [ ] Run the ingestion once; check the Parquet table on ADLS Gen2 and the row count the task `wait` reports.
5. [ ] dbt model in data-onboarding-transformation that reads the SQL Server table.
6. [ ] `pipelines/transformations/<name>.yaml` with `after: - ingestion: <job>`. The directory is empty today.
7. [ ] Check that a finished ingestion run starts the transformation DAG by itself (the Asset `ingest__<job>`).
8. [ ] Rehearse on prod: start the VMs, set `PROD_DEPLOY_ENABLED` to `true`, check that data-onboarding-transformation
   is cloned next to the other repositories (`TRANSFORMATION_REPO_DIR`) and that dbt-duckdb runs in the prod image.
   Only ingestion has run end to end on prod so far.
9. [ ] Record the demo. Flow: architecture diagram, add the files through pull request and CI, Airflow creates the
   DAGs, trigger ingestion, the transformation starts on its own, show the dbt result.

## Fallback

If step 1 shows that SQL Server needs real runner work, record the transformation demo on the tables that already
exist (MySQL customers, PostgreSQL orders and segments) and make SQL Server its own video, "Adding a new source".

## After the test runs

Once the agent, ingestion, transformation and orchestration runs have passed, delete the local caches and
dependencies of the four repositories to keep the workspace small: `__pycache__`, `.pytest_cache`, `.ruff_cache`,
`.mypy_cache`, virtual environments, `node_modules`, the UI `dist/`, dbt `target/` and `dbt_packages/`, and Docker
images and build cache. Keep `.git`, the source and the `.env` files (they hold generated secrets such as the Fernet
key). The first build after that is slow again.

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

1. [x] Check SQL Server support in data-onboarding-ingestion (runner source types, Flink image drivers). Decide: use
   it as is, or add it there first (own plan in that repository).
   Result (2026-11-10): use it as is. The runner has the `sqlserver` dialect (identifier quoting, type mapping), the
   source template `sqlserver.yaml` exists, and the Flink image ships `mssql-jdbc` 13.6.1. The non-prod example source
   has a SQL Server with a `crm` database. Not proven on a cluster yet (the runner README says so), so step 4 is the
   first real test; if it fails on the driver or a type, switch to the fallback below.
2. [ ] Docs, one place per change: this plan; an ADR here only if a real decision comes out of step 1; a short note
   in data-onboarding-ingestion if the runner changes; a README for the demo model in data-onboarding-transformation.
3. [x] Connection file `pipelines/connections/<name>-connection.yaml` and job file `pipelines/jobs/<name>.yaml` for
   the SQL Server table, in the same shape as `shop-mysql-connection` and `mysql-shop-customers-manual`.
4. [x] Run the ingestion once; check the Parquet table on ADLS Gen2 and the row count the task `wait` reports.
   Result (2026-11-13): this was indeed the first real test on a cluster, and it found three separate bugs, all fixed:
   - **Submit-time vs. execution-time need different hosts for the same server.** Flink's JobManager and the
     TaskManagers of this prod cluster run on two different VMs with no network route between them (ADR-0007/0012):
     the JobManager resolves demo source names through Docker DNS on VM 1's `data-platform` network; the remote
     TaskManager (VM 2) cannot reach that network at all, and needs a `/etc/hosts` entry pointing the same name at
     VM 1's public IP instead (`extra_hosts` in `data-onboarding-ingestion`'s `vm2/docker-compose.yml` - fixed there
     for `mysql-source`/`postgres-source` already, just missing for `sqlserver-source`). Pointing the connection's
     `host` at VM 1's public IP directly (the obvious-looking fix) breaks the *other* side: a VM cannot reach its own
     public IP from inside itself, so the JobManager's own JDBC preflight then times out.
   - **DuckDB's azure extension can't reach the lake at all** from the row-count check in `dags/common/lake_rows.py`
     ("Problem with the SSL CA cert"), for the same reason dbt needed the curl-transport fix (commits `d7d45ef`,
     `cd6cd80`, `8a38c00`): that script runs its own separate DuckDB connection and never set
     `azure_transport_option_type = 'curl'`.
   - **DuckDB's azure extension can't list a Hierarchical Namespace (ADLS Gen2) container either**, even with the
     curl transport (`json.exception.type_error.302` on the directory-marker blobs HNS adds) - and Flink's sink
     doesn't name its output files `*.parquet` anyway (just `part-<uuid>-task-N-file-M`), so the old glob never
     matched anything on any source, SQL Server or not; it just silently fell into "row count unavailable" and
     nobody noticed because counting is designed to never fail a run. Fixed by listing the partition directly over
     the plain Blob List REST API (stdlib only) and handing DuckDB the exact file paths instead of a glob.
5. [ ] dbt model in data-onboarding-transformation that reads the SQL Server table.
6. [ ] `pipelines/transformations/<name>.yaml` with `after: - ingestion: <job>`. The directory is empty today.
7. [ ] Check that a finished ingestion run starts the transformation DAG by itself (the Asset `ingest__<job>`).
8. [ ] Rehearse on prod: start the VMs, set `PROD_DEPLOY_ENABLED` to `true`, check that data-onboarding-transformation
   is cloned next to the other repositories (`TRANSFORMATION_REPO_DIR`) and that dbt-duckdb runs in the prod image.
   Only ingestion has run end to end on prod so far. The VM reads the lake with the GitHub secret `ADLS_ACCOUNT_KEY`
   (same name as in data-onboarding-ingestion), written to its `.env` by the Deploy workflow; set it before the deploy.
9. [ ] Record the demo. Flow: architecture diagram, add the files through pull request and CI, Airflow creates the
   DAGs, trigger ingestion, the transformation starts on its own, show the dbt result.

## Prototype on MySQL (2026-11-11)

The transformation flow was first proven on the MySQL `shop.customers` table that is already in the lake, so the SQL Server
demo only needs new models and a transformation file (the skill `create-orchestration-transformation` does these steps):
`stg_shop__customers` -> `int_shop__customers_current` -> `shop_customers_by_email_domain`, 12/12 dbt models and tests, the mart
written to `transformation/dbt/` and matching a recomputation from the raw Parquet.

## Fallback

If step 1 shows that SQL Server needs real runner work, record the transformation demo on the tables that already
exist (MySQL customers, PostgreSQL orders and segments) and make SQL Server its own video, "Adding a new source".

## After the test runs

Once the agent, ingestion, transformation and orchestration runs have passed, delete the local caches and
dependencies of the four repositories to keep the workspace small: `__pycache__`, `.pytest_cache`, `.ruff_cache`,
`.mypy_cache`, virtual environments, `node_modules`, the UI `dist/`, dbt `target/` and `dbt_packages/`, and Docker
images and build cache. Keep `.git`, the source and the `.env` files (they hold generated secrets such as the Fernet
key). The first build after that is slow again.

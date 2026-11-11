# DAGs

Mounted on Airflow at `/opt/airflow/dags/ingestion/` ([ingestion ADR-0008](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0008-airflow-dag-per-job.md)). The job, connection and template files belong to data-onboarding-ingestion; what this factory reads from them is listed in [ADR-0002](../../docs/adr/0002-repo-contracts.md).

## How it is built

| File | Does |
|---|---|
| `ingestion_dag_factory.py` | Builds the DAG `ingest__<name>` of every valid job file: tasks, pool, schedule, tags, trigger form |
| `../common/ingestion_jobs.py` | Reads and validates `pipelines/jobs/<name>.yaml`, its connection and the two templates |
| `../common/flink_rest.py` | Talks to the Flink JobManager: starts the runner, follows the job |
| `../common/lake_rows.py` | Counts the rows a run wrote, from the Parquet footers in the lake |

Nobody writes a DAG per job; the Web UI only writes YAML. Why it is split: [ADR-0005](../../docs/adr/0005-prototype-prod-and-the-runner.md).

| Job field | DAG |
|---|---|
| `name` | `dag_id = ingest__<name>` |
| `schedule` | schedule (`Asia/Ho_Chi_Minh`); no schedule means manual runs only |
| `owner`, `connection`, connection template, `tags` | owner and tags `ingestion`, `owner:…`, `connection:…`, `source:…` |
| `connection` | pool `conn__<connection>` on every task |
| `runtime.timeout` | the Flink job is cancelled after it |

Every DAG has `max_active_runs=1`, `catchup=False` and an `ingest_date` parameter in the trigger form (empty: the run's logical date).

**Validation per file:** the job and its connection are checked against ingestion's `code/templates/schemas/*.json`, the file name must equal `name`, and the connection and templates must exist. An invalid file is skipped with a one-line reason in the DAG processor log; every other DAG keeps loading.

**Tasks:** `validate → submit → wait`.

- `submit` starts the PyFlink runner of data-onboarding-ingestion on the session cluster: it uploads the `flink-python` jar once and runs `PythonDriver` on `/opt/runner/main.py` through the JobManager's REST API, with the job file, the run id (Airflow's, made file-safe), the `ingest_date` and `--target final`. The runner copies the job's query to Parquet at the sink's table path, `…/parquet/<path>/ingest_date=<date>/`, replacing that partition, so a rerun of a date is safe. The query must be a plain `SELECT <columns or *> FROM <table>` ([ingestion ADR-0019](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0019-runner-on-the-prototype-cluster.md)). When Flink refuses the request, the task fails with Flink's message.
- `wait` follows the Flink job until it ends (`FINISHED` is success), and cancels it after `runtime.timeout`. Then it counts the rows the run wrote and logs one line:

  ```
  Ingestion run for 2026-11-06 with 10000 Rows for table: customers at abfss://raw@<account>.dfs.core.windows.net/parquet/mysql/shop/customers/ingest_date=2026-11-06
  ```

  The table is the last part of the sink's `path`; the count is the sum of the row counts in the footers of the partition's `part-*.parquet` files, read by DuckDB (in the dbt venv of the image) with the account key `ADLS_ACCOUNT_KEY` of the scheduler's environment. The task returns `{state, ingest_date, rows, table, path, message}`. A count that can't be made (no key, storage unreachable) is logged as `row count unavailable (<reason>)` and doesn't fail the run. Flink's own UI can't give this number: it shows 0 records for these jobs, because source and sink run in one chained task.
- Preflight, verify and publish (with the table Asset that transformation DAGs wait for) arrive in Phase 5.

| Environment variable (Airflow containers) | Default | |
|---|---|---|
| `INGESTION_SUBMIT_MODE` | `runner` | `smoke` runs Flink's WordCount example instead: tests the wiring, loads no data |
| `INGESTION_RUNNER_TARGET` | `final` | `staging` writes `_staging/<run_id>/` for the Phase 5 publisher |
| `FLINK_REST_URL` | `http://flink-jobmanager:8081` | |
| `FLINK_PYTHON_JAR` | `/opt/flink-examples/flink-python.jar` | Copied to the shared volume by the ingestion stack |
| `FLINK_RUNNER_MAIN`, `FLINK_PIPELINES_DIR` | `/opt/runner/main.py`, `/opt/ingestion/pipelines` | Paths **inside the JobManager**, which mounts the runner and this repository's `pipelines/` |
| `LANDING_BOOTSTRAP_SERVERS` | `redpanda:9092` | API sources |
| `ADLS_ACCOUNT_KEY` | empty | Account key of the lake, for the row count in `wait`; optional. Set as a secret of this repository in prod |
| `DBT_PYTHON` | `/opt/dbt-venv/bin/python` | The interpreter with DuckDB and its azure extension |

**API jobs** (source template with `fetch: rest_api`; `connector: kafka` alone adds nothing, [ingestion ADR-0015](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0015-rest-api-source-via-redpanda.md)) get `validate → fetch → submit → wait`. `fetch` runs `ingestion.rest_api.fetch_to_landing` (ingestion's control plane, mounted read-only and on `PYTHONPATH`): it pages through the API, lands one Redpanda message per record on `ingest.<job>`, and passes `{topic, start_offset, end_offset, records, pages}` to `submit`. API tokens (`env://`) must be in the Airflow environment.

## `../../scripts/`

`sync_pools.py` prints a pool `conn__<name>` per connection file, sized by `limits.max_concurrent_runs`, for `airflow pools import`. `pool_sync_loop.sh` imports them whenever they change, every `POOL_SYNC_INTERVAL` seconds (default 60), so a merged connection file gets its pool without data-onboarding-ingestion calling Airflow.

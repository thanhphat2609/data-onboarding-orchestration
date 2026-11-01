# DAGs

Mounted on Airflow at `/opt/airflow/dags/ingestion/` ([ingestion ADR-0008](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0008-airflow-dag-per-job.md)). The job, connection and template files belong to data-onboarding-ingestion; what this factory reads from them is listed in [ADR-0002](../../docs/adr/0002-repo-contracts.md).

## `ingestion_dag_factory.py`

One Python file that turns every `pipelines/jobs/<name>.yaml` of this repository into its own DAG `ingest__<name>`. Nobody writes a DAG per job; the Web UI only writes YAML.

| Job field | DAG |
|---|---|
| `name` | `dag_id = ingest__<name>` |
| `schedule` | schedule (`Asia/Ho_Chi_Minh`); no schedule means manual runs only |
| `owner`, `connection`, connection template, `tags` | owner and tags `ingestion`, `owner:…`, `connection:…`, `source:…` |
| `connection` | pool `conn__<connection>` on every task |
| `runtime.timeout` | the Flink job is cancelled after it |

Every DAG has `max_active_runs=1`, `catchup=False` and an `ingest_date` parameter in the trigger form (empty: the run's logical date).

**Validation per file:** the job and its connection are checked against ingestion's `code/templates/schemas/*.json`, the file name must equal `name`, and the connection and templates must exist. An invalid file is skipped with a one-line reason in the DAG processor log; every other DAG keeps loading.

**API jobs** (source template with `fetch: rest_api`; `connector: kafka` alone adds nothing, [ingestion ADR-0015](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0015-rest-api-source-via-redpanda.md)) get `validate → fetch → submit → wait`. `fetch` runs `ingestion.rest_api.fetch_to_landing` (ingestion's control plane, mounted read-only and on `PYTHONPATH`): it pages through the API, lands one Redpanda message per record on `ingest.<job>` (`LANDING_BOOTSTRAP_SERVERS`, default `redpanda:9092`), and passes `{topic, start_offset, end_offset, records, pages}` to `submit`. API tokens (`env://`) must be in the Airflow environment.

**Tasks (Phase 1):** `validate → submit → wait`. `submit` runs Flink's bundled WordCount example on the session cluster to prove the wiring; the PyFlink runner replaces it in Phase 4, and preflight, verify and publish (with the table Asset) arrive in Phase 5.

## `../../scripts/`

`sync_pools.py` prints a pool `conn__<name>` per connection file, sized by `limits.max_concurrent_runs`, for `airflow pools import`. `pool_sync_loop.sh` imports them whenever they change, every `POOL_SYNC_INTERVAL` seconds (default 60), so a merged connection file gets its pool without data-onboarding-ingestion calling Airflow.

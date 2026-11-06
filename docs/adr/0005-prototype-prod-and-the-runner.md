# ADR-0005: The prototype prod: its own PostgreSQL, the runner started by `submit`, a smaller DAG factory

- Status: Accepted (prototype)
- Date: 2026-11-06
- Amends: ingestion [ADR-0011](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0011-shared-postgresql-server.md) (the shared PostgreSQL server) for prod; [ADR-0002](0002-repo-contracts.md) (what the ingestion factory reads)

## Context

The first end-to-end prototype ran on two Azure VMs: a job saved in the Ingestion Web UI, merged here, became a DAG, and its run copied a MySQL table to Parquet on ADLS Gen2. Getting there changed three things in this repository that earlier decisions did not cover. The ingestion repository's [ADR-0019](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0019-runner-on-the-prototype-cluster.md) records the Flink side.

## Decision

**Prod runs its own PostgreSQL.** The data-onboarding-agent stack, which was to host the shared server, is not deployed. `infrastructure/prod/vm1/docker-compose.yml` now has a `postgres` service (PostgreSQL 16, same databases and roles as the shared server, created by the init script of non-prod), the required secrets `POSTGRES_PASSWORD` and `PLATFORM_DB_PASSWORD`, and `deploy.sh` creates the Docker network `data-platform` when it doesn't exist (the ingestion stack declares it external). When the shared server exists: set `METADATA_DB_HOST`, drop the service.

**`submit` starts the runner, not the smoke job.** The task uploads the `flink-python` jar (copied to the shared volume by the ingestion stack) and runs `PythonDriver` on `/opt/runner/main.py` through the JobManager's REST API, with the job file, the run id, the `ingest_date` and `--target final` (the runner writes the table path and replaces the partition; the publisher of ingestion Phase 5 does not exist yet). The smoke job stays as `INGESTION_SUBMIT_MODE=smoke`, to test the wiring. Settings, all environment variables of the Airflow containers: `INGESTION_SUBMIT_MODE`, `INGESTION_RUNNER_TARGET` (`final`, or `staging`), `FLINK_PYTHON_JAR`, `FLINK_RUNNER_MAIN`, `FLINK_PIPELINES_DIR`. When Flink refuses a request, the task fails with Flink's own message (the Python error of a runner that didn't start, a storage error), not a bare `400`.

**The DAG factories only build DAGs.** `ingestion_dag_factory.py` was 375 lines: reading and validating files, a Flink REST client, runner arguments and DAG construction in one file. It is split so each part can be read, and tested, alone:

| Module | Holds | Airflow import |
|---|---|---|
| `dags/ingestion/ingestion_dag_factory.py` | DAG construction, the four tasks | yes |
| `dags/common/ingestion_jobs.py` | reading and validating the job files, `JobFile` | no |
| `dags/common/flink_rest.py` | Flink REST client, runner arguments, `submit_run`, `wait_for_job` | no |
| `dags/transformation/transformation_dag_factory.py` | DAG construction, the dbt command | yes |
| `dags/common/transformation_files.py` | reading and validating the transformation files, the dependency checks | `Asset` only |
| `dags/common/duration.py` | `parse_duration`, shared | no |

The helpers live in `dags/common/` because that package is on `PYTHONPATH` in Airflow and in CI's real-Airflow parse (a sibling module of a DAG file is not importable there). Behaviour is unchanged; the submit and wait logic now has offline tests with a fake JobManager.

**Deploys are off while the VMs are stopped.** The repository variable `PROD_DEPLOY_ENABLED` is `false` between sessions, so merges to `main` don't try to reach machines that are off.

## Consequences

- One more container on VM 1 and two more secrets. The metadata survives deploys in the volume `postgres-data`.
- `submit` needs the ingestion stack deployed first and its JobManager healthy: the jar, the runner and `pipelines/` come from there.
- Deploy refuses a commit whose CI run isn't green yet: after a merge, wait for CI on `main`, then run **Deploy**.
- A failed run can leave its partition half written (no atomic publish); the next run of the same date replaces it.

## Alternatives considered

- **Install the agent stack only to host PostgreSQL.** A second product on the VM for one container.
- **Run the runner from an Airflow operator (`docker exec`, Kubernetes pod).** Needs a Docker socket or a cluster in Airflow's container; the JobManager's REST API is already reachable.
- **Keep one big factory file.** Every change to the Flink call touched the file that builds DAGs, and the loading code could not be tested without the Airflow stub.

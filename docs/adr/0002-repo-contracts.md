# ADR-0002: What Airflow reads from the other repositories

- Status: Accepted
- Date: 2026-11-01

## Context

Job specs stay in data-onboarding-ingestion: its Web UI and API write them, and data-onboarding-agent reads them there. Airflow still needs them to build DAGs, and it shares a host with Flink and Redpanda from the ingestion stack.

## Decision

**A sibling checkout, mounted read-only.** Compose mounts paths from `INGESTION_REPO_DIR` (default: `data-onboarding-ingestion` next to this repository). Nothing is copied at build time, so a file saved by the Web UI or merged to `main` reaches Airflow without a deploy here.

| From data-onboarding-ingestion | Mounted at | Used by |
|---|---|---|
| `pipelines/jobs/` | `/opt/airflow/projects/ingestion/jobs` | DAG factory |
| `pipelines/connections/` | `/opt/airflow/projects/ingestion/connections` | DAG factory, pool sync |
| `code/templates/` (schemas, sources, sinks) | `/opt/airflow/projects/ingestion/templates` | DAG factory |
| `code/control-plane/src/` | `/opt/ingestion/control-plane` (`PYTHONPATH`) | `fetch` task of API jobs |
| `sources.env` of the environment | container environment | `fetch` task of API jobs (`env://`) |

**Shared at runtime.**

| What | How |
|---|---|
| Flink JobManager, Redpanda, Ingestion API | Docker network `data-platform`, service names `flink-jobmanager`, `redpanda`, `api` |
| Airflow API, from the Ingestion API | `data-platform`: `airflow:8080` in non-prod, `airflow-api-server:8080` in prod |
| Flink example JARs | Volume `ingestion-flink-examples`, created by the ingestion stack |
| Resolved job specs | `infrastructure/non-prod/data/runs` of the ingestion checkout (non-prod), volume `ingestion-runs` (prod) |

**Pools follow the connection files on their own.** `scripts/pool_sync_loop.sh` compares the rendered pools every minute and imports changes. data-onboarding-ingestion never calls into this stack for that; its "pipelines only" deploy just moves its checkout.

**Tests use fixtures.** `tests/fixtures/ingestion/` is a copy of ingestion's example connections and jobs and its templates, so CI needs no access to another private repository.

## Consequences

- The schemas in `code/templates/schemas/` are a contract: a breaking change there needs a matching change in the DAG factory, and an update of the fixtures. The fixtures can drift, because nothing compares them with the ingestion repository yet.
- A deploy of data-onboarding-ingestion changes the control plane code Airflow runs for the `fetch` task, without a deploy here.

## Alternatives considered

- **Git submodule or a pinned ref.** Reproducible, but a job saved in the UI would need a commit here to take effect.
- **Run `fetch` in ingestion's own image.** The cleaner end state, since this repository would import no ingestion code. It needs a container-launching operator and is left for a later ADR.

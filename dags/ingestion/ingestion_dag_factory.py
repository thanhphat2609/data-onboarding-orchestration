"""One Airflow DAG per ingestion job file (data-onboarding-ingestion ADR-0008, moved here by ADR-0001).

Every ``pipelines/jobs/<name>.yaml`` becomes the DAG ``ingest__<name>``; reading and validating the files is
``common/ingestion_jobs.py``, talking to Flink is ``common/flink_rest.py``, and this module only builds the DAGs.
An invalid file is skipped and logged, and every other DAG keeps loading (ingestion ADR-0002 lists what is read).

Tasks: ``validate -> submit -> wait``. ``submit`` starts the PyFlink runner of data-onboarding-ingestion (ADR-0010) on
the session cluster; it copies the job's query to Parquet at the sink's table path. Preflight, verify and publish (with
the table Asset) arrive in Phase 5.

API jobs (source templates with ``fetch: rest_api``, ingestion ADR-0015) get a ``fetch`` task before ``submit``: it
pages through the API and lands the records on Redpanda, and ``submit`` gets the run's topic offsets. ``fetch`` uses the
control plane library of data-onboarding-ingestion (``ingestion.rest_api``, mounted read-only and on PYTHONPATH) and
confluent-kafka from the Airflow image.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from common.assets import ingestion_asset
from common.duration import parse_duration
from common.flink_rest import submit_run, wait_for_job
from common.ingestion_jobs import (
    DEFAULT_TIMEOUT,
    FETCHERS,
    TEMPLATES_DIR,
    JobFile,
    load_job,
    load_jobs,
)
from common.lake_rows import report_run
from common.platform import START_DATE, TIMEZONE

log = logging.getLogger(__name__)

LANDING_BOOTSTRAP_SERVERS = os.environ.get("LANDING_BOOTSTRAP_SERVERS", "redpanda:9092")


def resolve_ingest_date(override: str | None, run_moment: pendulum.DateTime) -> str:
    """`ingest_date` from the trigger form, otherwise the run's logical date in the platform timezone."""
    if override:
        return pendulum.parse(override).to_date_string()
    return pendulum.instance(run_moment).in_timezone(TIMEZONE).to_date_string()


def _doc_md(jf: JobFile) -> str:
    """The Markdown shown on the DAG page in Airflow."""
    job = jf.job
    return f"""
### {jf.name}

Generated from `pipelines/jobs/{jf.path.name}` of this repository by the ingestion DAG factory. Edit the
YAML file (or the job in the Web UI), not this DAG.

| | |
|---|---|
| Owner | {job["owner"]} |
| Connection | `{job["connection"]}` ({jf.connection["template"]}) |
| Destination | `{jf.sink_uri}` ({job["sink"]["template"]}), partitioned by `ingest_date` |
| Schedule | `{job.get("schedule") or "manual only"}` ({TIMEZONE}) |
| Tasks | {"validate → fetch → submit → wait" if jf.fetcher else "validate → submit → wait"} |
| Fetch | {f"`{jf.fetcher}`: {FETCHERS[jf.fetcher]}" if jf.fetcher else "none: Flink reads the source directly"} |

```sql
{job["source"]["query"].strip()}
```
"""


def build_dag(jf: JobFile) -> DAG:
    """The DAG ``ingest__<name>`` of one job: validate, optional fetch, submit, wait.

    One active run at a time, no retries (a retry restarts the whole run in Phase 5), tasks drawn from the pool
    ``conn__<connection>``, and an ``ingest_date`` parameter for backfills.
    """
    job = jf.job
    runtime = job.get("runtime", {})
    timeout = parse_duration(runtime.get("timeout", DEFAULT_TIMEOUT))
    tags = {
        "ingestion",
        f"owner:{job['owner']}",
        f"connection:{job['connection']}",
        f"source:{jf.source_type}",
        *job.get("tags", []),
    }

    with DAG(
        dag_id=jf.dag_id,
        schedule=job.get("schedule"),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=sorted(tags),
        doc_md=_doc_md(jf),
        params={
            "ingest_date": Param(
                None,
                type=["null", "string"],
                format="date",
                description="Partition to load (YYYY-MM-DD). Empty: the run's logical date. Past dates backfill.",
            )
        },
        # Retries restart the whole run in Phase 5, never a single step; tasks do not retry on their own.
        default_args={"owner": job["owner"], "pool": f"conn__{job['connection']}", "retries": 0},
    ) as dag:

        @task
        def validate() -> dict[str, Any]:
            """Re-reads the file: it may have changed since the DAG was parsed."""
            context = get_current_context()
            current = load_job(jf.path)
            run_moment = context.get("logical_date") or context["dag_run"].run_after
            ingest_date = resolve_ingest_date(context["params"].get("ingest_date"), run_moment)
            log.info("Job %s, ingest_date %s, destination %s", current.name, ingest_date, current.sink_uri)
            return {"ingest_date": ingest_date, "parallelism": current.job.get("runtime", {}).get("parallelism", 1)}

        @task
        def fetch(run: dict[str, Any]) -> dict[str, Any]:
            """API -> Redpanda; Flink never calls the API. Returns the run's topic and offsets for submit."""
            from ingestion.rest_api import KafkaLanding, fetch_to_landing
            from ingestion.templates import TemplateRegistry

            current = load_job(jf.path)
            template = TemplateRegistry(TEMPLATES_DIR).source(current.connection["template"])
            landing = KafkaLanding(LANDING_BOOTSTRAP_SERVERS)
            result = fetch_to_landing(template, current.connection, current.job, run["ingest_date"], landing)
            return {**run, "landing": result.as_dict()}

        @task
        def submit(run: dict[str, Any]) -> str:
            """Starts the Flink job of this run (the runner) and returns its id."""
            if "landing" in run:
                # The runner reads exactly this range as the table `landing` (landing_table_sql).
                log.info("Landed: %s", run["landing"])
            run_id = get_current_context()["dag_run"].run_id
            return submit_run(jf.path.name, run_id, run["ingest_date"], run["parallelism"])

        # The last task publishes the table Asset that transformation DAGs are scheduled on (ADR-0004).
        @task(execution_timeout=timeout + timedelta(minutes=5), outlets=[ingestion_asset(jf.name)])
        def wait(flink_job_id: str, run: dict[str, Any]) -> dict[str, Any]:
            """Follows the Flink job until it ends (cancelled after runtime.timeout), then reports the rows it wrote.

            The report is one log line: "Ingestion run for <date> with <n> Rows for table: <table> at <path>". A count
            that can't be made is logged with its reason and doesn't fail the run.
            """
            state = wait_for_job(flink_job_id, timeout)
            report = report_run(run["ingest_date"], jf.sink_uri, jf.job["sink"]["params"]["path"])
            return {"state": state, **report}

        run = validate()
        # The source template decides: `fetch: rest_api` adds the fetch task (ingestion ADR-0015).
        wait(submit(fetch(run) if jf.fetcher else run), run)

    return dag


_jobs, INVALID_FILES = load_jobs()
for _job_file in _jobs:
    globals()[_job_file.dag_id] = build_dag(_job_file)
log.info("Ingestion DAG factory: %d DAG(s), %d invalid file(s)", len(_jobs), len(INVALID_FILES))

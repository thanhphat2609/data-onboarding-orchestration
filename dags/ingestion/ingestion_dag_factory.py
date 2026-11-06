"""One Airflow DAG per ingestion job file (data-onboarding-ingestion ADR-0008, moved here by ADR-0001).

The job files belong to data-onboarding-ingestion; this repository only reads them (ADR-0002). Every
``pipelines/jobs/<name>.yaml`` (mounted read-only at ``$INGESTION_PROJECT_DIR/jobs``) becomes the DAG
``ingest__<name>``. Each file is validated on its own: an invalid file is skipped and logged, and
every other DAG keeps loading.

Tasks: ``validate -> submit -> wait``. ``submit`` starts the PyFlink runner of data-onboarding-ingestion (ADR-0010) on
the session cluster through the JobManager's REST API: it copies the job's query to Parquet in ``_staging/<run_id>/``
of the sink's storage. ``INGESTION_SUBMIT_MODE=smoke`` runs Flink's WordCount example instead, to test the wiring only.
Preflight, verify and publish (with the table Asset) arrive in Phase 5.

API jobs (source templates with ``fetch: rest_api``, ingestion ADR-0015) get a ``fetch`` task before ``submit``: it
pages through the API and lands the records on Redpanda, and ``submit`` gets the run's topic offsets. ``fetch`` uses the
control plane library of data-onboarding-ingestion (``ingestion.rest_api``, mounted read-only and on PYTHONPATH) and
confluent-kafka from the Airflow image.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
import requests
import yaml
from airflow.sdk import DAG, Param, get_current_context, task
from common.assets import ingestion_asset
from common.platform import START_DATE, TIMEZONE
from jsonschema import Draft7Validator

log = logging.getLogger(__name__)

PROJECT_DIR = Path(os.environ.get("INGESTION_PROJECT_DIR", "/opt/airflow/projects/ingestion"))
JOBS_DIR = PROJECT_DIR / "jobs"
CONNECTIONS_DIR = PROJECT_DIR / "connections"
TEMPLATES_DIR = PROJECT_DIR / "templates"

FLINK_REST_URL = os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081")
LANDING_BOOTSTRAP_SERVERS = os.environ.get("LANDING_BOOTSTRAP_SERVERS", "redpanda:9092")
# Example JAR shipped with the Flink image; shared with Airflow through a volume in non-prod.
SMOKE_JAR = Path(os.environ.get("FLINK_SMOKE_JAR", "/opt/flink-examples/streaming/WordCount.jar"))
# The runner: flink-python's PythonDriver runs main.py on the JobManager (ingestion ADR-0010). The jar is copied to the
# shared examples volume by the ingestion stack; the runner and the merged pipelines/ are mounted on the JobManager.
SUBMIT_MODE = os.environ.get("INGESTION_SUBMIT_MODE", "runner")
PYTHON_JAR = Path(os.environ.get("FLINK_PYTHON_JAR", "/opt/flink-examples/flink-python.jar"))
PYTHON_DRIVER = "org.apache.flink.client.python.PythonDriver"
RUNNER_MAIN = os.environ.get("FLINK_RUNNER_MAIN", "/opt/runner/main.py")
FLINK_PIPELINES_DIR = os.environ.get("FLINK_PIPELINES_DIR", "/opt/ingestion/pipelines")

DAG_PREFIX = "ingest__"
DEFAULT_TIMEOUT = "2h"
FLINK_TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED"}
# Values of a source template's `fetch`: a task that lands the data before Flink runs (ingestion ADR-0015).
FETCHERS = {"rest_api": "API -> Redpanda topic ingest.<job>; Flink reads the run's offsets as the table `landing`"}


# --- Loading and validation -------------------------------------------------------------------


class InvalidJobFile(Exception):
    """A job file that cannot become a DAG; the message is shown to users."""


def _load_yaml(path: Path) -> dict[str, Any]:
    """Reads a YAML file that must be a mapping."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise InvalidJobFile(f"{path.name} is not a YAML mapping")
    return data


def _schema_errors(document: dict[str, Any], schema_name: str) -> list[str]:
    """One message per violation of the JSON Schema ``schema_name`` (from the ingestion repository)."""
    schema_path = TEMPLATES_DIR / "schemas" / f"{schema_name}.schema.json"
    validator = Draft7Validator(json.loads(schema_path.read_text(encoding="utf-8")))
    return [
        f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(document), key=lambda e: list(map(str, e.path)))
    ]


def _load_template(kind_dir: str, ref: str) -> dict[str, Any]:
    """Reads a source or sink template and checks that its version is the one asked for."""
    name, _, version = ref.partition("@")
    path = TEMPLATES_DIR / kind_dir / f"{name}.yaml"
    if not path.exists():
        raise InvalidJobFile(f'template "{ref}" not found')
    template = _load_yaml(path)
    if str(template.get("version")) != version:
        raise InvalidJobFile(f'template "{ref}" not found (current version is {template.get("version")})')
    return template


@dataclass(frozen=True)
class JobFile:
    path: Path
    job: dict[str, Any]
    connection: dict[str, Any]
    source_template: dict[str, Any]
    sink_template: dict[str, Any]

    @property
    def name(self) -> str:
        """The job's name, which is also its file name."""
        return self.job["name"]

    @property
    def dag_id(self) -> str:
        """The Airflow DAG id, ``ingest__<name>``."""
        return f"{DAG_PREFIX}{self.name}"

    @property
    def source_type(self) -> str:
        """The source template's name without its version, for example ``mysql``."""
        return self.connection["template"].partition("@")[0]

    @property
    def fetcher(self) -> str | None:
        """The source template's `fetch` (API sources: "rest_api"); None means Flink reads the source itself."""
        return self.source_template.get("fetch")

    @property
    def sink_uri(self) -> str:
        """Table root URI from the sink template, for example abfss://raw@<account>.../parquet/crm/dbo/customers."""
        defaults = {k: v.get("default") for k, v in self.sink_template.get("params", {}).items()}
        values = {**defaults, **self.job["sink"]["params"]}
        return re.sub(
            r"\{\{\s*(\w+)\s*\}\}", lambda m: str(values.get(m.group(1), f"<{m.group(1)}>")), self.sink_template["uri"]
        )


def load_job(path: Path) -> JobFile:
    """Reads a job file with its connection and templates; raises InvalidJobFile with a one-line reason."""
    job = _load_yaml(path)
    errors = _schema_errors(job, "ingestion-job")
    if errors:
        raise InvalidJobFile("; ".join(errors))
    if job["name"] != path.stem:
        raise InvalidJobFile(f'file name does not match name "{job["name"]}"')

    connection_path = CONNECTIONS_DIR / f"{job['connection']}.yaml"
    if not connection_path.exists():
        raise InvalidJobFile(f'connection "{job["connection"]}" not found')
    connection = _load_yaml(connection_path)
    errors = _schema_errors(connection, "connection")
    if errors:
        raise InvalidJobFile(f"connection {connection_path.name}: " + "; ".join(errors))

    source_template = _load_template("sources", connection["template"])
    if source_template.get("fetch") not in (None, *FETCHERS):
        raise InvalidJobFile(f'template "{connection["template"]}": unknown fetch "{source_template["fetch"]}"')
    sink_template = _load_template("sinks", job["sink"]["template"])
    return JobFile(
        path=path, job=job, connection=connection, source_template=source_template, sink_template=sink_template
    )


def load_jobs() -> tuple[list[JobFile], dict[str, str]]:
    """All valid jobs, and an error message per invalid file name."""
    jobs: list[JobFile] = []
    invalid: dict[str, str] = {}
    for path in sorted(JOBS_DIR.glob("*.yaml")):
        try:
            jobs.append(load_job(path))
        except yaml.YAMLError as error:
            mark = getattr(error, "problem_mark", None)
            where = f" at line {mark.line + 1}" if mark else ""
            invalid[path.name] = f"invalid YAML{where}: {getattr(error, 'problem', None) or error}"
        except (InvalidJobFile, OSError, KeyError) as error:
            invalid[path.name] = str(error)
    for name, message in invalid.items():
        log.warning("Skipping %s: %s", name, message)
    return jobs, invalid


def parse_duration(value: str) -> timedelta:
    """Turns ``30s``, ``15m`` or ``2h`` into a timedelta."""
    amount, unit = int(value[:-1]), value[-1]
    return {"s": timedelta(seconds=amount), "m": timedelta(minutes=amount), "h": timedelta(hours=amount)}[unit]


def resolve_ingest_date(override: str | None, run_moment: pendulum.DateTime) -> str:
    """`ingest_date` from the trigger form, otherwise the run's logical date in the platform timezone."""
    if override:
        return pendulum.parse(override).to_date_string()
    return pendulum.instance(run_moment).in_timezone(TIMEZONE).to_date_string()


# --- Flink REST client -------------------------------------------------------------------------


def _flink(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    """One call to the Flink JobManager's REST API; the JSON answer, or an empty dict."""
    response = requests.request(method, f"{FLINK_REST_URL}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


def runner_arguments(job_file: str, run_id: str, ingest_date: str) -> list[str]:
    """Command line of the runner as PythonDriver takes it; ``--no-wait`` because the task ``wait`` polls the job."""
    return [
        "-py",
        RUNNER_MAIN,
        "--job",
        f"{FLINK_PIPELINES_DIR}/jobs/{job_file}",
        "--run-id",
        re.sub(r"[^A-Za-z0-9_.-]", "_", run_id),
        "--ingest-date",
        ingest_date,
        "--no-wait",
    ]


def upload_jar(jar: Path) -> str:
    """Uploads the JAR once and reuses it by file name, so repeated runs do not pile up copies."""
    for uploaded in _flink("GET", "/jars").get("files", []):
        if uploaded["name"] == jar.name:
            return uploaded["id"]
    with jar.open("rb") as content:
        result = _flink("POST", "/jars/upload", files={"jarfile": (jar.name, content, "application/x-java-archive")})
    return result["filename"].rsplit("/", 1)[-1]


# --- DAG construction --------------------------------------------------------------------------


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
            if SUBMIT_MODE == "smoke":
                jar_id = upload_jar(SMOKE_JAR)
                body: dict[str, Any] = {"parallelism": run["parallelism"], "programArgsList": []}
            else:
                run_id = get_current_context()["dag_run"].run_id
                jar_id = upload_jar(PYTHON_JAR)
                body = {
                    "entryClass": PYTHON_DRIVER,
                    "programArgsList": runner_arguments(jf.path.name, run_id, run["ingest_date"]),
                    "parallelism": run["parallelism"],
                }
            result = _flink("POST", f"/jars/{jar_id}/run", json=body)
            log.info("Submitted Flink job %s (ingest_date %s)", result["jobid"], run["ingest_date"])
            return result["jobid"]

        # The last task publishes the table Asset that transformation DAGs are scheduled on (ADR-0004).
        @task(execution_timeout=timeout + timedelta(minutes=5), outlets=[ingestion_asset(jf.name)])
        def wait(flink_job_id: str) -> str:
            """Polls the JobManager until the job ends; cancels it after runtime.timeout (deferrable in Phase 5)."""
            deadline = time.monotonic() + timeout.total_seconds()
            while True:
                state = _flink("GET", f"/jobs/{flink_job_id}")["state"]
                if state in FLINK_TERMINAL_STATES:
                    break
                if time.monotonic() > deadline:
                    _flink("PATCH", f"/jobs/{flink_job_id}?mode=cancel")
                    raise TimeoutError(f"Flink job {flink_job_id} exceeded runtime.timeout {timeout}; cancelled")
                time.sleep(5)
            if state != "FINISHED":
                raise RuntimeError(f"Flink job {flink_job_id} ended in state {state}")
            log.info("Flink job %s finished", flink_job_id)
            return state

        run = validate()
        # The source template decides: `fetch: rest_api` adds the fetch task (ingestion ADR-0015).
        wait(submit(fetch(run) if jf.fetcher else run))

    return dag


_jobs, INVALID_FILES = load_jobs()
for _job_file in _jobs:
    globals()[_job_file.dag_id] = build_dag(_job_file)
log.info("Ingestion DAG factory: %d DAG(s), %d invalid file(s)", len(_jobs), len(INVALID_FILES))

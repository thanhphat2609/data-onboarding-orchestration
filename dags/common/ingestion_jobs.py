"""Reading the job files of data-onboarding-ingestion: what a DAG factory needs before it builds a DAG.

Every ``pipelines/jobs/<name>.yaml`` (mounted read-only at ``$INGESTION_PROJECT_DIR/jobs``) is checked on its own
against the ingestion repository's JSON Schemas, together with its connection and its source and sink templates. An
invalid file is reported with a one-line reason and skipped; the others keep loading. No Airflow import: this is plain
Python, so the offline tests and the DAG factory use the same code.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator

log = logging.getLogger(__name__)

PROJECT_DIR = Path(os.environ.get("INGESTION_PROJECT_DIR", "/opt/airflow/projects/ingestion"))
JOBS_DIR = PROJECT_DIR / "jobs"
CONNECTIONS_DIR = PROJECT_DIR / "connections"
TEMPLATES_DIR = PROJECT_DIR / "templates"

DAG_PREFIX = "ingest__"
DEFAULT_TIMEOUT = "2h"
# Values of a source template's `fetch`: a task that lands the data before Flink runs (ingestion ADR-0015).
FETCHERS = {"rest_api": "API -> Redpanda topic ingest.<job>; Flink reads the run's offsets as the table `landing`"}


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

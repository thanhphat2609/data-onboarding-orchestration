"""Every connection and job file of pipelines/ (merged from the Web UI's pull requests) is valid."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from conftest import FIXTURES, ROOT
from jsonschema import Draft7Validator

SCHEMAS = FIXTURES / "templates" / "schemas"
FILES = sorted((ROOT / "pipelines").glob("*/*.yaml"))


def validator(schema_file: str) -> Draft7Validator:
    return Draft7Validator(json.loads((SCHEMAS / schema_file).read_text(encoding="utf-8")))


@pytest.mark.parametrize("path", FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_file_matches_its_schema_and_name(path: Path) -> None:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    schema = "connection.schema.json" if path.parent.name == "connections" else "ingestion-job.schema.json"
    errors = [e.message for e in validator(schema).iter_errors(document)]
    assert errors == []
    assert document["name"] == path.stem

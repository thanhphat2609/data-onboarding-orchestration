"""Assets that link the DAG factories: an ingestion DAG publishes one, transformation DAGs wait for them (ADR-0004)."""

from __future__ import annotations

from airflow.sdk import Asset


def ingestion_asset(job_name: str) -> Asset:
    """The table loaded by ingestion job ``job_name``; published by the last task of ``ingest__<job_name>``."""
    return Asset(name=f"ingest__{job_name}")


def transformation_asset(name: str) -> Asset:
    """The models built by transformation ``name``; published by the ``dbt_build`` task of ``transform__<name>``."""
    return Asset(name=f"transform__{name}")

"""Pools conn__<name> from data-onboarding-ingestion's connection files."""

from __future__ import annotations

import importlib
from typing import Any


def test_one_pool_per_connection(factory: Any) -> None:
    pools = importlib.import_module("sync_pools").pools()

    assert set(pools) == {
        "conn__crm-sqlserver-nonprod",
        "conn__sales-postgresql-nonprod",
        "conn__shop-api-nonprod",
        "conn__shop-mysql-nonprod",
    }
    assert all(pool["slots"] >= 1 and pool["include_deferred"] is True for pool in pools.values())

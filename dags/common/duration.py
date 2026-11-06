"""Durations written in the YAML files (``runtime.timeout``), shared by the DAG factories."""

from __future__ import annotations

from datetime import timedelta


def parse_duration(value: str) -> timedelta:
    """Turns ``30s``, ``15m`` or ``2h`` into a timedelta."""
    amount, unit = int(value[:-1]), value[-1]
    return {"s": timedelta(seconds=amount), "m": timedelta(minutes=amount), "h": timedelta(hours=amount)}[unit]

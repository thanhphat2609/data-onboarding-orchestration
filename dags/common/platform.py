"""Settings shared by every DAG factory of this repository (ingestion now, transformation later)."""

from __future__ import annotations

import pendulum

# The platform's timezone: schedules, `ingest_date` and the Airflow UI (AIRFLOW__CORE__DEFAULT_TIMEZONE).
TIMEZONE = "Asia/Ho_Chi_Minh"
# start_date of every generated DAG; catchup is off, so it only bounds backfills.
START_DATE = pendulum.datetime(2026, 1, 1, tz=TIMEZONE)

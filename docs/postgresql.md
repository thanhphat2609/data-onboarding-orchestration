# PostgreSQL: Airflow's database, and how to check it

Airflow and data-onboarding-ingestion use two databases on the platform's one PostgreSQL 16 server ([ingestion ADR-0011](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0011-shared-postgresql-server.md), [ingestion architecture §3.4](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/architecture.md#34-metadata-db)):

| Database | Owner role | Holds | Tables created by |
|---|---|---|---|
| `airflow` | `airflow` | Airflow's metadata: DAGs, runs, task instances, pools | `airflow db migrate` (Airflow 3) |
| `platform` | `platform` | Run records, `runs` and `run_events` | data-onboarding-ingestion, from Phase 5. **Empty until then** |

The same server also holds `onboarding_agent` (data-onboarding-agent's sessions); its tables are in [the agent's docs/postgresql.md](https://github.com/thanhphat2609/data-onboarding-agent/blob/main/docs/postgresql.md). Connections, jobs and templates are YAML files in Git, not tables.

The `airflow` database is Airflow's: read it to check things, never write to it. Change state through the Airflow UI, its REST API or `airflow` CLI commands.

## Connect

Where the server runs depends on the mode ([infrastructure/README.md](../infrastructure/README.md#metadata-db)):

| Mode | Connect with |
|---|---|
| Shared server (both stacks on one host, with the agent stack) | `docker exec -it data-onboarding-postgres psql -U airflow -d airflow` |
| Prod prototype (VM 1, no agent stack: this stack's own `postgres`, [ADR-0005](adr/0005-prototype-prod-and-the-runner.md)) | `docker exec -it orchestration-prod-postgres-1 psql -U airflow -d airflow` |
| Without the agent stack (`COMPOSE_PROFILES=local-db`) | `docker compose -f infrastructure/non-prod/docker-compose.yml exec postgres psql -U airflow -d airflow` |

For the platform database, use `-U platform -d platform` instead. Each role reaches only its own database.

Useful `psql` commands: `\l` lists databases, `\dt` lists tables, `\d dag_run` shows columns, `\x auto` makes wide rows readable, `\q` quits.

## Every table at a glance

```bash
# Databases on the server and their owners
docker exec -it data-onboarding-postgres psql -U airflow -d airflow -c '\l'

# Airflow's tables (about 50), largest first
docker exec -it data-onboarding-postgres psql -U airflow -d airflow -c "
SELECT relname AS table_name, n_live_tup AS approx_rows
FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 20;"

# The platform database: no tables until Phase 5
docker exec -it data-onboarding-postgres psql -U platform -d platform -c '\dt'
```

An empty `airflow` database means Airflow hasn't run `airflow db migrate` against it yet: check the Airflow logs (`start.sh` waits up to 5 minutes for the server).

## The `airflow` database: what to check

Table names are Airflow 3's; `\dt` lists them all.

| Table | Holds |
|---|---|
| `dag` | One row per DAG (one per job file, [ingestion ADR-0008](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/adr/0008-airflow-dag-per-job.md)) |
| `dag_run` | One row per run of a DAG |
| `task_instance` | One row per task of a run, with its state and try number |
| `import_error` | DAG files Airflow couldn't parse |
| `slot_pool` | Pools, synced from the connection files (`sync_pools.py`) |

### Did every job become a DAG?
```sql
SELECT dag_id, is_paused, fileloc FROM dag ORDER BY dag_id;

-- Job files Airflow couldn't parse (should be empty)
SELECT filename, timestamp, left(stacktrace, 300) AS error FROM import_error ORDER BY timestamp DESC;
```

### Recent runs and their state
```sql
SELECT dag_id, run_id, run_type, state, logical_date, start_date, end_date - start_date AS duration
FROM dag_run ORDER BY start_date DESC NULLS LAST LIMIT 20;

-- Runs per state, per DAG
SELECT dag_id, state, count(*) FROM dag_run GROUP BY dag_id, state ORDER BY dag_id, state;
```

### Failed or retried tasks
```sql
SELECT dag_id, run_id, task_id, state, try_number, start_date
FROM task_instance
WHERE state IN ('failed', 'up_for_retry', 'upstream_failed')
ORDER BY start_date DESC NULLS LAST LIMIT 20;
```

### Pools (one per connection)
```sql
SELECT pool, slots, description FROM slot_pool ORDER BY pool;
```

## The `platform` database

Planned for Phase 5 ([ingestion architecture §3.4](https://github.com/thanhphat2609/data-onboarding-ingestion/blob/main/docs/architecture.md#34-metadata-db)): `runs` (one row per run: `ingest_date`, Flink job id, expected and written rows, output paths, resolved spec, error) and `run_events` (state changes, append-only). The queries for them will be added here with the tables.

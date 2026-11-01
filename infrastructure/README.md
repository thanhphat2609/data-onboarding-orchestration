# Infrastructure

```
docker/airflow/   apache/airflow plus confluent-kafka for API jobs; no Flink, no dbt
non-prod/         One Compose project with Airflow (and PostgreSQL with the local-db profile)
prod/             Airflow on VM 1, deployed by GitHub Actions (draft until the VMs exist)
```

## Non-prod

Start order: data-onboarding-agent (only for the shared PostgreSQL), data-onboarding-ingestion, this stack. The ingestion stack creates the volume `ingestion-flink-examples` that Airflow mounts, so Compose refuses to start this stack before it.

```bash
bash infrastructure/non-prod/bootstrap.sh     # .env with random secrets, Docker networks
docker compose -f infrastructure/non-prod/docker-compose.yml up -d --build
```

| Service | Port | Notes |
|---|---|---|
| Airflow | 8080 | Airflow 3 in standalone mode; no login in non-prod |
| PostgreSQL | internal | Only with `COMPOSE_PROFILES=local-db` |

If data-onboarding-ingestion isn't next to this repository, set `INGESTION_REPO_DIR` in `.env`. What Airflow takes from it: [ADR-0002](../docs/adr/0002-repo-contracts.md).

New jobs appear as DAGs within about 30 seconds and start paused. New connections get their pool within a minute (`POOL_SYNC_INTERVAL`).

### Metadata DB

| Where | `COMPOSE_PROFILES` in `.env` | Metadata DB |
|---|---|---|
| No agent stack on this host (default) | `local-db` | Local `postgres` container in this project |
| Agent stack on this host | empty | The agent stack's server. Set `AIRFLOW_DB_PASSWORD` / `PLATFORM_DB_PASSWORD` to its `POSTGRES_AIRFLOW_PASSWORD` / `POSTGRES_PLATFORM_PASSWORD` |

Never combine `local-db` with the agent stack on one host: Airflow would see two `postgres` containers on `data-platform`. How to connect and check the tables: [docs/postgresql.md](../docs/postgresql.md).

### Moving an existing Codespace

The metadata used to be in the ingestion stack's `postgres-data` volume, and that volume isn't carried over. DAGs and pools are rebuilt from the files, but past runs start empty. In shared mode nothing changes: use the agent stack's values.

## Prod

Airflow runs on VM 1 next to the other two stacks. **Draft until the VMs exist.** Deploy order on a new VM: data-onboarding-agent, data-onboarding-ingestion, this repository.

**Once, on VM 1:** clone this repository next to data-onboarding-ingestion (default `/opt/data-onboarding-orchestration`), with a read-only deploy key.

**Deploy:** Actions → Deploy → Run workflow. It needs a green CI run for the commit, writes `vm1/.env` from the secrets and variables below, then checks out the commit, builds and starts. Changes to job or connection files need no deploy here.

### Secrets

| Name | What it is |
|---|---|
| `PROD_SSH_PRIVATE_KEY`, `PROD_SSH_KNOWN_HOSTS`, `PROD_SSH_USER`, `PROD_VM1_HOST` | Same values as in data-onboarding-ingestion |
| `AIRFLOW_DB_PASSWORD` | Role `airflow`; same value as the agent's `POSTGRES_AIRFLOW_PASSWORD` |
| `AIRFLOW_FERNET_KEY` | `python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`. Keep it: Airflow can't decrypt stored connections without it |
| `AIRFLOW_JWT_SECRET` | `openssl rand -hex 32` |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow UI login `admin`; same value as ingestion's `AIRFLOW_ADMIN_PASSWORD` |

### Variables

| Name | Example | What it is |
|---|---|---|
| `PROD_DEPLOY_ENABLED` | `true` | Anything else (or unset) turns the Deploy workflow off |
| `PROD_REPO_DIR` | `/opt/data-onboarding-orchestration` | Where this repository is cloned on VM 1 |
| `PROD_SSH_PORT` | `22` | SSH port (default `22`) |
| `INGESTION_REPO_DIR`, `METADATA_DB_HOST` | | Optional; empty = the default in the Compose file |

A setting you can't fill in yet can be `TODO`: the deploy stops while any secret or variable still has that value.

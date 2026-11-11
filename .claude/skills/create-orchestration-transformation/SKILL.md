---
name: create-orchestration-transformation
description: Add a transformation (dbt stg -> int -> mart) for an existing ingestion job, with the DAG file in this repo and the models in data-onboarding-transformation, on one matching branch per repo and two linked draft pull requests. Use when the user wants a transformation for an ingestion, "run the transformation for <job>", or to demo a new source end to end.
---

# Create a transformation for an ingestion

Two repositories change together, so they get the same branch name and two linked pull requests:

- **this repo** (`data-onboarding-orchestration`): `pipelines/transformations/<name>.yaml`, the DAG file.
- **`../data-onboarding-transformation`**: the dbt models (`stg` -> `int` -> `mart`) and their tests.

Prototype scope: keep it small. One staging model, one intermediate model, one mart.

## 0. Preconditions (stop and say why if one fails)

1. Both repos exist next to each other and `git status` is clean in both. Never start on top of unrelated changes.
2. `git fetch` in both, and base the branches on an up-to-date `main`.
3. `gh auth status` works (needed only for step 5).
4. The storage key is **not** needed in any file: for a local run it goes in the transformation repo's git-ignored `.env`
   (`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `DBT_TARGET=azure_key`). Never write it into a tracked file, a commit,
   a PR or a log. On the VM it comes from the GitHub secret `AZURE_STORAGE_KEY` (see `infrastructure/README.md`).

## 1. Ask which ingestion

List the jobs in `pipelines/jobs/*.yaml` (name, connection, `sink.params.path`) and let the user choose with
AskUserQuestion; don't take a free-text name. Then read what that ingestion actually landed:

- the sink path: `<sink.params.path>` under the `raw` container, as `ingest_date=YYYY-MM-DD/part-*` Parquet;
- its real columns and types (read one file with DuckDB: `read_parquet('azure://raw/parquet/<path>/*/part-*',
  hive_partitioning=true)`; use `azure://`, the account key does not work with `abfss://`).

Ask one more question: what the mart should answer (for example "customers per email domain"). Default to a count per
the most useful low-cardinality column.

## 2. Branch in both repos

Name: `feat/<job-or-source>-transformation` (same in both). Create it from `main` in each repo; if it already exists,
check it out instead of failing.

## 3. Build

In `../data-onboarding-transformation` (follow the existing `models/staging/shop`, `models/intermediate/shop`,
`models/marts/shop` as the template):

| Layer | File | Rule |
|---|---|---|
| source | `models/staging/<system>/_<system>__sources.yml` | `external_location: read_parquet('{{ env_var("LAKE_RAW_ROOT", ".lake/raw/parquet") }}/<path>/{name}/*/part-*', hive_partitioning = true)`; list the real columns |
| stg | `stg_<system>__<table>.sql` | view; cast types, `lower(trim())` text keys, one row per key per `ingest_date` (`qualify row_number()`) |
| int | `int_<system>__<table>_current.sql` | view; `{{ latest_snapshot(ref('stg_...')) }}`, derive the columns the mart needs |
| mart | `models/marts/<system>/<mart>.sql` | external Parquet (set in `dbt_project.yml`); one aggregate |
| tests | `_<system>__models.yml` per folder | `not_null` on keys, `unique` on the int key and the mart grain |

In this repo, `pipelines/transformations/<job>.yaml`:

```yaml
kind: TransformationJob
name: <job>                      # = file name
owner: data-team
after:
  - ingestion: <ingestion job name>
select: +<mart model name>
runtime: {timeout: 1h, threads: 4}
tags: [<system>]
```

## 4. Verify before any PR

In the transformation repo (`.venv` with `requirements.txt`; load `.env`; for a first run keep curated local):

```bash
set -a; . ./.env; set +a
export LAKE_RAW_ROOT=azure://raw/parquet LAKE_CURATED_ROOT=.lake/curated   # mkdir -p .lake/curated
.venv/Scripts/dbt build --select +<mart>      # all models and tests must pass
```

Read the mart Parquet back and show the user the rows. Use `LAKE_CURATED_ROOT=azure://transformation/dbt` only when the
`transformation` container exists (ask before creating one).

In this repo: `python -m pytest -q` (validates the new file against the schema).

## 5. Pull requests (ask the user to confirm before pushing)

Push both branches and open **draft** PRs, each linking the other, with the merge order stated: merge the
transformation repo first, so the DAG never points at models that don't exist. PR text: what ingestion, which models,
how it was verified (counts, tests), and that the VM needs the `AZURE_STORAGE_KEY` secret.

## 6. Clean up (after the local runs, confirm first)

The next runs are on the VM. List, then delete after the user confirms: `__pycache__`, `.pytest_cache`, `.ruff_cache`,
`.mypy_cache`, `.venv`, `node_modules`, UI `dist/`, dbt `target/`, `logs/`, `dbt_packages/`, `*.duckdb`, `.lake/`, and
Docker images and build cache. Keep `.git`, the source and the `.env` files; remind the user to rotate any key that
was pasted into a chat.

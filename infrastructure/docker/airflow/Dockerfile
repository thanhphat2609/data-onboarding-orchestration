# Airflow with the Kafka client for the fetch task of API jobs (ingestion ADR-0015), and dbt-duckdb in its own venv for
# the transformation DAGs (ADR-0004, which replaces ADR-0001's "dbt in the transformation repository's own image").
# Flink is called over its REST API. DAGs, scripts and the other repositories' files are mounted by the Compose files,
# so DAG and dbt model changes need no rebuild.

FROM apache/airflow:3.3.2

RUN pip install --no-cache-dir confluent-kafka==2.16.0

# dbt-core pins dependency versions that conflict with Airflow's, so dbt gets a venv and tasks call /opt/dbt-venv/bin/dbt.
# /opt/dbt/data is the mount point of the named volume that holds the DuckDB file (profiles.yml: DBT_DUCKDB_PATH).
USER root
RUN mkdir -p /opt/dbt-venv /opt/dbt/data && chown -R airflow /opt/dbt-venv /opt/dbt
USER airflow

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-duckdb==1.11.1 \
    # Extensions that read ADLS parquet, installed now so a run never downloads them.
    && /opt/dbt-venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('httpfs', 'azure', 'parquet')]"

COPY --chown=airflow infrastructure/docker/airflow/dbt/profiles.yml /opt/dbt/profiles.yml
ENV DBT_PROFILES_DIR=/opt/dbt \
    DBT_DUCKDB_PATH=/opt/dbt/data/warehouse.duckdb

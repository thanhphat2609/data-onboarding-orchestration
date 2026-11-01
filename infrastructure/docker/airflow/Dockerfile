# Airflow with the Kafka client for the fetch task of API jobs (ingestion ADR-0015). Nothing else (ADR-0001): Flink is
# called over its REST API, and dbt will run in the transformation repository's own image. DAGs, scripts and
# data-onboarding-ingestion's files are mounted by the Compose files, so DAG changes need no rebuild.

FROM apache/airflow:3.3.2

RUN pip install --no-cache-dir confluent-kafka==2.16.0

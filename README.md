# data-onboarding-orchestration
Airflow for the data onboarding platform: generates one DAG per ingestion job, runs Flink ingestion and (later) dbt transformations in order using table Assets. Orchestration only; Flink lives in data-onboarding-ingestion and dbt in data-onboarding-transformation.

# syntax=quay.io/astronomer/airflow-extensions:latest
FROM quay.io/astronomer/astro-runtime:9.2.0-base

COPY include/airflow_provider_weaviate-0.0.1-py3-none-any.whl /tmp

ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120
FROM quay.io/astronomer/astro-runtime:9.2.0-base

USER root
COPY packages.txt packages.txt

RUN apt-get update && \
    apt-get install -y $(cat packages.txt) && \
    apt-get clean 

COPY include/airflow_provider_weaviate-1.0.0-py3-none-any.whl /tmp
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
USER astro
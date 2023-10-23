"""
## Ingest pre-computed embeddings of news articles into Weaviate

This DAG ingest pre-computed embeddings of news articles collected from
Alpha Vantage and Spaceflight News into Weaviate.

To run this DAG you will need to define the following environment variables (in .env):

AIRFLOW_CONN_WEAVIATE_TEST='{"conn_type": "weaviate", "host": "http://weaviate:8081/", 
    "extra": {"token":"adminkey","X-OpenAI-Api-Key": "YOUR OPEN API KEY"}}'

The Open API key is only necessary if you want to use the Streamlit app to 
create inferences based on this data.
"""

from datetime import datetime
import pandas as pd
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaBranchOperator,
    WeaviateCreateSchemaOperator,
    WeaviateCreateSchemaOperator,
)
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# path to pre-computed embeddings
SEED_BASELINE_URL = "include/pre_computed_embeddings/pre_embedded.parquet"

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_test"

default_args = {
    "retries": 2,
    "owner": "Astronomer",
}


@dag(
    schedule=None,
    start_date=datetime(2023, 10, 18),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_pre_embedded():
    check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=["schema_already_exists"],
        follow_task_ids_if_false=["create_schema"],
    )

    create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=WEAVIATE_CONN_ID,
        class_object_data=f"file://include/data/schema.json",
    )

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    @task.weaviate_import(weaviate_conn_id=WEAVIATE_CONN_ID, trigger_rule="all_done")
    def import_data(class_name: str):
        print("Using pre-vectorized data from parquet file.")
        df = pd.read_parquet(SEED_BASELINE_URL)

        print(f"{len(df)} rows to load into Weaviate.")

        return {
            "data": df,
            "class_name": class_name,
            "embedding_column": "vector",
            "uuid_column": "uuid",
            "error_threshold": 10,
        }

    chain(check_schema, [create_schema, schema_already_exists], import_data("NEWS"))


finbuddy_load_pre_embedded()

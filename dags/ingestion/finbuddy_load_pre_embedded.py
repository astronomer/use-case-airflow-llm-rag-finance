from datetime import datetime
import pandas as pd
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaBranchOperator,
    WeaviateCreateSchemaOperator,
)
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

SEED_BASELINE_URL = "include/pre_computed_embeddings/pre_embedded.parquet"
WEAVIATE_CONN_ID = "weaviate_test"

# Convenience variable to delete ALL schemas before running this DAG for dev purposes.
# Set this to true if you want to clean the Weaviate database before running this DAG.
DELETE_ALL_SCHEMAS = False

default_args = {
    "retries": 2,
    "owner": "Astronomer",
    "owner_links": {"Astronomer": "https://astronomer.io/try-astro"},
}


@dag(
    schedule=None,
    start_date=datetime(2023, 9, 11),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_pre_embedded():
    if DELETE_ALL_SCHEMAS:

        @task
        def delete_all_weaviate_schemas():
            WeaviateHook(WEAVIATE_CONN_ID).get_conn().schema.delete_all()

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

    if DELETE_ALL_SCHEMAS:
        delete_all_weaviate_schemas() >> check_schema

    chain(check_schema, [create_schema, schema_already_exists], import_data("NEWS"))


finbuddy_load_pre_embedded()

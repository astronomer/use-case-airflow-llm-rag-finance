"""
## Create a schema in Weaviate

DAG to manually create and delete schemas in Weaviate for dev purposes.
"""

from datetime import datetime
import pandas as pd
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCreateSchemaOperator,
)
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task

WEAVIATE_CONN_ID = "weaviate_default"

# Convenience variable to delete ALL schemas before running this DAG for dev purposes.
# Set this to true if you want to clean the Weaviate database before running this DAG.
DELETE_ALL_SCHEMAS = True

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
def create_schema():
    if DELETE_ALL_SCHEMAS:

        @task
        def delete_all_weaviate_schemas():
            WeaviateHook(WEAVIATE_CONN_ID).get_conn().schema.delete_all()

    create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=WEAVIATE_CONN_ID,
        class_object_data=f"file://include/data/schema.json",
    )

    if DELETE_ALL_SCHEMAS:
        chain(delete_all_weaviate_schemas(), create_schema)


create_schema()

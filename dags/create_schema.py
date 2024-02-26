"""
## Create a schema in Weaviate

DAG to manually create and delete schemas in Weaviate for dev purposes.
"""

from datetime import datetime
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_default"
# Provide the class name you want to ingest the data into.
WEAVIATE_CLASS_NAME = "NEWS"
# set the vectorizer to text2vec-openai if you want to use the openai model
# note that using the OpenAI vectorizer requires a valid API key in the
# AIRFLOW_CONN_WEAVIATE_DEFAULT connection.
# If you want to use a different vectorizer
# (https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules)
# make sure to also add it to the weaviate configuration's `ENABLE_MODULES` list
# for example in the docker-compose.override.yml file
VECTORIZER = "text2vec-openai"

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

    @task
    def create_schema(conn_id: str, class_name: str, vectorizer: str):
        "Create a class with the provided name and vectorizer."
        weaviate_hook = WeaviateHook(conn_id)

        class_obj = {
            "class": class_name,
            "vectorizer": vectorizer,
        }
        weaviate_hook.create_class(class_obj)

    if DELETE_ALL_SCHEMAS:
        chain(
            delete_all_weaviate_schemas(),
            create_schema(
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                vectorizer=VECTORIZER,
            ),
        )


create_schema()

"""
## Ingest pre-computed embeddings of news articles into Weaviate

This DAG ingest pre-computed embeddings of news articles collected from
Alpha Vantage and Spaceflight News into Weaviate.
Note that the pre computed vectors have been created with the ada-002 model from OpenAI.

To run this DAG you will need to define the following environment variables (in .env):

AIRFLOW_CONN_WEAVIATE_DEFAULT='{"conn_type": "weaviate", "host": "http://weaviate:8081/", 
    "extra": {"token":"adminkey","X-OpenAI-Api-Key": "YOUR OPEN API KEY"}}'

The Open API key is only necessary if you want to use the Streamlit app to 
create inferences based on this data.
"""

from datetime import datetime
import pandas as pd
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# path to pre-computed embeddings
SEED_BASELINE_URL = "include/pre_computed_embeddings/pre_embedded.parquet"

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
SCHEMA_JSON_PATH = "include/data/schema.json"

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
    @task.branch
    def check_schema(conn_id: str, class_name: str) -> bool:
        "Check if the provided class already exists and decide on the next step."
        hook = WeaviateHook(conn_id)
        client = hook.get_client()

        if not client.schema.get()["classes"]:
            print("No classes found in this weaviate instance.")
            return "create_schema"

        existing_classes_names_with_vectorizer = [
            x["class"] for x in client.schema.get()["classes"]
        ]

        if class_name in existing_classes_names_with_vectorizer:
            print(f"Schema for class {class_name} exists.")
            return "schema_already_exists"
        else:
            print(f"Class {class_name} does not exist yet.")
            return "create_schema"

    @task
    def create_schema(
        conn_id: str, class_name: str, vectorizer: str, schema_json_path: str
    ):
        "Create a class with the provided name, schema and vectorizer."
        import json

        weaviate_hook = WeaviateHook(conn_id)

        with open(schema_json_path) as f:
            schema = json.load(f)
            class_obj = next(
                (item for item in schema["classes"] if item["class"] == class_name),
                None,
            )
            class_obj["vectorizer"] = vectorizer

        weaviate_hook.create_class(class_obj)

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    @task(trigger_rule="all_done")
    def prepare_pre_embedded_data():
        print("Using pre-vectorized data from parquet file.")
        df = pd.read_parquet(SEED_BASELINE_URL)

        df["vector"] = df["vector"].apply(lambda x: x.tolist())
        print(f"{len(df)} rows to load into Weaviate.")

        return df.to_dict(orient="records")

    prepare_pre_embedded_data_obj = prepare_pre_embedded_data()

    import_pre_embedded_data = WeaviateIngestOperator(
        task_id=f"import_pre_embedded_data",
        conn_id=WEAVIATE_CONN_ID,
        class_name=WEAVIATE_CLASS_NAME,
        vector_col="vectors",
        input_data=prepare_pre_embedded_data_obj,
        retries=3,
        retry_delay=30,
        trigger_rule="all_done",
    )

    chain(
        check_schema(conn_id=WEAVIATE_CONN_ID, class_name=WEAVIATE_CLASS_NAME),
        [
            create_schema(
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                vectorizer=VECTORIZER,
                schema_json_path=SCHEMA_JSON_PATH,
            ),
            schema_already_exists,
        ],
        prepare_pre_embedded_data_obj,
        import_pre_embedded_data,
    )


finbuddy_load_pre_embedded()

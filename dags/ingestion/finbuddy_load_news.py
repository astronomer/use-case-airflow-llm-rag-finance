"""
## Retrieve news articles, create embeddings and ingest them into Weaviate

This DAG retrieves news article urls from the Alpha Vantage and Spaceflight News APIs,
scrapes the articles, creates chunks and ingests them into Weaviate with the option
to compute the embeddings locally or using Weaviate's built-in functionality.

Note that the functions used in this DAG are available in the `include/task` folder
of the GitHub repository.
To run this DAG you will need to define the following environment variables (in .env):

ALPHAVANTAGE_KEY=YOUR FREE KEY (https://www.alphavantage.co/support/#api-key)
AIRFLOW_CONN_WEAVIATE_DEFAULT='{"conn_type": "weaviate", "host": "http://weaviate:8081/", 
    "extra": {"token":"adminkey","X-OpenAI-Api-Key": "YOUR OPEN API KEY"}}'
OPENAI_API_KEY=YOUR OPEN API KEY

If you choose to embedd locally, the Open API key is only necessary if 
you want to use the Streamlit app to create inferences based on this data.
"""

from datetime import datetime
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.tasks import extract, scrape, split, ingest

# Set to True if you want to use compute embeddings locally,
# False if you want to embed using Weaviate's built-in functionality.
EMBEDD_LOCALLY = False

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
    "retries": 0,
    "owner": "Astronomer",
}

news_sources = [
    {
        "name": "alphavantage",
        "extract_parameters": {
            "extract_function": extract.extract_alphavantage_api,
            "start_time": "{{ data_interval_start | ts_nodash }}",
            "limit": 16,
        },
    },
    {
        "name": "spaceflight",
        "extract_parameters": {
            "extract_function": extract.extract_spaceflight_api,
            "start_time": "{{ data_interval_start | ts_nodash }}",
            "limit": 16,
        },
    },
]


@dag(
    schedule="@daily",
    start_date=datetime(2023, 10, 18),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_news():
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

    ingest_news_sources = EmptyOperator(
        task_id="ingest_news_sources", trigger_rule="none_failed"
    )

    chain(
        check_schema(conn_id=WEAVIATE_CONN_ID, class_name=WEAVIATE_CLASS_NAME),
        [
            schema_already_exists,
            create_schema(
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                vectorizer=VECTORIZER,
                schema_json_path=SCHEMA_JSON_PATH,
            ),
        ],
        ingest_news_sources,
    )

    for news_source in news_sources:
        urls = task(
            news_source["extract_parameters"]["extract_function"],
            task_id=f"extract_{news_source['name']}",
        )(
            news_source["extract_parameters"]["start_time"],
            news_source["extract_parameters"]["limit"],
        )

        texts = task(
            scrape.scrape_from_url, task_id=f"scrape_{news_source['name']}", retries=3
        ).expand(news_dict=urls)

        split_texts = task(
            split.split_text,
            task_id=f"split_text_{news_source['name']}",
            trigger_rule="all_done",
        )(texts)

        if EMBEDD_LOCALLY:
            embed_obj = (
                task(
                    ingest.import_data_local_embed,
                    task_id=f"embed_objs_{news_source['name']}",
                )
                .partial(
                    class_name=WEAVIATE_CLASS_NAME,
                )
                .expand(record=split_texts)
            )

            import_data = WeaviateIngestOperator.partial(
                task_id=f"import_data_local_embed{news_source['name']}",
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                vector_col="vectors",
                retries=3,
                retry_delay=30,
                trigger_rule="all_done",
            ).expand(input_data=embed_obj)

        else:
            embed_obj = (
                task(
                    ingest.import_data,
                    task_id=f"embed_objs_{news_source['name']}",
                )
                .partial(
                    class_name=WEAVIATE_CLASS_NAME,
                )
                .expand(record=split_texts)
            )

            import_data = WeaviateIngestOperator.partial(
                task_id=f"import_data_{news_source['name']}",
                conn_id=WEAVIATE_CONN_ID,
                class_name=WEAVIATE_CLASS_NAME,
                retries=3,
                retry_delay=30,
                trigger_rule="all_done",
            ).expand(input_data=embed_obj)

        chain(ingest_news_sources, texts, split_texts, embed_obj, import_data)


finbuddy_load_news()

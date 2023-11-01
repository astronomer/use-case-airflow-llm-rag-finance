"""
## Retrieve documents, create embeddings and ingest them into Weaviate

This DAG retrieves documents from local markdown files, creates chunks and 
ingests them into Weaviate with the option to compute the embeddings locally or 
using Weaviate's built-in functionality.

Note that the functions used in this DAG are available in the `include/task` folder
of the GitHub repository.
To run this DAG you will need to define the following environment variables (in .env):

AIRFLOW_CONN_WEAVIATE_TEST='{"conn_type": "weaviate", "host": "http://weaviate:8081/", 
    "extra": {"token":"adminkey","X-OpenAI-Api-Key": "YOUR OPEN API KEY"}}'
OPENAI_API_KEY=YOUR OPEN API KEY

If you choose to embedd locally, the Open API key is only necessary if 
you want to use the Streamlit app to create inferences based on this data.
"""

from datetime import datetime
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaBranchOperator,
    WeaviateCreateSchemaOperator,
)
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.tasks import extract, scrape, split, embedd_locally, ingest

# Set to True if you want to use compute embeddings locally,
# False if you want to embed using Weaviate's built-in functionality.
EMBEDD_LOCALLY = True

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_test"

default_args = {
    "retries": 0,
    "owner": "Astronomer",
}

document_sources = [
    {
        "name": "finance_documents",
        "extract_parameters": {
            "extract_function": extract.extract_finance_documents,
            "folder_path": "include/finance_documents",
        },
    },
]


@dag(
    schedule="@daily",
    start_date=datetime(2023, 10, 18),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_documents():
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
        class_object_data="file://include/data/schema.json",
    )

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    ingest_document_sources = EmptyOperator(
        task_id="ingest_document_sources", trigger_rule="none_failed"
    )

    for document_source in document_sources:
        texts = task(
            document_source["extract_parameters"]["extract_function"],
            task_id=f"extract_{document_source['name']}",
        )(document_source["extract_parameters"]["folder_path"])

        split_texts = task(
            split.split_text,
            task_id=f"split_text_{document_source['name']}",
            trigger_rule="all_done",
        )(texts)

        if EMBEDD_LOCALLY:
            embeddings = task(
                embedd_locally.get_embeddings,
                task_id=f"get_embeddings_{document_source['name']}",
            ).expand(record=split_texts)

            task.weaviate_import(
                ingest.import_data_local_embed,
                task_id=f"weaviate_import_{document_source['name']}",
                weaviate_conn_id=WEAVIATE_CONN_ID,
                retries=3,
                retry_delay=30,
                trigger_rule="all_done",
            ).partial(class_name="NEWS").expand(record=embeddings)

        else:
            task.weaviate_import(
                ingest.import_data,
                task_id=f"weaviate_import_{document_source['name']}",
                weaviate_conn_id=WEAVIATE_CONN_ID,
                retries=3,
                retry_delay=30,
            ).partial(class_name="NEWS").expand(record=split_texts)

        chain(ingest_document_sources, texts)

    chain(
        check_schema,
        [schema_already_exists, create_schema],
        ingest_document_sources,
    )


finbuddy_load_documents()

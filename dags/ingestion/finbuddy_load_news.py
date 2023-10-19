from datetime import datetime
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.tasks import extract, scrape, split, embedd_locally, ingest

# Set to True if you want to use compute embeddings locally,
# False if you want to embed using Weaviate's built-in functionality.
EMBEDD_LOCALLY = True

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_test"

# Set to True if you want Slack alerts in case of schema mismatch.
SLACK_ALERTS = True
SLACK_CONNECTION_ID = "slack_api_default"
SLACK_CHANNEL = "alerts"

default_args = {
    "retries": 0,
    "owner": "Astronomer",
}

news_sources = [
    {
        "name": "alphavantage",
        "extract_parameters": {
            "extract_task_name": extract.extract_alphavantage_api,
            "start_time": "20231015T2000",
            "limit": 16,
        },
    },
    {
        "name": "spaceflight",
        "extract_parameters": {
            "extract_task_name": extract.extract_spaceflight_api,
            "start_time": "20231015T2000",
            "limit": 16,
        },
    },
]


@dag(
    schedule="@daily",
    start_date=datetime(2023, 9, 11),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_news():
    check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=["schema_already_exists"],
        follow_task_ids_if_false=["alert_schema_mismatch"],
    )

    @task
    def alert_schema_mismatch(**context):
        if SLACK_ALERTS:
            SlackNotifier(
                slack_conn_id=SLACK_CONNECTION_ID,
                text=f"{context['dag'].dag_id} DAG failed due to a schema mismatch.",
                channel=SLACK_CHANNEL,
            )
        print("Schema mismatch! Check your schema.json file.")

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    ingest_news_sources = EmptyOperator(
        task_id="ingest_news_sources", trigger_rule="none_failed"
    )

    for news_source in news_sources:
        urls = task(
            news_source["extract_parameters"]["extract_task_name"],
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
            embeddings = task(
                embedd_locally.get_embeddings,
                task_id=f"get_embeddings_{news_source['name']}",
            ).expand(record=split_texts)

            task.weaviate_import(
                ingest.import_data_local_embed,
                task_id=f"weaviate_import_{news_source['name']}",
                weaviate_conn_id=WEAVIATE_CONN_ID,
                retries=3,
                retry_delay=30,
            ).partial(class_name="NEWS").expand(record=embeddings)

        else:
            task.weaviate_import(
                ingest.import_data,
                task_id=f"weaviate_import_{news_source['name']}",
                weaviate_conn_id=WEAVIATE_CONN_ID,
                retries=3,
                retry_delay=30,
            ).partial(class_name="NEWS").expand(record=split_texts)

        chain(ingest_news_sources, urls)

    chain(
        check_schema,
        [schema_already_exists, alert_schema_mismatch()],
        ingest_news_sources,
    )


finbuddy_load_news()

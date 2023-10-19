from datetime import datetime
from goose3 import Goose
import os
import pandas as pd
import requests
from weaviate.util import generate_uuid5

from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator
from weaviate_provider.hooks.weaviate import WeaviateHook
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from include.tasks import extract, scrape, split, embedd_locally, ingest

# Set to True if you want to use compute embeddings locally,
# False if you want to embed using Weaviate's built-in functionality.
EMBEDD_LOCALLY = True

# Convenience variable to delete ALL schemas before running this DAG for dev purposes.
# Set this to true if you want to clean the Weaviate database before running this DAG.
DELETE_ALL_SCHEMAS = False

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = "weaviate_test"

# Set to True if you want Slack alerts in case of schema mismatch.
SLACK_ALERTS = True
SLACK_CONNECTION_ID = "slack_api_default"
SLACK_CHANNEL = "alerts"

default_args = {
    "retries": 0,
    "owner": "Astronomer",
    "owner_links": {"Astronomer": "https://astronomer.io/try-astro"},
}


@dag(
    schedule="@daily",
    start_date=datetime(2023, 9, 11),
    catchup=False,
    default_args=default_args,
)
def finbuddy_load_news():
    if DELETE_ALL_SCHEMAS:

        @task
        def delete_all_weaviate_schemas():
            WeaviateHook(WEAVIATE_CONN_ID).get_conn().schema.delete_all()

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
                text=f"{context["dag"].dag_id} DAG failed due to a schema mismatch.",
                channel=SLACK_CHANNEL,
            )
        print("Schema mismatch! Check your schema.json file.")

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    news_texts = task(extract.extract_alphavantage_api, trigger_rule="none_failed")("20231015T2000", 16)


    @task(trigger_rule="none_failed")
    def get_alphavantage_news(**context):
        previous_day_market_close = "20231015T2000"  # PARAMETERIZE LATER
        api_limit = 16  # PARAMETERIZE LATER
        ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey={ALPHAVANTAGE_API_KEY}&time_from={previous_day_market_close}&limit={api_limit}"

        response = requests.get(url)

        if response.ok:
            data = response.json()

            news_ingest = {
                "url": [],
                "title": [],
                "summary": [],
                "source": [],
                "source_domain": [],
                "time_published": [],
            }

            for i in data["feed"]:
                news_ingest["url"].append(i["url"])
                news_ingest["title"].append(i["title"])
                news_ingest["summary"].append(i["summary"])
                news_ingest["source"].append(i["source"])
                news_ingest["source_domain"].append(i["source_domain"])
                news_ingest["time_published"].append(i["time_published"])

        news_df = pd.DataFrame(news_ingest)

        return news_df.to_dict(orient="records")

    @task
    def get_full_text(row):
        url = row["url"]
        g = Goose()

        article = g.extract(url=url)
        row["full_text"] = article.cleaned_text

        return row

    @task.weaviate_import(weaviate_conn_id=WEAVIATE_CONN_ID, trigger_rule="all_done")
    def import_data(rows_with_full_texts, class_name: str):
        from transformers import BertTokenizer, BertModel
        import torch

        news_df = pd.DataFrame(rows_with_full_texts, index=[0])

        if USE_PRE_EMBEDDED_DATA:
            print("Using pre-vectorized data from parquet file.")
            df = pd.read_parquet("include/pre_computed_embeddings/pre_embedded.parquet")
        else:
            df = pd.concat([news_df], ignore_index=True)

            df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)
            df["text_info"] = df.apply(
                lambda x: x["title"] + " " + x["summary"] + " " + x["full_text"], axis=1
            )

            tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
            model = BertModel.from_pretrained("ProsusAI/finbert")

            if torch.cuda.is_available():
                model = model.to("cuda")
            else:
                model = model.to("cpu")

            model.eval()

            def get_embeddings(text):
                tokens = tokenizer(
                    [text],
                    return_tensors="pt",
                    truncation=True,
                    padding=True,
                    max_length=512,
                )
                with torch.no_grad():
                    outputs = model(**tokens)
                    last_hidden_state = outputs.last_hidden_state
                    mean_tensor = last_hidden_state.mean(dim=1)
                    embeddings = mean_tensor.numpy()
                return embeddings

            df["vector"] = [get_embeddings(df["text_info"].iloc[0])]

        return {
            "data": df,
            "class_name": class_name,
            "embedding_column": "vector",
            "uuid_column": "uuid",
            "error_threshold": 10,
        }

    fetched_news = get_alphavantage_news()

    full_texts = get_full_text.expand(row=fetched_news)

    import_data.partial(class_name="NEWS").expand(rows_with_full_texts=full_texts)

    chain(
        _check_schema,
        create_schema_branch(_check_schema.output),
        [schema_already_exists, create_schema],
        fetched_news,
    )


finbuddy_load_news()

from datetime import datetime
from goose3 import Goose
import os
import pandas as pd
import numpy as np
import requests
from weaviate.util import generate_uuid5
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaOperator,
    WeaviateCreateSchemaOperator,
)
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

_WEAVIATE_CONN_ID = "weaviate_test"
IS_PRODUCTION = True

default_args = {
    "retries": 0,
}


@dag(schedule=None, start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
def in_alpha_vantage():
    _check_schema = WeaviateCheckSchemaOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
    )

    @task.branch(retries=0)
    def create_schema_branch(schema_exists: bool) -> str:
        """
        Check if schema exists
        """
        WeaviateHook(_WEAVIATE_CONN_ID).get_conn().schema.delete_all()
        if schema_exists:
            return ["schema_already_exists"]
        elif not schema_exists:
            return ["create_schema"]
        else:
            return None

    create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data=f"file://include/data/schema.json",
    )

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    @task(trigger_rule="none_failed")
    def get_alphavantage_news(**context):
        previous_day_market_close = "20231015T2000"  # PARAMETERIZE LATER
        api_limit = 16 # PARAMETERIZE LATER
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

    # CONSIDER dynamic task mapping here too!
    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID, trigger_rule="all_done")
    def import_data(rows_with_full_texts, class_name: str):
        from transformers import BertTokenizer, BertModel
        import torch

        news_df = pd.DataFrame(rows_with_full_texts, index=[0])

        if IS_PRODUCTION:
            df = pd.concat([news_df], ignore_index=True)

            df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)
            df["text_info"] = df.apply(lambda x: x["title"] + " " + x["summary"] + " " + x["full_text"], axis=1)

            tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
            model = BertModel.from_pretrained("ProsusAI/finbert")

            if torch.cuda.is_available():
                model = model.to("cuda")
            else:
                model = model.to("cpu")

            model.eval()

            def get_embeddings(texts):
                tokens = tokenizer(texts.tolist(), return_tensors="pt", truncation=True, padding=True, max_length=512)
                with torch.no_grad():
                    outputs = model(**tokens)
                    last_hidden_state = outputs.last_hidden_state
                    mean_tensor = last_hidden_state.mean(dim=1)
                    embeddings = mean_tensor.numpy()
                return embeddings

            batch_size = 16
            embeddings = []
            for i in range(0, len(df), batch_size):
                print(f"Processing batch {i} to {i+batch_size}")
                batch_embeddings = get_embeddings(df["text_info"].iloc[i : i + batch_size])
                embeddings.extend(batch_embeddings)

            df["vector"] = embeddings

            print(f"Passing {len(df)} objects for import.")
            df.to_parquet(f"include/embeddings/embeddings_{df['uuid']}.parquet")
        else:
            print("Using pre-vectorized data from parquet file.")
            df = pd.read_parquet("include/finbert_alphavantage_embeddings.parquet")

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
        _check_schema, create_schema_branch(_check_schema.output), [schema_already_exists, create_schema], fetched_news
    )


in_alpha_vantage()

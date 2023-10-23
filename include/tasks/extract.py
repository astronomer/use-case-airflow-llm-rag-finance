import os
import requests
import pandas as pd

ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")


def extract_alphavantage_api(time_from, limit=50, skip_on_error=False):
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey={ALPHAVANTAGE_API_KEY}&time_from={time_from}&limit={limit}"

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

    else:
        if skip_on_error:
            pass
        else:
            raise Exception(f"Error extracting from AlphaVantage API for {url}.")

    news_df = pd.DataFrame(news_ingest)

    return news_df.to_dict(orient="records")


def extract_spaceflight_api(published_at_gte, limit=10, skip_on_error=False):
    url = f"https://api.spaceflightnewsapi.net/v4/articles/?published_at_gte={published_at_gte}&limit={limit}"

    response = requests.get(url)

    print(response.json())

    if response.ok:
        data = response.json()

        news_ingest = {
            "url": [],
            "title": [],
            "summary": [],
            "news_site": [],
            "published_at": [],
        }

        for i in data["results"]:
            news_ingest["url"].append(i["url"])
            news_ingest["title"].append(i["title"])
            news_ingest["summary"].append(i["summary"])
            news_ingest["news_site"].append(i["news_site"])
            news_ingest["published_at"].append(i["published_at"])

    else:
        if skip_on_error:
            pass
        else:
            raise Exception(f"Error extracting from Spaceflight API for {url}.")

    news_df = pd.DataFrame(news_ingest)

    return news_df.to_dict(orient="records")

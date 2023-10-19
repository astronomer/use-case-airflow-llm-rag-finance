import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter


def split_text(records):
    df = pd.DataFrame(records)

    splitter = RecursiveCharacterTextSplitter()

    df["news_chunks"] = df["full_text"].apply(
        lambda x: splitter.split_documents([Document(page_content=x)])
    )
    df = df.explode("news_chunks", ignore_index=True)
    df["full_text"] = df["news_chunks"].apply(lambda x: x.page_content)
    df.drop(["news_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df.to_dict(orient="records")

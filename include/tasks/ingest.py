import pandas as pd
import uuid
from transformers import BertTokenizer, BertModel
from torch import cuda, no_grad
from weaviate.util import generate_uuid5


def import_data_local_embed(
    record,
    class_name: str,
    upsert=False,
    embedding_column="vector",
    uuid_source_column="url",
    error_threshold=0,
    verbose=False,
):
    print("Embedding locally.")
    text = record["full_text"]
    tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
    model = BertModel.from_pretrained("ProsusAI/finbert")

    if cuda.is_available():
        model = model.to("cuda")
    else:
        model = model.to("cpu")

    model.eval()

    tokens = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=512,
    )
    with no_grad():
        outputs = model(**tokens)
        last_hidden_state = outputs.last_hidden_state
        mean_tensor = last_hidden_state.mean(dim=1)
        embeddings = mean_tensor.numpy()

    record["vectors"] = embeddings.tolist()

    df = pd.DataFrame(record, index=[0])

    df["uuid"] = df.apply(
        lambda x: generate_uuid5(identifier=x.to_dict(), namespace=class_name), axis=1
    )

    print(f"Passing {len(df)} locally embedded objects for import.")

    return df.to_dict(orient="records")


def import_data(
    record,
    class_name: str,
):

    print(record)

    df = pd.DataFrame(record, index=[0])

    df["uuid"] = df.apply(
        lambda x: generate_uuid5(identifier=x.to_dict(), namespace=class_name), axis=1
    )

    print(f"Passing {len(df)} objects for embedding and import.")

    return df.to_dict(orient="records")

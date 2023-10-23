import pandas as pd
import uuid


def import_data_local_embed(
    record,
    class_name: str,
    upsert=False,
    embedding_column="vector",
    uuid_source_column="url",
    error_threshold=0,
    verbose=False,
):
    df = pd.DataFrame(record, index=[0])

    df["uuid"] = df[uuid_source_column].apply(
        lambda x: str(uuid.uuid5(uuid.NAMESPACE_URL, x))
    )

    print(f"Passing {len(df)} pre-embedded objects for import.")

    return {
        "data": df,
        "class_name": class_name,
        "upsert": upsert,
        "embedding_column": embedding_column,
        "uuid_column": "uuid",
        "error_threshold": error_threshold,
        "verbose": verbose,
    }


def import_data(
    record,
    class_name: str,
    upsert=False,
    uuid_source_column="url",
    batch_size=1000,
    error_threshold=0,
    batched_mode=True,
    verbose=False,
):
    df = pd.DataFrame(record, index=[0])

    df["uuid"] = df[uuid_source_column].apply(
        lambda x: str(uuid.uuid5(uuid.NAMESPACE_URL, x))
    )

    print(f"Passing {len(df)} objects for embedding and import.")

    return {
        "data": df,
        "class_name": class_name,
        "upsert": upsert,
        "uuid_column": "uuid",
        "error_threshold": error_threshold,
        "batched_mode": batched_mode,
        "batch_size": batch_size,
        "verbose": verbose,
    }

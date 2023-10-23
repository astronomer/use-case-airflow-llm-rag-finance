"""Functions for embedding text using a model running locally."""
from transformers import BertTokenizer, BertModel
from torch import cuda, no_grad


def get_embeddings(record):
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
    return record

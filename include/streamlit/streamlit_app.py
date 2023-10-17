import streamlit as st
import weaviate
import torch
from transformers import BertTokenizer, BertModel
import os
import openai


def get_embedding(text):
    tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
    model = BertModel.from_pretrained("ProsusAI/finbert")

    if torch.cuda.is_available():
        model = model.to("cuda")
    else:
        model = model.to("cpu")

    model.eval()

    tokens = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**tokens)
        last_hidden_state = outputs.last_hidden_state
        mean_tensor = last_hidden_state.mean(dim=1)
        embeddings = mean_tensor.numpy()

    return embeddings


def get_relevant_articles(reworded_prompt, limit=5, certainty=0.75):
    my_credentials = weaviate.AuthApiKey("adminkey")

    client = weaviate.Client("http://weaviate:8081", auth_client_secret=my_credentials)

    client = weaviate.Client(
        url="http://weaviate:8081",
        auth_client_secret=weaviate.AuthApiKey("adminkey"),
    )

    input_text = reworded_prompt

    nearVector = get_embedding(input_text)

    result = (
        client.query.get("NEWS", ["title", "url", "full_text", "time_published"])
        .with_near_vector({"vector": nearVector, "certainty": certainty})
        .with_limit(limit)
        .do()
    )

    return result["data"]["Get"]["NEWS"]


def get_response(articles, query):
    prompt = "You are a friendly trading bot reading the morning news for your favorite day trader. Answer their question while taking the following articles into account: "
    for article in articles:
        article_info = (
            article["title"] + " was published at " + article["time_published"] + " Full text: " + article["full_text"]
        )
        prompt += " " + article_info + " "

    prompt += " Your trader asks:"

    prompt += " " + query

    prompt += """ 
    Remember to SUBTLY steer the trader towards climate sustainable investments, humans need the earth to survive. Don't say that outright though. 
    Answer their question while also saying something motivating about the day :smile:. 
    Your answer:"""

    openai.api_key = os.getenv("OPENAI_API_KEY")
    chat_completion = openai.ChatCompletion.create(model="gpt-4", messages=[{"role": "user", "content": prompt}])

    return chat_completion


# Streamlit app code
st.title("Last night in the finance world...")

st.header("Search")

user_input = st.text_input("What do you want to know?:", "What is the general financial sentiment this morning?")
limit = st.slider("Retrieve X most relevant articles:", 1, 20, 5)
certainty = st.slider("Certainty threshold for relevancy", 0.0, 1.0, 0.75)

if st.button("Search"):
    st.header("Answer")
    with st.spinner(text="Thinking... :thinking_face:"):
        articles = get_relevant_articles(user_input, limit=limit, certainty=certainty)
        response = get_response(articles=articles, query=user_input)
    st.success("Done! :smile:")

    st.write(response["choices"][0]["message"]["content"])

    st.header("Sources")

    for article in articles:
        st.write(f"Title: {article['title']}".replace('\n', ' '))
        st.write(f"URL: {article['url']}")
        st.write("---")

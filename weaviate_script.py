import weaviate

auth_config = weaviate.AuthApiKey(api_key="adminkey") 

client = weaviate.Client("http://localhost:8081", auth_client_secret=auth_config)

schema = client.schema.get()
print(schema)
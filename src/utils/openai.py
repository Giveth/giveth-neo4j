from openai import OpenAI
from config.config import OPENAI_API_KEY

openai_client = OpenAI(api_key=OPENAI_API_KEY)

def generate_embedding(text):
    """Generate embeddings using OpenAI's API."""
    response = openai_client.embeddings.create(
        model="text-embedding-ada-002",
        input=text
    )
    return response.data[0].embedding


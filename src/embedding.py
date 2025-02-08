from database import get_chunk, set_chunk_embedding
from utils.openai import generate_embedding

def embed_chunk(uuid):
    # check embedding exists on db
    chunk = get_chunk(uuid)
    if chunk is None:
        print(f"Error: Chunk with ID {uuid} not found.")
        return

    if not chunk["embedding"]:
        embedding = generate_embedding(chunk["text"])
        set_chunk_embedding(uuid, embedding)

# Test
if __name__ == "__main__":
    text = "This project restores biodiversity in Costa Rica."
    embedding = generate_embedding(text)
    print(f"Embedding size: {len(embedding)}")
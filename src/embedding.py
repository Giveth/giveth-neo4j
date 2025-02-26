import logging
from database import ChunkManager
from utils.openai import generate_embedding

logging.basicConfig(level=logging.INFO)


def embed_chunk(uuid: str) -> None:
    """Embeds a text chunk if it is not already embedded."""
    chunk = ChunkManager.get_chunk(uuid)
    if chunk is None:
        logging.error(f"Chunk with ID {uuid} not found.")
        return

    if not chunk.get("embedding"):
        embedding = generate_embedding(chunk["text"])
        ChunkManager.set_embedding(uuid, embedding)
        logging.info(f"Embedding stored for chunk ID: {uuid}")


if __name__ == "__main__":
    # Example usage
    text = "This project restores biodiversity in Costa Rica."
    embedding = generate_embedding(text)
    print(f"Embedding size: {len(embedding)}")

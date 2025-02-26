import hashlib
from langchain.text_splitter import RecursiveCharacterTextSplitter


def generate_chunk_uuid(chunk_text: str) -> str:
    """Generate a consistent UUID (MD5 hash) for a given chunk of text."""
    return hashlib.md5(chunk_text.encode()).hexdigest()


def chunk_text(content: str, chunk_size: int = 512, chunk_overlap: int = 50) -> list:
    """Splits text into smaller chunks while preserving meaning."""
    if not content:
        return []

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    return text_splitter.split_text(content)


if __name__ == "__main__":
    # Example usage
    sample_text = "This project helps protect Costa Rica’s rainforests."
    chunks = chunk_text(sample_text)
    print(f"Chunks: {chunks}")

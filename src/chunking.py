import hashlib
from langchain.text_splitter import RecursiveCharacterTextSplitter
from html_cleaner import clean_html

def generate_chunk_uuid(chunk_text):
    """Generate a consistent UUID for a given chunk."""
    return hashlib.md5(chunk_text.encode()).hexdigest()


def chunk_text(html_content, chunk_size=512, chunk_overlap=50):
    """Cleans HTML, then splits text into overlapping chunks."""
    clean_text = clean_html(html_content)  # 🧹 Clean HTML first

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    return text_splitter.split_text(clean_text)

# Test
if __name__ == "__main__":
    sample_text = "This project helps protect Costa Rica’s rainforests."
    chunks = chunk_text(sample_text)
    print(chunks)

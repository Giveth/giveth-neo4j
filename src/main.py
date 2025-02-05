import openai
import sqlite3
from chunking import chunk_text, generate_chunk_uuid
from database import DB_PATH, insert_chunk, get_projects
from config import OPENAI_API_KEY
from embedding import embed_chunk

openai.api_key = OPENAI_API_KEY



if __name__ == "__main__":
    projects =  get_projects()
    chunks = chunk_text(projects[0]["description"])
    for chunk in chunks:
        print(chunk)
        uuid = generate_chunk_uuid(chunk)
        insert_chunk(uuid, chunk, 1)
        embed_chunk(uuid)
        print(uuid)
        print("-" * 40)



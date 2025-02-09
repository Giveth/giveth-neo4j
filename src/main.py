import openai
import sqlite3
from chunking import chunk_text, generate_chunk_uuid
from database import DB_PATH, insert_chunk, get_giveth_projects, insert_project
from config import OPENAI_API_KEY
from embedding import embed_chunk

openai.api_key = OPENAI_API_KEY


if __name__ == "__main__":
    projects = get_giveth_projects()

    for project in projects:
        insert_project(id=project["id"], title=project["title"], description=project["description"],
                       raised_amount=project["raised_amount"], giv_power=project["giv_power"], listed=project["listed"])

        chunks = chunk_text(project["description"])
        for chunk in chunks:
            print(chunk)
            uuid = generate_chunk_uuid(chunk)
            insert_chunk(uuid, chunk, project["id"])
            embed_chunk(uuid)
            print(uuid)
            print("-" * 40)

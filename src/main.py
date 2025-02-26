import openai
import sqlite3
from helper.chunking import chunk_text, generate_chunk_uuid
from database import DonationManager, ProjectManager, ChunkManager
from config.config import OPENAI_API_KEY
from embedding import embed_chunk

openai.api_key = OPENAI_API_KEY


if __name__ == "__main__":
    projects = ProjectManager.get_projects_from_postgres()

    print(f"Found {len(projects)} projects")
    for project in projects:

        ProjectManager.save_project(project)

        chunks = chunk_text(project["description"])
        for chunk in chunks:
            print(chunk)
            uuid = generate_chunk_uuid(chunk)
            ChunkManager.save_chunk(uuid, chunk, project["id"])
            embed_chunk(uuid)
            print(uuid)
            print("-" * 40)

    donations = DonationManager.get_donations_from_postgres()
    for donation in donations:
        DonationManager.save_donation(donation)

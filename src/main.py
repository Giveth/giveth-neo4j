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
        addresses = {
            "polygon": project["polygon_address"],
            "optimism": project["optimism_address"],
            "celo": project["celo_address"],
            "base": project["base_address"],
            "arbitrum": project["arbitrum_address"],
            "gnosis": project["gnosis_address"],
            "zkevm": project["zkevm_address"],
            "ethereum_classic": project["ethereum_classic_address"],
            "stellar": project["stellar_address"],
            "solana": project["solana_address"],
        }

        socials = {
            "x": project["x"],
            "facebook": project["facebook"],
            "instagram": project["instagram"],
            "youtube": project["youtube"],
            "linkedin": project["linkedin"],
            "reddit": project["reddit"],
            "discord": project["discord"],
            "farcaster": project["farcaster"],
            "lens": project["lens"],
            "website": project["website"],
            "telegram": project["telegram"],
            "github": project["github"],
        }

        insert_project(
            id=project["id"],
            title=project["title"],
            description=project["description"],
            raised_amount=project["raised_amount"],
            giv_power=project["giv_power"],
            giv_power_rank=project["giv_power_rank"],
            listed=project["listed"],
            givbacks_eligible=project["givbacks_eligible"],
            in_active_qf_round=project["in_active_qf_round"],
            unique_donors=project["unique_donors"],
            updated_at=project["updated_at"],
            owner_wallet=project["owner_wallet"],
            addresses=addresses,
            socials=socials,
        )

        chunks = chunk_text(project["description"])
        for chunk in chunks:
            print(chunk)
            uuid = generate_chunk_uuid(chunk)
            insert_chunk(uuid, chunk, project["id"])
            embed_chunk(uuid)
            print(uuid)
            print("-" * 40)

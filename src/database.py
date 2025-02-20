import numpy as np
import sqlite3
import ast  # Used to safely convert string representation of a list to an actual list
import os
import json
from decimal import Decimal
from html_cleaner import clean_html
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from helper.project_data_parser import extract_flat_project_data

CACHE_FILE = "data/projects_cache2.json"


def get_sqlite_connection():
    """
    Create a connection to the SQLite database.
    """
    return sqlite3.connect(DB_PATH)


def get_giveth_projects():
    """Fetch projects from PostgreSQL and cache the result."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            get_giveth_projects.cache = json.load(f)
    else:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
        )
        cursor = conn.cursor()

        query = """SELECT
                    p.ID,
                    p.TITLE,
                    p.DESCRIPTION,
                    p."totalDonations",
                    p."giveBacks",
                    p.LISTED,
                    p."countUniqueDonors",
                    p."updatedAt",
                    u."walletAddress",
                    qfr."isActive",

                    COALESCE(
                        JSONB_OBJECT_AGG(
                            pa."chainType",
                            pa.networks
                        ) FILTER (WHERE pa."chainType" IS NOT NULL), '{}'::JSONB
                    ) AS addresses,

                    COALESCE(
                        JSONB_OBJECT_AGG(
                            psm."type",
                            psm."link"
                        ) FILTER (WHERE psm."link" IS NOT NULL), '{}'::JSONB
                    ) AS social_media

                    -- pipv."totalPower"

                FROM public.project p
                LEFT JOIN project_qf_rounds_qf_round pqrq ON p.id = pqrq."projectId"
                LEFT JOIN qf_round qfr ON qfr.id = pqrq."qfRoundId"
                INNER JOIN public.user u ON p."adminUserId" = u.id
                -- INNER JOIN public.project_instant_power_view pipv  ON p.id = pipv."projectId"

                LEFT JOIN (
                    SELECT 
                        pa."projectId",
                        pa."chainType",
                        JSONB_OBJECT_AGG(pa."networkId", pa."address") AS networks
                    FROM public.project_address pa
                    GROUP BY pa."projectId", pa."chainType"
                ) pa ON p.id = pa."projectId"

                LEFT JOIN public.project_social_media psm ON p.id = psm."projectId"

                WHERE p.LISTED = TRUE

                GROUP BY 
                    p.ID, p.TITLE, p.DESCRIPTION, p."totalDonations", p."giveBacks", 
                    p."updatedAt", p.LISTED, qfr."isActive", u."walletAddress"

                LIMIT 1000;"""
        # Adjust as needed
        cursor.execute(query)
        projects = cursor.fetchall()

        conn.close()
        get_giveth_projects.cache = [extract_flat_project_data(p) for p in projects]

        with open(CACHE_FILE, "w") as f:
            json.dump(get_giveth_projects.cache, f)

    return get_giveth_projects.cache


DB_PATH = "data/local_data.db"

def create_tables():
    """Create tables for storing project chunks and embeddings."""
    conn = get_sqlite_connection()
    cursor = conn.cursor()

    # Chunks Table: Stores tokenized descriptions
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chunks (
            id TEXT PRIMARY KEY,  -- UUID of the chunk
            project_id INTEGER,
            text TEXT UNIQUE,  -- Add UNIQUE constraint on uuid
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            embedding BLOB
        )
    """
    )

    # Project Table: Stores project id, title, raised_amount, giv_power
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            title TEXT,
            description TEXT,
            raised_amount REAL,
            giv_power REAL,
            listed BOOLEAN,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.commit()
    conn.close()


# insert_chunk function


def insert_chunk(uuid, text, project_id):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO chunks (id, text, project_id) VALUES (?, ?, ?)",
            (uuid, text, project_id),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Ignore if the chunk with the same UUID already exists
        pass
    finally:
        conn.close()


# get_chunk function by id


def get_chunk(uuid):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, text, embedding FROM chunks WHERE id = ?", (uuid,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0],
            "text": row[1],
            "embedding": json.loads(row[2]) if row[2] else None,
        }
    return None


# add_chunk_embedding function


def set_chunk_embedding(uuid, embedding):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE chunks SET embedding = ? WHERE id = ?", (json.dumps(embedding), uuid)
    )
    conn.commit()
    conn.close()


# Run on startup
if __name__ == "__main__":
    create_tables()
    print("✅ Local database initialized!")


# write a test to add a chunk twice
def test_add_chunk_twice():
    insert_chunk(uuid="123", text="abc", project_id=0)
    insert_chunk(uuid="123", text="abc", project_id=0)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM chunks WHERE id = '123'")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 1, "Chunk was added twice!"


# insert project


def insert_project(id, title, description, raised_amount, giv_power, listed):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO projects (id, title, description, raised_amount, giv_power, listed) 
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            raised_amount=excluded.raised_amount,
            giv_power=excluded.giv_power,
            listed=excluded.listed,
            description=excluded.description
    """,
        (id, title, description, raised_amount, giv_power, listed),
    )
    conn.commit()
    conn.close()


def get_all_projects():
    """
    Fetch all projects from SQLite.
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, title, raised_amount, giv_power, listed, updated_at, description FROM projects"
    )
    projects = cursor.fetchall()
    conn.close()

    return [
        {
            "id": row[0],
            "title": row[1],
            "raised_amount": row[2],
            "giv_power": row[3],
            "listed": bool(row[4]),
            "updated_at": row[5],
            "description": row[6],
        }
        for row in projects
    ]


def get_all_chunks():
    """
    Fetch all chunks from SQLite and correctly convert the embedding stored as a string into a float list.
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, project_id, text, created_at, embedding FROM chunks WHERE embedding IS NOT NULL"
    )
    chunks = cursor.fetchall()
    conn.close()

    formatted_chunks = []
    for row in chunks:
        embedding_blob = row[4]  # The stored embedding

        if embedding_blob is None:
            continue

        # Convert string representation of a list to an actual list
        if isinstance(embedding_blob, str):
            # Safely parse the string to a Python list
            embedding_array = ast.literal_eval(embedding_blob)
        else:
            embedding_array = np.frombuffer(embedding_blob, dtype=np.float32).tolist()

        formatted_chunks.append(
            {
                "id": row[0],
                "project_id": row[1],
                "text": row[2],
                "created_at": row[3],
                "embedding": embedding_array,
            }
        )

    return formatted_chunks


if __name__ == "__main__":
    create_tables()
    # test_add_chunk_twice()
    print("✅ Test passed!")

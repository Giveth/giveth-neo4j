from decimal import Decimal
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

CACHE_FILE = "data/projects_cache.json"

def get_giveth_projects():
    """Fetch projects from PostgreSQL and cache the result."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            get_giveth_projects.cache = json.load(f)
    else:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        cursor = conn.cursor()

        query = """SELECT
                    ID,
                    TITLE,
                    DESCRIPTION,
                    "totalDonations",
                    "updatedAt",
                    LISTED,
                    "totalPower"
                FROM
                    PROJECT inner join public.project_instant_power_view  on project.id = project_instant_power_view."projectId"
                WHERE
                    LISTED = TRUE
                ORDER BY
                    "totalPower" DESC
                LIMIT
                    10;"""
          # Adjust as needed
        cursor.execute(query)
        projects = cursor.fetchall()

        conn.close()
        print(projects[0][6])
        get_giveth_projects.cache = [{
            "id": p[0],
            "title": p[1],
            "description": p[2],
            "raised_amount": p[3],
            "updated_at": p[4].isoformat() if p[4] else None,
            "listed": bool(p[5]),
            "giv_power": float(p[6]),
            } for p in projects]

        with open(CACHE_FILE, "w") as f:
            json.dump(get_giveth_projects.cache, f)

    return get_giveth_projects.cache

import sqlite3
import json
import os

DB_PATH = "data/local_data.db"

def create_tables():
    """Create tables for storing project chunks and embeddings."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Chunks Table: Stores tokenized descriptions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id TEXT PRIMARY KEY,  -- UUID of the chunk
            project_id INTEGER,
            text TEXT UNIQUE,  -- Add UNIQUE constraint on uuid
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            embedding BLOB
        )
    """)

    # Project Table: Stores project id, title, raised_amount, giv_power
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            title TEXT,
            raised_amount REAL,
            giv_power REAL,
            listed BOOLEAN,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

# insert_chunk function
def insert_chunk(uuid, text, project_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO chunks (id, text, project_id) VALUES (?, ?, ?)", 
                       (uuid, text, project_id))
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
            "embedding": json.loads(row[2]) if row[2] else None
        }
    return None

# add_chunk_embedding function
def set_chunk_embedding(uuid, embedding):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE chunks SET embedding = ? WHERE id = ?", 
                   (json.dumps(embedding), uuid))
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
def insert_project(id, title, raised_amount, giv_power, listed):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO projects (id, title, raised_amount, giv_power, listed) VALUES (?, ?, ?, ?, ?)", 
                   (id, title, raised_amount, giv_power, listed))
    conn.commit()
    conn.close()

# Run the test
if __name__ == "__main__":
    create_tables()
    test_add_chunk_twice()
    print("✅ Test passed!")

import sqlite3
from database import ProjectManager
from helper.chunking import chunk_text
from database import DB_PATH


def store_chunks():
    """Fetch a project, tokenize it, and store chunks in SQLite."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    projectManager = ProjectManager()

    # Fetch project (ID = 1)
    project = projectManager.get_all_projects()[0]  # Assumes first project is ID=1
    chunks = chunk_text(project["description"])

    for chunk in chunks:
        cursor.execute("SELECT id FROM chunks WHERE id = ?", (chunk["id"],))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                "INSERT INTO chunks (id, project_id, title, chunk_text) VALUES (?, ?, ?, ?)",
                (chunk["id"], project["id"], project["title"], chunk["text"]),
            )

    conn.commit()
    conn.close()
    print(f"✅ Stored {len(chunks)} chunks for project: {project['title']}")


if __name__ == "__main__":
    store_chunks()

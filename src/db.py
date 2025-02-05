import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

def get_projects():
    """Fetch projects from PostgreSQL."""
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cursor = conn.cursor()

    query = "SELECT id, title, description FROM projects LIMIT 10;"  # Adjust as needed
    cursor.execute(query)
    projects = cursor.fetchall()

    conn.close()
    return [{"id": p[0], "title": p[1], "description": p[2]} for p in projects]

# Test
if __name__ == "__main__":
    projects = get_projects()
    print(projects)

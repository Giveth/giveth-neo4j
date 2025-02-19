import sqlite3
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, '..', 'data', 'local_data.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create api_keys table to store API keys
cursor.execute('''
CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT NOT NULL,
    api_key TEXT NOT NULL UNIQUE
)
''')

# Add user API key
cursor.execute('''
INSERT INTO api_keys (user, api_key) VALUES (?, ?)
''', ('sample_name', 'sample_unique_api_key_12345')) # Replace with your desired info

# Create a new table for tracking API key usage
cursor.execute('''
CREATE TABLE IF NOT EXISTS api_key_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_key TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    request_body TEXT
)
''')

conn.commit()
conn.close()
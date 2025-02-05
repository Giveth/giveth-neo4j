import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "your_database")
DB_USER = os.getenv("DB_USER", "your_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
DB_PORT = os.getenv("DB_PORT", "5432")

# OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

import numpy as np
import sqlite3
import ast
import os
import json
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import psycopg2
from pathlib import Path

from config.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
from helper.html_cleaner import clean_html
from helper.project_data_parser import extract_flat_project_data

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("database")

# Constants
DATA_DIR = Path("data")
PROJECT_CACHE_FILE = DATA_DIR / "projects_cache.json"
DONATION_CACHE_FILE = DATA_DIR / "donations_cache.json"
DB_PATH = DATA_DIR / "local_data.db"

# Ensure data directory exists
DATA_DIR.mkdir(exist_ok=True)


class PostgresConnector:
    """Handles connections and queries to PostgreSQL database."""

    @staticmethod
    def get_connection():
        """Create and return a PostgreSQL database connection."""
        try:
            connection = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
            )
            return connection
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise

    @staticmethod
    def execute_query(query: str, params: tuple = None) -> List[tuple]:
        """Execute a query and return results."""
        connection = PostgresConnector.get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            return results
        except psycopg2.Error as e:
            logger.error(f"Query execution error: {e}")
            raise
        finally:
            cursor.close()
            connection.close()


class SQLiteConnector:
    """Handles connections and operations for SQLite database."""

    @staticmethod
    def get_connection():
        """Create and return an SQLite database connection."""
        try:
            return sqlite3.connect(DB_PATH)
        except sqlite3.Error as e:
            logger.error(f"SQLite connection error: {e}")
            raise

    @staticmethod
    def execute_query(
        query: str, params: tuple = None, fetch: bool = True
    ) -> Optional[List[tuple]]:
        """Execute a query and optionally return results."""
        connection = SQLiteConnector.get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute(query, params or ())

            if fetch:
                return cursor.fetchall()
            else:
                connection.commit()
                return None
        except sqlite3.Error as e:
            logger.error(f"Query execution error: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def execute_many(query: str, params_list: List[tuple]) -> None:
        """Execute multiple similar queries with different parameters."""
        connection = SQLiteConnector.get_connection()
        cursor = connection.cursor()

        try:
            cursor.executemany(query, params_list)
            connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Batch execution error: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


class DatabaseInitializer:
    """Handles database schema creation and initialization."""

    @staticmethod
    def create_tables():
        """Create necessary tables if they don't exist."""
        connection = SQLiteConnector.get_connection()
        cursor = connection.cursor()

        try:
            # Projects table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    raised_amount REAL,
                    giv_power REAL,
                    giv_power_rank INTEGER,
                    listed BOOLEAN,
                    givbacks_eligible BOOLEAN,
                    in_active_qf_round BOOLEAN,
                    unique_donors INTEGER,
                    owner_wallet TEXT,

                    polygon_address TEXT,
                    celo_address TEXT,
                    base_address TEXT,
                    solana_address TEXT,
                    ethereum_address TEXT,
                    arbitrum_address TEXT,
                    optimism_address TEXT,
                    gnosis_address TEXT,
                    stellar_address TEXT,
                    zkevm_address TEXT,
                    ethereum_classic_address TEXT,

                    x TEXT,
                    discord TEXT,
                    telegram TEXT,
                    instagram TEXT,
                    facebook TEXT,
                    github TEXT,
                    linkedin TEXT,
                    website TEXT,
                    farcaster TEXT,
                    youtube TEXT,
                    reddit TEXT,
                    lens TEXT,

                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Chunks table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS chunks (
                    id TEXT PRIMARY KEY,
                    project_id INTEGER,
                    text TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    embedding BLOB,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
            """
            )

            # Donations table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS donations (
                    id INTEGER PRIMARY KEY,
                    project_id INTEGER,
                    tx_hash TEXT,
                    to_address TEXT,
                    from_address TEXT,
                    currency TEXT,
                    anonymous BOOLEAN,
                    amount REAL,
                    value_usd REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    chain_id INTEGER,
                    token_address TEXT,
                    chain_type TEXT,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
            """
            )

            connection.commit()
            logger.info("Database schema initialized successfully")

        except sqlite3.Error as e:
            logger.error(f"Table creation error: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


class ProjectManager:
    """Handles operations related to projects."""

    @staticmethod
    def get_projects_from_postgres() -> List[Dict[str, Any]]:
        """Fetch projects from PostgreSQL database with caching."""
        # Check if cached data exists
        if PROJECT_CACHE_FILE.exists():
            with open(PROJECT_CACHE_FILE, "r") as f:
                logger.info("Using cached project data")
                return json.load(f)

        # Query to fetch project data
        query = """
            SELECT
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
                ) AS social_media,

                pipv."totalPower",
                pipv."powerRank"

            FROM public.project p
            LEFT JOIN project_qf_rounds_qf_round pqrq ON p.id = pqrq."projectId"
            LEFT JOIN qf_round qfr ON qfr.id = pqrq."qfRoundId"
            INNER JOIN public.user u ON p."adminUserId" = u.id
            INNER JOIN public.project_instant_power_view pipv ON p.id = pipv."projectId"

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
                p."updatedAt", p.LISTED, qfr."isActive", u."walletAddress", pipv."totalPower", pipv."powerRank"

            ORDER BY
                pipv."totalPower" DESC

            LIMIT 100;
        """

        results = PostgresConnector.execute_query(query)
        projects = [extract_flat_project_data(p) for p in results]

        # Cache the results
        with open(PROJECT_CACHE_FILE, "w") as f:
            json.dump(projects, f)

        logger.info(f"Fetched {len(projects)} projects from PostgreSQL")
        return projects

    @staticmethod
    def save_project(project_data: Dict[str, Any]) -> None:
        """Insert or update a project in the SQLite database."""
        # Extract fields from project data
        project_id = project_data["id"]
        title = project_data["title"]
        description = project_data.get("description", "")
        raised_amount = project_data.get("raised_amount", 0)
        giv_power = project_data.get("giv_power", 0)
        giv_power_rank = project_data.get("giv_power_rank", 0)
        listed = project_data.get("listed", True)
        givbacks_eligible = project_data.get("givbacks_eligible", False)
        in_active_qf_round = project_data.get("in_active_qf_round", False)
        unique_donors = project_data.get("unique_donors", 0)
        updated_at = project_data.get("updated_at", datetime.now().isoformat())
        owner_wallet = project_data.get("owner_wallet", "")

        addresses = {
            "polygon": project_data["polygon_address"],
            "optimism": project_data["optimism_address"],
            "celo": project_data["celo_address"],
            "base": project_data["base_address"],
            "arbitrum": project_data["arbitrum_address"],
            "gnosis": project_data["gnosis_address"],
            "zkevm": project_data["zkevm_address"],
            "ethereum_classic": project_data["ethereum_classic_address"],
            "stellar": project_data["stellar_address"],
            "solana": project_data["solana_address"],
        }

        socials = {
            "x": project_data["x"],
            "facebook": project_data["facebook"],
            "instagram": project_data["instagram"],
            "youtube": project_data["youtube"],
            "linkedin": project_data["linkedin"],
            "reddit": project_data["reddit"],
            "discord": project_data["discord"],
            "farcaster": project_data["farcaster"],
            "lens": project_data["lens"],
            "website": project_data["website"],
            "telegram": project_data["telegram"],
            "github": project_data["github"],
        }

        query = """
            INSERT INTO projects (
                id, title, description, raised_amount, giv_power, giv_power_rank, listed, 
                givbacks_eligible, in_active_qf_round, unique_donors, owner_wallet, 
                polygon_address, celo_address, base_address, solana_address, 
                ethereum_address, arbitrum_address, optimism_address, gnosis_address, 
                stellar_address, zkevm_address, ethereum_classic_address, x, 
                discord, telegram, instagram, facebook, github, linkedin, website, 
                farcaster, youtube, reddit, lens, updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                raised_amount = excluded.raised_amount,
                giv_power = excluded.giv_power,
                giv_power_rank = excluded.giv_power_rank,
                listed = excluded.listed,
                description = excluded.description,
                givbacks_eligible = excluded.givbacks_eligible,
                in_active_qf_round = excluded.in_active_qf_round,
                unique_donors = excluded.unique_donors,
                owner_wallet = excluded.owner_wallet,
                polygon_address = excluded.polygon_address,
                celo_address = excluded.celo_address,
                base_address = excluded.base_address,
                solana_address = excluded.solana_address,
                ethereum_address = excluded.ethereum_address,
                arbitrum_address = excluded.arbitrum_address,
                optimism_address = excluded.optimism_address,
                gnosis_address = excluded.gnosis_address,
                stellar_address = excluded.stellar_address,
                zkevm_address = excluded.zkevm_address,
                ethereum_classic_address = excluded.ethereum_classic_address,
                x = excluded.x,
                discord = excluded.discord,
                telegram = excluded.telegram,
                instagram = excluded.instagram,
                facebook = excluded.facebook,
                github = excluded.github,
                linkedin = excluded.linkedin,
                website = excluded.website,
                farcaster = excluded.farcaster,
                youtube = excluded.youtube,
                reddit = excluded.reddit,
                lens = excluded.lens,
                updated_at = excluded.updated_at
        """

        params = (
            project_id,
            title,
            description,
            raised_amount,
            giv_power,
            giv_power_rank,
            listed,
            givbacks_eligible,
            in_active_qf_round,
            unique_donors,
            owner_wallet,
            addresses.get("polygon"),
            addresses.get("celo"),
            addresses.get("base"),
            addresses.get("solana"),
            addresses.get("ethereum"),
            addresses.get("arbitrum"),
            addresses.get("optimism"),
            addresses.get("gnosis"),
            addresses.get("stellar"),
            addresses.get("zkevm"),
            addresses.get("ethereum_classic"),
            socials.get("x"),
            socials.get("discord"),
            socials.get("telegram"),
            socials.get("instagram"),
            socials.get("facebook"),
            socials.get("github"),
            socials.get("linkedin"),
            socials.get("website"),
            socials.get("farcaster"),
            socials.get("youtube"),
            socials.get("reddit"),
            socials.get("lens"),
            updated_at,
        )

        SQLiteConnector.execute_query(query, params, fetch=False)

    @staticmethod
    def get_all_projects() -> List[Dict[str, Any]]:
        """Retrieve all projects from SQLite database."""
        query = """
            SELECT 
                id, title, raised_amount, giv_power, giv_power_rank, listed, 
                updated_at, givbacks_eligible, description, in_active_qf_round, 
                unique_donors, owner_wallet, polygon_address, celo_address, 
                base_address, solana_address, ethereum_address, arbitrum_address, 
                optimism_address, gnosis_address, stellar_address, zkevm_address, 
                ethereum_classic_address, x, discord, telegram, instagram, facebook, 
                github, linkedin, website, farcaster, youtube, reddit, lens 
            FROM projects
        """

        results = SQLiteConnector.execute_query(query)

        projects = []
        for row in results:
            projects.append(
                {
                    "id": row[0],
                    "title": row[1],
                    "raised_amount": row[2],
                    "giv_power": row[3],
                    "giv_power_rank": row[4],
                    "listed": bool(row[5]),
                    "updated_at": row[6],
                    "givbacks_eligible": bool(row[7]),
                    "description": row[8],
                    "in_active_qf_round": bool(row[9]),
                    "unique_donors": row[10],
                    "owner_wallet": row[11],
                    "addresses": {
                        "polygon": row[12],
                        "celo": row[13],
                        "base": row[14],
                        "solana": row[15],
                        "ethereum": row[16],
                        "arbitrum": row[17],
                        "optimism": row[18],
                        "gnosis": row[19],
                        "stellar": row[20],
                        "zkevm": row[21],
                        "ethereum_classic": row[22],
                    },
                    "socials": {
                        "x": row[23],
                        "discord": row[24],
                        "telegram": row[25],
                        "instagram": row[26],
                        "facebook": row[27],
                        "github": row[28],
                        "linkedin": row[29],
                        "website": row[30],
                        "farcaster": row[31],
                        "youtube": row[32],
                        "reddit": row[33],
                        "lens": row[34],
                    },
                }
            )

        return projects


class ChunkManager:
    """Handles operations related to text chunks."""

    @staticmethod
    def save_chunk(chunk_id: str, text: str, project_id: int) -> None:
        """Insert a new chunk if it doesn't already exist."""
        try:
            query = "INSERT INTO chunks (id, text, project_id) VALUES (?, ?, ?)"
            SQLiteConnector.execute_query(
                query, (chunk_id, text, project_id), fetch=False
            )
            logger.debug(f"Chunk {chunk_id} saved successfully")
        except sqlite3.IntegrityError:
            logger.debug(f"Chunk {chunk_id} already exists, skipping")

    @staticmethod
    def get_chunk(chunk_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific chunk by ID."""
        query = "SELECT id, text, embedding, project_id FROM chunks WHERE id = ?"
        results = SQLiteConnector.execute_query(query, (chunk_id,))

        if not results:
            return None

        row = results[0]
        return {
            "id": row[0],
            "text": row[1],
            "embedding": json.loads(row[2]) if row[2] else None,
            "project_id": row[3],
        }

    @staticmethod
    def set_embedding(chunk_id: str, embedding: List[float]) -> None:
        """Update the embedding for a specific chunk."""
        query = "UPDATE chunks SET embedding = ? WHERE id = ?"
        SQLiteConnector.execute_query(
            query, (json.dumps(embedding), chunk_id), fetch=False
        )
        logger.debug(f"Embedding updated for chunk {chunk_id}")

    @staticmethod
    def get_all_chunks() -> List[Dict[str, Any]]:
        """Retrieve all chunks with embeddings."""
        query = """
            SELECT id, project_id, text, created_at, embedding 
            FROM chunks 
            WHERE embedding IS NOT NULL
        """
        results = SQLiteConnector.execute_query(query)

        chunks = []
        for row in results:
            embedding_blob = row[4]

            if embedding_blob is None:
                continue

            # Parse embedding from string or binary
            if isinstance(embedding_blob, str):
                embedding_array = ast.literal_eval(embedding_blob)
            else:
                embedding_array = np.frombuffer(
                    embedding_blob, dtype=np.float32
                ).tolist()

            chunks.append(
                {
                    "id": row[0],
                    "project_id": row[1],
                    "text": row[2],
                    "created_at": row[3],
                    "embedding": embedding_array,
                }
            )

        return chunks


class DonationManager:
    """Handles operations related to donations."""

    @staticmethod
    def get_donations_from_postgres() -> List[Dict[str, Any]]:
        """Fetch donations from PostgreSQL database."""

        if DONATION_CACHE_FILE.exists():
            with open(DONATION_CACHE_FILE, "r") as f:
                return json.load(f)

        # Query donations from PostgreSQL
        query = """
            SELECT
                DONATION.id,
                "projectId",
                "transactionId",
                "toWalletAddress",
                "fromWalletAddress",
                currency,
                "anonymous",
                DONATION.amount,
                "valueUsd",
                "createdAt",
                "transactionNetworkId",
                "tokenAddress",
                "chainType"
            FROM
                DONATION
                INNER JOIN (
                    SELECT
                        ID,
                        "totalPower" AMOUNT
                    FROM
                        PROJECT
                        INNER JOIN PUBLIC.PROJECT_INSTANT_POWER_VIEW PIPV ON PROJECT.ID = PIPV."projectId"
                    ORDER BY
                        PIPV."totalPower" DESC
                    LIMIT
                        100
                ) AS P ON DONATION."projectId" = P.ID
                WHERE DONATION."valueUsd" >= 1
        """

        # Execute query
        results = PostgresConnector.execute_query(query)

        donations = []
        for donation in results:
            donations.append(
                {
                    "id": donation[0],
                    "projectId": donation[1],
                    "transactionId": donation[2],
                    "toWalletAddress": donation[3],
                    "fromWalletAddress": donation[4],
                    "currency": donation[5],
                    "anonymous": donation[6],
                    "amount": donation[7],
                    "valueUsd": donation[8],
                    "createdAt": donation[9].isoformat() if donation[9] else None,
                    "transactionNetworkId": donation[10],
                    "tokenAddress": donation[11],
                    "chainType": donation[12],
                }
            )

        # Cache the results
        with open(DONATION_CACHE_FILE, "w") as f:
            json.dump(donations, f)

        return donations

    @staticmethod
    def save_donation(donation_data: Dict[str, Any]) -> None:
        """Insert a donation if it doesn't already exist."""
        query = """
            INSERT INTO donations (
                id, project_id, tx_hash, to_address, from_address, currency, 
                anonymous, amount, value_usd, created_at, chain_id, token_address, chain_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
        """

        params = (
            donation_data.get("id"),
            donation_data.get("projectId"),
            donation_data.get("transactionId"),
            donation_data.get("toWalletAddress"),
            donation_data.get("fromWalletAddress"),
            donation_data.get("currency"),
            donation_data.get("anonymous", False),
            donation_data.get("amount"),
            donation_data.get("valueUsd"),
            donation_data.get("createdAt"),
            donation_data.get("transactionNetworkId"),
            donation_data.get("tokenAddress"),
            donation_data.get("chainType"),
        )

        SQLiteConnector.execute_query(query, params, fetch=False)

    @staticmethod
    def get_all_donations() -> List[Dict[str, Any]]:
        """Retrieve all donations from SQLite database."""
        query = """
            SELECT 
                id, project_id, tx_hash, to_address, from_address, currency, 
                anonymous, amount, value_usd, created_at, chain_id, token_address, chain_type 
            FROM donations
        """

        results = SQLiteConnector.execute_query(query)

        donations = []
        for row in results:
            donations.append(
                {
                    "id": row[0],
                    "project_id": row[1],
                    "tx_hash": row[2],
                    "to_address": row[3],
                    "from_address": row[4],
                    "currency": row[5],
                    "anonymous": bool(row[6]),
                    "amount": row[7],
                    "value_usd": row[8],
                    "created_at": row[9],
                    "chain_id": row[10],
                    "token_address": row[11],
                    "chain_type": row[12],
                }
            )

        return donations


class DataSynchronizer:
    """Handles synchronization between PostgreSQL and SQLite databases."""

    @staticmethod
    def sync_projects():
        """Synchronize projects from PostgreSQL to SQLite."""
        projects = ProjectManager.get_projects_from_postgres()

        for project in projects:
            ProjectManager.save_project(project)

        logger.info(f"Synchronized {len(projects)} projects")

    @staticmethod
    def sync_donations():
        """Synchronize donations from PostgreSQL to SQLite."""
        donations = DonationManager.get_donations_from_postgres()

        for donation in donations:
            DonationManager.save_donation(donation)

        logger.info(f"Synchronized {len(donations)} donations")


def test_chunk_duplicate_insertion():
    """Test that duplicate chunk insertions are handled properly."""
    DatabaseInitializer.create_tables()

    # Insert the same chunk twice
    ChunkManager.save_chunk("test-chunk-123", "Test text", 1)
    ChunkManager.save_chunk("test-chunk-123", "Test text", 1)

    # Check that only one copy exists
    connection = SQLiteConnector.get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM chunks WHERE id = 'test-chunk-123'")
    count = cursor.fetchone()[0]
    connection.close()

    assert count == 1, "Chunk was added twice!"
    logger.info("Duplicate chunk insertion test passed")


def initialize_database():
    """Initialize the database and create required tables."""
    DatabaseInitializer.create_tables()
    logger.info("Local database initialized!")


if __name__ == "__main__":
    initialize_database()
    test_chunk_duplicate_insertion()

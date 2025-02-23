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

PROJECT_CACHE_FILE = "data/projects_cache.json"
PROJECT_CACHE_FILE = "data/donations_cache.json"
DB_PATH = "data/local_data.db"


def get_sqlite_connection():
    """
    Create a connection to the SQLite database.
    """
    return sqlite3.connect(DB_PATH)


def get_giveth_projects():
    """Fetch projects from PostgreSQL and cache the result."""
    if os.path.exists(PROJECT_CACHE_FILE):
        with open(PROJECT_CACHE_FILE, "r") as f:
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

                LIMIT 100;"""

        # Adjust as needed
        cursor.execute(query)
        projects = cursor.fetchall()

        conn.close()
        get_giveth_projects.cache = [extract_flat_project_data(p) for p in projects]

        with open(PROJECT_CACHE_FILE, "w") as f:
            json.dump(get_giveth_projects.cache, f)

    return get_giveth_projects.cache


def get_giveth_donations():
    """Fetch donations from PostgreSQL."""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    cursor = conn.cursor()

    query = """SELECT
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

    cursor.execute(query)
    donations = cursor.fetchall()
    conn.close()

    return [
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
        } for donation in donations
    ]



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

    # Project Table: Stores project id, title, raised_amount, giv_power...
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

    # Donation with id, "txHash", "toAddress", "fromAddress", currency, "anonymous", amount, "valueUsd", "createdAt", "chainId", "tokenAddress", "chainType"
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
            chain_type TEXT
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


def insert_project(
    id,
    title,
    description,
    raised_amount,
    giv_power,
    giv_power_rank,
    listed,
    givbacks_eligible,
    in_active_qf_round,
    unique_donors,
    updated_at,
    owner_wallet,
    addresses,
    socials,
):
    conn = get_sqlite_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
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
    """,
        (
            id,
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
            addresses.get("polygon", None),
            addresses.get("celo", None),
            addresses.get("base", None),
            addresses.get("solana", None),
            addresses.get("ethereum", None),
            addresses.get("arbitrum", None),
            addresses.get("optimism", None),
            addresses.get("gnosis", None),
            addresses.get("stellar", None),
            addresses.get("zkevm", None),
            addresses.get("ethereum_classic", None),
            socials.get("x", None),
            socials.get("discord", None),
            socials.get("telegram", None),
            socials.get("instagram", None),
            socials.get("facebook", None),
            socials.get("github", None),
            socials.get("linkedin", None),
            socials.get("website", None),
            socials.get("farcaster", None),
            socials.get("youtube", None),
            socials.get("reddit", None),
            socials.get("lens", None),
            updated_at,
        ),
    )
    conn.commit()
    conn.close()


def insert_donation(
    id,
    project_id,
    tx_hash,
    to_address,
    from_address,
    currency,
    anonymous,
    amount,
    value_usd,
    created_at,
    chain_id,
    token_address,
    chain_type,
):
    """ Insert a donation into the SQLite database. """

    conn = get_sqlite_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO donations (
            id, project_id, tx_hash, to_address, from_address, currency, anonymous, amount, value_usd, created_at, chain_id, token_address, chain_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
    """,
        (
            id,
            project_id,
            tx_hash,
            to_address,
            from_address,
            currency,
            anonymous,
            amount,
            value_usd,
            created_at,
            chain_id,
            token_address,
            chain_type,
        ),
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
        "SELECT id, title, raised_amount, giv_power, giv_power_rank, listed, updated_at, givbacks_eligible, description, in_active_qf_round, unique_donors, owner_wallet, polygon_address, celo_address, base_address, solana_address, ethereum_address, arbitrum_address, optimism_address, gnosis_address, stellar_address, zkevm_address, ethereum_classic_address, x, discord, telegram, instagram, facebook, github, linkedin, website, farcaster, youtube, reddit, lens FROM projects"
    )
    projects = cursor.fetchall()
    conn.close()

    return [
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

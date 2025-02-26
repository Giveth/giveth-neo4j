from database import ChunkManager, ProjectManager, DonationManager
from neo4j import GraphDatabase
from config.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


def get_neo4j_driver():
    """
    Get a Neo4j driver instance.
    """
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def insert_projects_to_neo4j():
    """
    Insert all projects from SQLite into Neo4j.
    """
    projects = ProjectManager.get_all_projects()

    query = """
    UNWIND $data AS row
    MERGE (p:Project {id: row.id})
    ON CREATE SET 
        p.title = row.title,
        p.raised_amount = row.raised_amount,
        p.giv_power = row.giv_power,
        p.giv_power_rank = row.giv_power_rank,
        p.listed = row.listed,
        p.givbacks_eligible = row.givbacks_eligible,
        p.in_active_qf_round = row.in_active_qf_round,
        p.unique_donors = row.unique_donors,
        p.owner_wallet = row.owner_wallet,
        p.ethereum_address = row.addresses.ethereum,
        p.polygon_address = row.addresses.polygon,
        p.optimism_address = row.addresses.optimism,
        p.celo_address = row.addresses.celo,
        p.base_address = row.addresses.base,
        p.arbitrum_address = row.addresses.arbitrum,
        p.gnosis_address = row.addresses.gnosis,
        p.zkevm_address = row.addresses.zkevm,
        p.ethereum_classic_address = row.addresses.ethereum_classic,
        p.stellar_address = row.addresses.stellar,
        p.solana_address = row.addresses.solana,
        p.x = row.socials.x,
        p.facebook = row.socials.facebook,
        p.instagram = row.socials.instagram,
        p.youtube = row.socials.youtube,
        p.linkedin = row.socials.linkedin,
        p.reddit = row.socials.reddit,
        p.discord = row.socials.discord,
        p.farcaster = row.socials.farcaster,
        p.lens = row.socials.lens,
        p.website = row.socials.website,
        p.telegram = row.socials.telegram,
        p.github = row.socials.github,
        p.updated_at = row.updated_at
    ON MATCH SET 
        p.raised_amount = row.raised_amount,
        p.giv_power = row.giv_power,
        p.listed = row.listed,
        p.givbacks_eligible = row.givbacks_eligible,
        p.in_active_qf_round = row.in_active_qf_round,
        p.unique_donors = row.unique_donors,
        p.owner_wallet = row.owner_wallet,
        p.ethereum_address = row.addresses.ethereum,
        p.polygon_address = row.addresses.polygon,
        p.optimism_address = row.addresses.optimism,
        p.celo_address = row.addresses.celo,
        p.base_address = row.addresses.base,
        p.arbitrum_address = row.addresses.arbitrum,
        p.gnosis_address = row.addresses.gnosis,
        p.zkevm_address = row.addresses.zkevm,
        p.ethereum_classic_address = row.addresses.ethereum_classic,
        p.stellar_address = row.addresses.stellar,
        p.solana_address = row.addresses.solana,
        p.x = row.socials.x,
        p.facebook = row.socials.facebook,
        p.instagram = row.socials.instagram,
        p.youtube = row.socials.youtube,
        p.linkedin = row.socials.linkedin,
        p.reddit = row.socials.reddit,
        p.discord = row.socials.discord,
        p.farcaster = row.socials.farcaster,
        p.lens = row.socials.lens,
        p.website = row.socials.website,
        p.telegram = row.socials.telegram,
        p.github = row.socials.github,
        p.updated_at = row.updated_at
    """

    with get_neo4j_driver() as driver:
        with driver.session() as session:
            session.run(query, data=projects)


def insert_chunks_to_neo4j():
    """
    Insert all chunks from SQLite into Neo4j and link them to projects.
    """
    chunks = ChunkManager.get_all_chunks()

    query = """
    UNWIND $data AS row
    MATCH (p:Project {id: row.project_id})  // Ensure the project exists
    MERGE (c:Chunk {id: row.id})
    ON CREATE SET 
        c.text = row.text,
        c.created_at = row.created_at,
        c.embedding = row.embedding
    MERGE (p)-[:HAS_CHUNK]->(c)  // Create relationship
    """

    with get_neo4j_driver() as driver:
        with driver.session() as session:
            session.run(query, data=chunks)


def insert_donations_to_neo4j():
    """
    Insert all donations from SQLite into Neo4j.
    """
    donations = DonationManager.get_all_donations()

    query = """
    UNWIND $data AS row
    MATCH (p:Project {id: row.project_id})  // Ensure the project exists
    MERGE (d:Donation {id: row.id})
    ON CREATE SET 
        d.id = row.id,
        d.project_id = row.project_id,
        d.tx_hash = row.tx_hash,
        d.to_address = row.to_address,
        d.from_address = row.from_address,
        d.currency = row.currency,
        d.anonymous = row.anonymous,
        d.amount = row.amount,
        d.value_usd = row.value_usd,
        d.created_at = row.created_at,
        d.chain_id = row.chain_id,
        d.token_address = row.token_address,
        d.chain_type = row.chain_type
    MERGE (p)-[:HAS_DONATION]->(d)  // Create relationship
    """

    with get_neo4j_driver() as driver:
        with driver.session() as session:
            session.run(query, data=donations)


if __name__ == "__main__":
    from neo4j import GraphDatabase


def test_neo4j_connection():
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j Connected' AS message")
        print(result.single()["message"])


if __name__ == "__main__":
    test_neo4j_connection()

    # Example Usage
    insert_projects_to_neo4j()
    print("✅ Projects inserted into Neo4j!")

    # Example Usage
    insert_chunks_to_neo4j()
    print("✅ Chunks inserted and linked to projects in Neo4j!")

    # Example Usage
    insert_donations_to_neo4j()
    print("✅ Donations inserted into Neo4j!")

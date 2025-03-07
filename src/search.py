from neo4j_utils import Neo4jImporter
from utils.openai import generate_embedding


# def search_similar_projects(query_text, top_n=5):
#     query_embedding = generate_embedding(query_text)

#     query = """
#     WITH $queryVector AS queryVector
#     MATCH (c:Chunk)
#     WITH c, gds.similarity.cosine(c.embedding, queryVector) AS similarity
#     ORDER BY similarity DESC
#     RETURN c.text AS chunk, similarity LIMIT $top_n
#     """
#     importer = Neo4jImporter()
#     with importer.get_driver() as driver:
#         with driver.session() as session:
#             result = session.run(query, queryVector=query_embedding, top_n=top_n)
#             return result.data()


# query_text = "climate change impact on renewable energy"
# results = search_similar_projects(query_text)
# print(json.dumps(results, indent=4))
def search_projects_with_chunks(query_text, similarity_threshold=0.7):
    """
    Perform a semantic search to return projects and their related chunks, ordered by average similarity.
    """
    # Generate embedding for the query
    query_embedding = generate_embedding(query_text)

    # Define the Cypher query
    query = """
    MATCH (p:Project)-[:HAS_CHUNK]->(c:Chunk)
    WHERE p.listed = true
    WITH 
        p, 
        c,
        gds.similarity.cosine(c.embedding, $queryVector) AS similarity
    WHERE similarity > $similarityThreshold
    WITH 
        p.id AS project_id,
        p.title AS project_title,
        p.raised_amount AS raised_amount,
        p.giv_power AS giv_power,
        p.giv_power_rank AS giv_power_rank,
        p.givbacks_eligible AS givbacks_eligible,
        p.in_active_qf_round AS in_active_qf_round,
        p.unique_donors AS unique_donors,
        p.owner_wallet AS owner_wallet,
        p.ethereum_address AS ethereum_address,
        p.polygon_address AS polygon_address,
        p.optimism_address AS optimism_address,
        p.celo_address AS celo_address,
        p.base_address AS base_address,
        p.arbitrum_address AS arbitrum_address,
        p.gnosis_address AS gnosis_address,
        p.zkevm_address AS zkevm_address,
        p.ethereum_classic_address AS ethereum_classic_address,
        p.stellar_address AS stellar_address,
        p.solana_address AS solana_address,
        p.x AS x,
        p.facebook AS facebook,
        p.instagram AS instagram,
        p.youtube AS youtube,
        p.linkedin AS linkedin,
        p.reddit AS reddit,
        p.discord AS discord,
        p.farcaster AS farcaster,
        p.lens AS lens,
        p.website AS website,
        p.telegram AS telegram,
        p.github AS github,
        AVG(similarity) AS average_similarity,  // Calculate average similarity
        COLLECT({chunk_id: c.id, text: c.text, similarity: similarity}) AS related_chunks
    RETURN 
        project_id,
        project_title,
        raised_amount,
        giv_power,
        giv_power_rank,
        givbacks_eligible,
        in_active_qf_round,
        unique_donors,
        owner_wallet,
        ethereum_address,
        polygon_address,
        optimism_address,
        celo_address,
        base_address,
        arbitrum_address,
        gnosis_address,
        zkevm_address,
        ethereum_classic_address,
        stellar_address,
        solana_address,
        x,
        facebook,
        instagram,
        youtube,
        linkedin,
        reddit,
        discord,
        farcaster,
        lens,
        website,
        telegram,
        github,
        average_similarity,  // Return average similarity
        related_chunks
    ORDER BY average_similarity DESC LIMIT 5 // Order by most relevant projects
    """

    # Execute the query
    importer = Neo4jImporter()
    
    with importer.get_driver() as driver:
        with driver.session() as session:
            result = session.run(
                query,
                parameters={
                    "queryVector": query_embedding,  # Pass query embedding
                    "similarityThreshold": similarity_threshold,  # Pass threshold
                },
            )
            # Transform the results into a structured format
            return [
                {
                    "project_id": record["project_id"],
                    "project_title": record["project_title"],
                    "raised_amount": record["raised_amount"],
                    "giv_power": record["giv_power"],
                    "giv_power_rank": record["giv_power_rank"],
                    "givbacks_eligible": record["givbacks_eligible"],
                    "in_active_qf_round": record["in_active_qf_round"],
                    "unique_donors": record["unique_donors"],
                    "owner_wallet": record["owner_wallet"],
                    "ethereum_address": record["ethereum_address"],
                    "polygon_address": record["polygon_address"],
                    "optimism_address": record["optimism_address"],
                    "celo_address": record["celo_address"],
                    "base_address": record["base_address"],
                    "arbitrum_address": record["arbitrum_address"],
                    "gnosis_address": record["gnosis_address"],
                    "zkevm_address": record["zkevm_address"],
                    "ethereum_classic_address": record["ethereum_classic_address"],
                    "stellar_address": record["stellar_address"],
                    "solana_address": record["solana_address"],
                    "x": record["x"],
                    "facebook": record["facebook"],
                    "instagram": record["instagram"],
                    "youtube": record["youtube"],
                    "linkedin": record["linkedin"],
                    "reddit": record["reddit"],
                    "discord": record["discord"],
                    "farcaster": record["farcaster"],
                    "lens": record["lens"],
                    "website": record["website"],
                    "telegram": record["telegram"],
                    "github": record["github"],
                    "average_similarity": record[
                        "average_similarity"
                    ],  # Include average similarity
                    "related_chunks": record["related_chunks"],
                }
                for record in result
            ]


# Example Usage
query_text = "What are the effects of climate change on renewable energy?"
results = search_projects_with_chunks(query_text)
for project in results:
    print(f"Project: {project['project_title']}")
    print(
        f"Raised Amount: {project['raised_amount']}, GIV Power: {project['giv_power']}"
    )
    print(f"Average Similarity: {project['average_similarity']:.2f}")
    print("Related Chunks:")
    for chunk in project["related_chunks"]:
        print(f"  - {chunk['text']} (Similarity: {chunk['similarity']:.2f})")
    print("\n")

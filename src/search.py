from neo4j_utils import get_neo4j_driver
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

#     with get_neo4j_driver() as driver:
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
        AVG(similarity) AS average_similarity,  // Calculate average similarity
        COLLECT({chunk_id: c.id, text: c.text, similarity: similarity}) AS related_chunks
    RETURN 
        project_id,
        project_title,
        raised_amount,
        giv_power,
        average_similarity,  // Return average similarity
        related_chunks
    ORDER BY average_similarity DESC LIMIT 5 // Order by most relevant projects
    """

    # Execute the query
    with get_neo4j_driver() as driver:
        with driver.session() as session:
            result = session.run(
                query,
                parameters={
                    "queryVector": query_embedding,  # Pass query embedding
                    "similarityThreshold": similarity_threshold  # Pass threshold
                }
            )
            # Transform the results into a structured format
            return [
                {
                    "project_id": record["project_id"],
                    "project_title": record["project_title"],
                    "raised_amount": record["raised_amount"],
                    "giv_power": record["giv_power"],
                    "average_similarity": record["average_similarity"],  # Include average similarity
                    "related_chunks": record["related_chunks"],
                }
                for record in result
            ]

# Example Usage
query_text = "What are the effects of climate change on renewable energy?"
results = search_projects_with_chunks(query_text)
for project in results:
    print(f"Project: {project['project_title']}")
    print(f"Raised Amount: {project['raised_amount']}, GIV Power: {project['giv_power']}")
    print(f"Average Similarity: {project['average_similarity']:.2f}")
    print("Related Chunks:")
    for chunk in project["related_chunks"]:
        print(f"  - {chunk['text']} (Similarity: {chunk['similarity']:.2f})")
    print("\n")
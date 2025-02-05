from neo4j import GraphDatabase, RoutingControl


# define the function to connect to the neo4j database
def connect_neo4j(uri, user, password):
    """Connect to the Neo4j database."""
    return GraphDatabase.driver(uri, auth=(user, password))



# add chunks of project descriptions to the Neo4j database with their embeddings and project_id  for later similarity search
def add_chunks_to_neo4j(driver, project_id, chunks):
    """Add chunks to the Neo4j database."""
    with driver.session() as session:
        for chunk in chunks:
            session.write_transaction(create_chunk, project_id, chunk)
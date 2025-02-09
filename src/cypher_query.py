from utils.openai import generate_embedding, openai_client
from neo4j_utils import get_neo4j_driver
import json


def check_if_embedding_needed(request, schema_hint):
    print(f"request: {request}")
    """
    Ask the LLM if an embedding is needed for the query.
    """
    prompt = f"""
    Schema Information:
    {schema_hint}
    -----------------------------------
    Output Format: {request['output_format']}
    -----------------------------------

    The user has requested what came below:
    BEGINIG OF THE QUERY:
    {request['query']}
    END OF THE QUERY

    By looking at the query, I want you to infer whether any semantic search is needed or not.
    If needed, please provide a message that can be used to generate an embedding.
    For example, if the query asks "provide me 2 projects related to climate change impact on renewable energy", you must provide a message like "climate change impact on renewable energy" as the embedding message.
    But if the query is like "provide me 2 projects", then no embedding is needed.

    Now, please tell me does the query need an embedding? Respond strictly in this JSON format:
    {{
        "embedding_needed": True/False,
        "embedding_message": "message to embed"
    }}
    """
    response = openai_client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=prompt,
        max_tokens=100,
        temperature=0.3,
    )
    result = response.choices[0].text.strip()
    print(f"Embedding Check Result: {result}")
    return eval(result)  # Convert the JSON-like string to a dictionary


def generate_neo4j_query(request, schema_hint, embedding_message=None, embedding=None):
    """
    Ask the LLM to generate a Cypher query based on the request and optional embedding.
    """
    prompt = f"""
    Schema Information:
    {schema_hint}

    -------------------------------------------
    Query: "{request['query']}"
    Output Format: {request['output_format']}
    -------------------------------------------
    """

    # If embedding is provided, include it in the prompt
    if embedding:
        prompt += f"""
        I have embedding of {embedding_message}.
        If you want to use it, I will pass it as a parmeter named queryVector.
        Please for similarity use gds.similarity.cosine(c.embedding, $queryVector) as similarity if you need.
        """

    prompt += """
    Generate a Cypher query that can be executed on Neo4j to fulfill the request.
    Use gds page rank if it helps.

    Return only the Cypher query, no additional commentary.
    """

    print(f"prompt: {prompt}")

    response = openai_client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=prompt,
        max_tokens=300,
        temperature=0.3,
    )
    # Replace the similarity function
    cypher_query = response.choices[0].text.strip()
    cypher_query = cypher_query.replace(
        "gds.alpha.similarity.cosine", "gds.similarity.cosine")
    cypher_query = cypher_query.replace("gds.alpha.pageRank", "gds.pageRank")
    return cypher_query


def process_user_request(request, schema_hint):
    """
    Full workflow for processing a user's request: check embedding, generate query, and return results.
    """
    # Step 1: Check if embedding is needed
    embedding_check = check_if_embedding_needed(request, schema_hint)
    print(f"Embedding Check: {embedding_check}")

    if embedding_check["embedding_needed"]:
        # Step 2: Generate the embedding
        embedding_message = embedding_check["embedding_message"]
        # Your embedding generation function
        embedding = generate_embedding(embedding_message)
        print(
            f"Generated Embedding for '{embedding_message}': {embedding[:5]}...")
    else:
        embedding_message = None
        embedding = None

    # Step 3: Generate the Cypher query
    cypher_query = generate_neo4j_query(
        request, schema_hint, embedding_message=embedding_message, embedding=embedding)
    print(f"Generated Cypher Query: {cypher_query}")

    if embedding:
        parameters = {"queryVector": embedding, }  # Pass query embedding
    else:
        parameters = {}

    # Step 4: Execute the query
    # Your Neo4j execution function
    results = execute_cypher_query(cypher_query, parameters)
    return results


def execute_cypher_query(cypher_query, parameters):
    with get_neo4j_driver() as driver:
        with driver.session() as session:
            result = session.run(cypher_query, parameters=parameters)
            return [record.data() for record in result]


schema_hint = """
Neo4j Schema:
Node labels: Project, Chunk
Relationships: Project -> Chunk (:HAS_CHUNK)
Project properties: id, title, raised_amount, giv_power, listed
Chunk properties: id, text, embedding, created_at
Chunks are generated by spliting the description of a project.
"""
user_request = {
    'query': "I want to hear about projecs impact kids health",
    'output_format': "{project_id, project_title, raised_amount, giv_power, related_chunks: [text]}"
}
results = process_user_request(schema_hint=schema_hint, request=user_request)
print('#######################')

print(json.dumps(results, indent=4))

# from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
# from langchain_openai import ChatOpenAI
# # from langchain_community.embeddings import OpenAIEmbeddings
# from config import OPENAI_API_KEY, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


# graph = Neo4jGraph(url=NEO4J_URI, username=NEO4J_USER, password=NEO4J_PASSWORD)

# graph.refresh_schema()

# print("##################################################")
# print(f"Graph Schema: {graph.schema}")
# print("##################################################")
# chain = GraphCypherQAChain.from_llm(
#     ChatOpenAI(temperature=0, openai_api_key=OPENAI_API_KEY),
#     graph=graph,
#     verbose=True,
#     allow_dangerous_requests=True,
#     return_intermediate_steps=True,
# )


# result = chain.invoke({
#     "query": "I want to hear about projecs impact kids health",
# })
# print(result)

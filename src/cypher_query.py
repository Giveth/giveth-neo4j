import re
import json
from typing import Dict, Any, List, Optional
from utils.openai import generate_embedding, openai_client
from neo4j_utils import Neo4jImporter


class CypherQueryProcessor:
    """Class for handling Neo4j query generation using LLM assistance."""

    def __init__(self, schema_hint: str):
        """Initialize with the database schema information."""
        self.schema_hint = schema_hint

    def process_user_request(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Full workflow for processing a user's request:
        1. Checks if embedding is needed.
        2. Generates an embedding if applicable.
        3. Generates and executes the Cypher query.
        4. Returns query results.
        """

        # Check if semantic search is needed
        embedding_info = self._check_embedding_requirement(request)
        print(f"Embedding Check: {embedding_info}")

        # Generate embedding if needed
        embedding, embedding_message = None, None
        if embedding_info["embedding_needed"]:
            embedding_message = embedding_info["embedding_message"]
            embedding = generate_embedding(embedding_message)
            print(
                f"Generated embedding for: '{embedding_message}' (first 5 values: {embedding[:5]}...)"
            )

        # Generate Cypher query
        cypher_query: str = self._generate_cypher_query(
            request, embedding_message, embedding
        )
        print(f"Generated Cypher Query: {cypher_query}")

        # Execute query with parameters
        parameters = {"queryVector": embedding} if embedding else {}
        results = self._execute_query(cypher_query, parameters)

        return results

    def _check_embedding_requirement(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Determine if semantic search (embedding) is needed for the query.
        Returns a JSON with embedding_needed flag and embedding_message if needed.
        """
        print(f"Processing request: {request}")

        prompt: str = f"""
        Schema Information:
        {self.schema_hint}
        -----------------------------------
        Output Format: {request['output_format']}
        -----------------------------------

        The user has requested what came below:
        BEGINIG OF THE QUERY:
        {request['query']}
        END OF THE QUERY

        By looking at the query, determine whether semantic search is needed or not.
        Specifically, should I search for project chunks with similar meaning to the query?
        
        If semantic search is needed, provide a concise message that can be used to generate an embedding.
        For example, if the query asks "provide me 2 projects related to climate change impact on renewable energy", 
        you should provide "climate change impact on renewable energy" as the embedding message.

        If the intention is to find random projects or random donations, then no embedding is needed, 
        and you should return {{"embedding_needed": False}}.

        Respond strictly in this JSON format:
        {{
            "embedding_needed": True/False,
            "embedding_message": "message to embed" 
        }}

        Note: Only include "embedding_message" if "embedding_needed" is True.
        """

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=100,
            temperature=0.3,
        )

        result: str = response.choices[0].message.content.strip()

        # Ensure proper JSON formatting
        result_clean = (
            result.strip("`")
            .replace("json", "")
            .replace("```", "")
            .strip()
            .replace("True", "true")
            .replace("False", "false")
        )
        print(f"Cleaned result: {result_clean}")

        embedding_info = json.loads(result_clean)

        return embedding_info

    def _generate_cypher_query(
        self,
        request: Dict[str, Any],
        embedding_message: Optional[str] = None,
        embedding: Optional[List[float]] = None,
    ) -> str:
        """
        Generates a Cypher query for Neo4j based on the user's request.
        Uses embedding for semantic similarity search if available.
        """
        prompt = f"""
        Database Schema:
        {self.schema_hint}
        
        Search Request:
        - Query: "{request['query']}"
        - Output Format: {request['output_format']}
        """

        if embedding:
            prompt += f"""
        SEMANTIC SEARCH REQUIREMENTS:
        
        You must use semantic vector search with these exact requirements:
        
        1. Base your query on this pattern:
        ```
        MATCH (p:Project)-[:HAS_CHUNK]->(c:Chunk)
        WHERE c.embedding IS NOT NULL
        WITH p, c, gds.similarity.cosine(c.embedding, $queryVector) AS similarity
        WHERE similarity > {0.7}
        WITH p, c, similarity
        ORDER BY similarity DESC
        WITH p, collect({{text: c.text, similarity: similarity}}) AS chunk_matches
        WHERE size(chunk_matches) > 0
        WITH p, [match IN chunk_matches | match.text] AS chunk_texts
        WHERE p.listed = true
        ```
        
        2. $queryVector will contain the embedding for: "{embedding_message}"
        
        3. Additional instructions:
        - NO text-based matching (CONTAINS, regex) as primary filtering
        - Adjust similarity threshold (0.7-0.85) based on query specificity
        - Always include `p.listed = true`
        - For topic queries like "kids health", add secondary filters after semantic match if needed
        - Include all requested fields in RETURN
        - Add LIMIT 20 unless otherwise specified
        - Order by similarity first, then by relevance indicators
        """
        else:
            prompt += """
        DIRECT PROPERTY MATCHING REQUIREMENTS:
        
        Use direct property matches:
        
        ```
        MATCH (p:Project)
        WHERE p.listed = true
        AND <appropriate conditions based on query>
        ```
        
        - Use direct property comparisons (=, >, <, IN, etc.)
        - For text searches, use CONTAINS, STARTS WITH, or regex
        - Always include `p.listed = true`
        - Include all requested fields in RETURN
        - Add LIMIT 20 unless otherwise specified
        """

        prompt += """
        
        IMPORTANT: Return ONLY the Cypher query without ANY explanation, commentary, or markdown formatting. 
        The output should begin directly with a Cypher keyword like MATCH, WITH, or CALL.
        DO NOT include any headings, code blocks, or other text - JUST the raw Cypher query.
        """

        print(f"Generated Prompt: {prompt}")

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert Cypher query generator.",
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
            temperature=0.1,
        )

        cypher_query: str = response.choices[0].message.content.strip()

        # Remove any backticks or code block markers
        cypher_query = re.sub(r"^```cypher\s*", "", cypher_query)
        cypher_query = re.sub(r"^```\s*", "", cypher_query)
        cypher_query = re.sub(r"\s*```$", "", cypher_query)

        # Update deprecated function names
        cypher_query = self._update_deprecated_functions(cypher_query)

        return cypher_query

    def _update_deprecated_functions(self, query: str) -> str:
        """Update any deprecated Neo4j function names in the query."""
        replacements = {
            "gds.alpha.similarity.cosine": "gds.similarity.cosine",
            "gds.alpha.pageRank": "gds.pageRank",
        }

        for old, new in replacements.items():
            query = query.replace(old, new)

        return query

    def _execute_query(
        self, cypher_query: str, parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query against Neo4j.
        """
        importer = Neo4jImporter()
        with importer.get_driver() as driver:
            with driver.session() as session:
                result = session.run(cypher_query, parameters=parameters)
                return [record.data() for record in result]


schema_hint = """
    Neo4j Schema:
    Node labels: Project, Chunk, Donation
    Relationships: Project -> Chunk (:HAS_CHUNK), Project -> Donation (:HAS_DONATION)
    Project properties: id, title, raised_amount, giv_power, given_power_rank, 
    givbacks_eligible, in_active_qf_round, unique_donors, owner_wallet, 
    ethereum_address, polygon_address, optimism_address, celo_address, base_address, 
    arbitrum_address, gnosis_address, zkevm_address, ethereum_classic_address, 
    stellar_address, solana_address, x, facebook, instagram, youtube, linkedin, 
    reddit, discord, farcaster, lens, website, telegram, github, listed
    Chunk properties: id, text, embedding, created_at
    Donation properties: id, tx_hash, chain_id, project_title, created_at, 
    amount, value_usd, currency
    Chunks are generated by splitting the description of a project.
    """

# Example Usage
if __name__ == "__main__":
    processor = CypherQueryProcessor(schema_hint)

    user_request: Dict[str, Any] = {
        "query": "Get me 10 projects related to environment and their 5 donations with highest amount, use 0.8 as similarity threshold.",
        "output_format": """{
            project_id, project_title, raised_amount, giv_power, 
            giv_power_rank, related_chunks: [text] (array), donations: [amount, value_usd, currency] (array)
        }""",
    }

    results: List[Dict[str, Any]] = processor.process_user_request(user_request)
    print(json.dumps(results, indent=4))

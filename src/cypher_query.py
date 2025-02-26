import re
import json
from typing import Dict, Any, List, Optional
from utils.openai import generate_embedding, openai_client
from neo4j_utils import get_neo4j_driver


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

        response = openai_client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=100,
            temperature=0.3,
        )

        result: str = response.choices[0].text.strip()

        # Ensure proper JSON formatting
        result = re.sub(r'("embedding_needed": )false', r"\1False", result)
        result = re.sub(r'("embedding_needed": )true', r"\1True", result)

        print(f"Embedding Check Result: {result}")

        try:
            return json.loads(result)  # Safe JSON parsing
        except json.JSONDecodeError:
            return {
                "embedding_needed": False,
                "embedding_message": "",
            }  # Default fallback

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
        prompt: str = f"""
        Schema Information:
        {self.schema_hint}

        -------------------------------------------
        Query: "{request['query']}"
        Output Format: {request['output_format']}
        -------------------------------------------
        """

        if embedding:
            prompt += f"""
            I have embedding of {embedding_message}.
            If you want to use it, I will pass it as a parameter named queryVector.
            Please for similarity use gds.similarity.cosine(c.embedding, $queryVector) as similarity with threshold more than 0.8 if you need.
            """

        prompt += """
        Generate a Cypher query that can be executed on Neo4j to fulfill the request.

        Return only the Cypher query, no additional commentary.
        """

        print(f"Generated Prompt: {prompt}")

        response = openai_client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=300,
            temperature=0.3,
        )

        cypher_query: str = response.choices[0].text.strip()

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
        with get_neo4j_driver() as driver:
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
    amount, value_usd
    Chunks are generated by splitting the description of a project.
    """

# Example Usage
if __name__ == "__main__":
    processor = CypherQueryProcessor(schema_hint)

    user_request: Dict[str, Any] = {
        "query": "I want to hear about projects impacting kids' health",
        "output_format": """{
            project_id, project_title, raised_amount, giv_power, 
            giv_power_rank, givbacks_eligible, in_active_qf_round, 
            unique_donors, owner_wallet, ethereum_address, polygon_address,  
            optimism_address, celo_address, base_address, arbitrum_address, 
            gnosis_address, zkevm_address, ethereum_classic_address, 
            stellar_address, solana_address, x, facebook, instagram, youtube, 
            linkedin, reddit, discord, farcaster, lens, website, telegram, 
            github, related_chunks: [text] (array)
        }""",
    }

    results: List[Dict[str, Any]] = processor.process_user_request(user_request)
    print(json.dumps(results, indent=4))

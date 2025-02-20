# Giveth Neo4j API

This project provides a Flask web server that interacts with a Neo4j database. It includes functionality for managing API keys, logging API key usage, and processing user requests to generate and execute Cypher queries.

## 1. Requirements

- Python 3.13+
- pip

## 2. Installation

1. Install the required packages:

```bash
pip install -r requirements.txt
```

2. Set up environment variables:

```bash
cp .env.example .env
```
   
## 3. Running the Main App

The main application logic is in `src/cypher_query.py`. To run it, you can use the following command:

```bash
python src/cypher_query.py
```

## 4. Running the Web Server

To start the Flask web server, run:

```bash
python src/server.py
```

The server will be available at `http://127.0.0.1:5000/`.

## 5. Adding an API Key

To add a desired API key, modify the `src/add_api_key.py` file. Replace the placeholder values with your desired user and API key:

```python
cursor.execute('''
    INSERT INTO api_keys (user, api_key)
    VALUES (?, ?)
''', ('your_user', 'your_unique_api_key'))
```
then run:
```bash
python src/add_api_key.py
```
It will add your API key to the local sqlite DB

## 6. Endpoints to use cypher query

### Health Check:
- **GET** `/`
- Returns a simple message indicating the server is running.

### Query:
- **POST** `/query`
- **Headers:**
    - `X-API-KEY`: Your API key
- **Body:**
  ```json
  {
    "query": "your_query",
    "output_format": "your_output_format"
  }
  ```
- Returns the results of the processed query.

This is the sample curl to use cypher query for your app (replace `your_unique_api_key` with the API key you added in step 5):

```curl
curl --location '127.0.0.1:5000/query' \
--header 'X-API-KEY: your_unique_api_key' \
--header 'Content-Type: application/json' \
--data '{
    "query":"I want to hear about projects impact kids health",
    "output_format": "{project_id, project_title, raised_amount, giv_power, related_chunks: [text]}"
}'
```
from flask import Flask, request, jsonify
import sqlite3
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, '..', 'data', 'local_data.db')

from src.cypher_query import CypherQueryProcessor, schema_hint

app = Flask(__name__)

def check_api_key(api_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM api_keys WHERE api_key = ?', (api_key,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def log_api_key_usage(api_key, endpoint, request_body):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO api_key_usage (api_key, endpoint, timestamp, request_body)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
    ''', (api_key, endpoint, request_body))
    conn.commit()
    conn.close()

@app.route('/', methods=['GET'])
def health_check():
    return "App is healthy ;)"

@app.route('/query', methods=['POST'])
def query():
    api_key = request.headers.get('X-API-KEY')
    if not api_key or not check_api_key(api_key):
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.get_json()
    if not data or 'query' not in data or 'output_format' not in data:
        return jsonify({'error': 'Invalid request body'}), 400

    log_api_key_usage(api_key, '/query', str(data))

    user_request = {
        'query': data['query'],
        'output_format': data['output_format']
    }

    try:
        query_processor = CypherQueryProcessor(schema_hint)
        results = query_processor.process_user_request(user_request)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
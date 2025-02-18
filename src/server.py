from flask import Flask, request, jsonify
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.cypher_query import process_user_request, schema_hint

app = Flask(__name__)

@app.route('/', methods=['GET'])
def health_check():
    return "App is healthy ;)"

@app.route('/query', methods=['POST'])
def query():
    data = request.get_json()
    if not data or 'query' not in data or 'output_format' not in data:
        return jsonify({'error': 'Invalid request body'}), 400

    user_request = {
        'query': data['query'],
        'output_format': data['output_format']
    }

    try:
        results = process_user_request(schema_hint=schema_hint, request=user_request)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
from opensearchpy import OpenSearch
from random import choice
import time

def connect_to_opensearch():
    client = OpenSearch(
        hosts=[{'host': 'opensearch', 'port': 9200}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        http_auth=('admin', 'admin')
    )
    return client

def create_index(client):
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1
            }
        },
        'mappings': {
            'properties': {
                'title': {'type': 'text'},
                'content': {'type': 'text'},
                'content_type': {'type': 'keyword'}
            }
        }
    }
    
    if not client.indices.exists(index='documents'):
        client.indices.create(index='documents', body=index_body)

def generate_documents():
    content_types = ['article', 'news', 'report', 'tutorial']
    documents = [
        {
            'title': 'Introduction to OpenSearch',
            'content': 'OpenSearch is a community-driven, open source fork of Elasticsearch and Kibana.',
            'content_type': choice(content_types)
        },
        {
            'title': 'Python and OpenSearch',
            'content': 'Learn how to use Python to interact with OpenSearch for search and analytics.',
            'content_type': choice(content_types)
        },
        {
            'title': 'Docker Compose for Development',
            'content': 'Using Docker Compose to set up development environments with multiple services.',
            'content_type': choice(content_types)
        },
        {
            'title': 'Full-Text Search Basics',
            'content': 'Understanding the fundamentals of full-text search and how it works in OpenSearch.',
            'content_type': choice(content_types)
        },
        {
            'title': 'Data Indexing Strategies',
            'content': 'Best practices for indexing data in OpenSearch for optimal search performance.',
            'content_type': choice(content_types)
        }
    ]
    return documents

def index_documents(client, documents):
    for i, doc in enumerate(documents):
        client.index(
            index='documents',
            body=doc,
            id=i+1,
            refresh=True
        )

def search_documents(client, query, content_type=None):
    search_body = {
        'query': {
            'bool': {
                'must': {
                    'multi_match': {
                        'query': query,
                        'fields': ['title', 'content']
                    }
                }
            }
        }
    }
    
    if content_type:
        search_body['query']['bool']['filter'] = {
            'term': {'content_type': content_type}
        }
    
    response = client.search(
        index='documents',
        body=search_body
    )
    
    results = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        results.append({
            'title': source['title'],
            'snippet': source['content'][:50] + '...' if len(source['content']) > 50 else source['content']
        })
    
    return results

from flask import Flask, request, render_template

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    client = connect_to_opensearch()
    
    if request.method == 'POST':
        query = request.form.get('query', '')
        content_type = request.form.get('content_type', '')
        results = search_documents(client, query, content_type if content_type != 'all' else None)
        return render_template('index.html', results=results, query=query, selected_type=content_type)
    
    return render_template('index.html', results=None, query='', selected_type='all')

def initialize():
    client = connect_to_opensearch()

    for _ in range(10):
        try:
            if client.ping():
                break
        except:
            pass
        time.sleep(5)
    
    create_index(client)
    documents = generate_documents()
    index_documents(client, documents)

if __name__ == '__main__':
    initialize()
    app.run(host='0.0.0.0', port=5000, debug=True)
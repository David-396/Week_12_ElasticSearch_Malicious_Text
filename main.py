from loader import Loader
from elasticsearch import Elasticsearch

data_file_path=r'data\tweets_injected 3.csv'
loader = Loader(data_file_path=data_file_path)
data = loader.load()

es_port=9200
es_host='localhost'
es_uri = [f"https://{es_host}:{es_port}"]
es = Elasticsearch(es_uri)

mapping = {
    "mappings": {
        "properties": {
            "TweetID": {"type": "long"},
            "CreateDate": {"type": "date"},
            "Antisemitic": {"type": "integer"},
            "text": {"type": "text"}
        }
    }
}

index_name = 'sentiment_weapons_analyzer'
print(es.ping())
print(es.indices.exists(index=index_name))
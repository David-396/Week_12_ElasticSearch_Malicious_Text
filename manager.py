from data_loader import Loader
from es import ElasticConnector
from process import Process

data_file_path = r'data\tweets_injected 3.csv'
loader = Loader(data_file_path=data_file_path)
data = loader.load()

index_name = 'sentiment_weapons_analyzer'
mapping = {
    "properties": {
        "TweetID": {"type": "float"},
        "CreateDate": {"type": "text"},
        "Antisemitic": {"type": "integer"},
        "text": {"type": "text"},
        "sentiment": {"type": "keyword"},
        "weapons_detected": {"type": "text"}
    }
}

settings = {
    "index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }
}

es_host = 'localhost'
es_port = '9200'

client = ElasticConnector(es_host=es_host, es_port=es_port)
es_obj = client.es_client

es_obj.indices.delete(index=index_name, ignore_unavailable=True)

if not client.es_client.indices.exists(index=index_name):
    client.create_index(index_name=index_name, mapping=mapping, settings=settings)

    client.insert_docs(index_name=index_name, docs=data[:3])

    ids = client.get_ids(index_name=index_name)
    print(ids)

    all_docs = es_obj.search(index=index_name)["hits"]["hits"]
    print(all_docs)

    processor = Process(weapon_list_file_path=r'data\weapon_list.txt')
    client.update_docs(index_name=index_name, docs=all_docs, process_col='sentiment', callback=processor.identify_sentiment)
    es_obj.indices.refresh(index=index_name)

    all_docs = es_obj.search(index=index_name)["hits"]["hits"]
    print(all_docs)


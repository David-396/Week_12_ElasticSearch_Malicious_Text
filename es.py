from elasticsearch import Elasticsearch

class ElasticConnector:
    def __init__(self,es_port:str,es_host:str):
        self.es_port = es_port
        self.es_host = es_host
        self.es_uri = f"http://{es_host}:{es_port}"
        self.es_client = Elasticsearch(self.es_uri)

    # creating a new index
    def create_index(self, index_name:str, mapping:dict, settings:dict):
        if not self.es_client.ping():
            print("connection to es failed")
            return
        
        if self.es_client.indices.exists(index=index_name):
            print("index already exist")
            return

        self.es_client.indices.create(index=index_name, mappings=mapping, settings=settings)
        print(f"'{index_name}' index created.")

    # index one document
    def insert_doc(self, index_name:str, doc:dict):
        if not self.es_client.ping():
            print("connection to es failed")
            return

        self.es_client.index(index=index_name, body=doc)
        print(f"TweetID: {doc["TweetID"]} indexed.")

    # index many documents
    def insert_docs(self, index_name:str, docs:list):
        if not self.es_client.ping():
            print("connection to es failed")
            return
        
        for doc in docs:
            self.insert_doc(index_name=index_name, doc=doc)
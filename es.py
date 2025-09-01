from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

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

    # index many documents
    def insert_docs(self, index_name:str, docs:list):

        if not self.es_client.ping():
            print("connection to es failed")
            return

        for document in tqdm(docs, total=len(docs)):
            self.insert_doc(index_name=index_name, doc=document)

        self.es_client.indices.refresh(index=index_name)

    # get the documents id
    def get_ids(self, index_name:str):
        ids = []

        query = { "match_all" : {} }
        metadata = self.es_client.search(index=index_name, query=query, source=False)

        for meta in metadata['hits']['hits']:
            ids.append(meta['_id'])
        return ids


    # return a document that match to the given id
    def get_all_docs(self, index_name:str):
        docs = self.es_client.search(index=index_name)
        return docs

    # get a document by id
    def update_docs(self, index_name:str, docs:list, process_col:str, callback):

        actions = []
        for hit in docs:
            doc_id = hit["_id"]
            doc = hit["_source"]

            old_val = doc[process_col]
            new_val = callback(old_val)

            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "doc": {process_col: new_val}
            }
            actions.append(action)

        helpers.bulk(self.es_client, actions)
        self.es_client.indices.refresh(index=index_name)

        print(f'{len(docs)} documents has updated.')

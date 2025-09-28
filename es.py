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

    # return all documents
    def get_all_docs(self, index_name:str):
        docs = self.es_client.search(index=index_name)
        return docs

    # update a list of docs
    def update_docs(self, index_name:str, docs:list, process_col:str, callback=None, updated_val=None):

        actions = []
        i = 0
        for hit in docs:
            doc_id = hit["_id"]
            doc = hit["_source"]

            old_val = doc[process_col]

            new_val = updated_val if updated_val else callback(old_val)

            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "doc": {process_col: new_val[i] if isinstance(updated_val, list) else new_val}
            }
            actions.append(action)

            i += 1

        helpers.bulk(self.es_client, actions)

        self.es_client.indices.refresh(index=index_name)

        print(f'{len(docs)} documents has updated.')

    # update one doc
    def update_doc(self, index_name:str, doc:dict, process_col:str, callback=None, updated_val=None):
        doc_id = doc["_id"]
        doc = doc["_source"]

        old_val = doc[process_col]

        new_val = updated_val if updated_val else callback(old_val)

        script = {"source": f"ctx._source.{process_col} = params.{process_col}",
                  "params": {f"{process_col}": new_val} }
        response = self.es_client.update(index=index_name, id=doc_id, script=script)

        self.es_client.indices.refresh(index=index_name)

        print(f'{doc_id} document has updated.')

        return response

    # delete docs by query
    def delete_docs(self, index_name:str, query:dict):
        self.es_client.delete_by_query(index=index_name, body=query)
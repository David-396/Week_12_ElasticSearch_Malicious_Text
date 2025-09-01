from data_loader import Loader
from es import ElasticConnector
from process import Process


class Manager:
    def __init__(self,
                 data_file_path:str,
                 index_name:str,
                 index_mapping:dict,
                 index_settings:dict,
                 es_host:str,
                 es_port:str,
                 text_col:str,
                 sentiment_col:str,
                 weapons_col:str,
                 weapon_list_file_path:str,
                 delete_query:dict):

        self.index_name = index_name
        self.index_mapping = index_mapping
        self.index_settings = index_settings
        self.loader = Loader(data_file_path=data_file_path)
        self.text_col = text_col
        self.sentiment_col = sentiment_col
        self.weapons_col = weapons_col
        self.delete_query = delete_query
        self.data = self.loader.load()
        self.client = ElasticConnector(es_host=es_host, es_port=es_port)
        self.es_obj = self.client.es_client
        self.processor = Process(weapon_list_file_path=weapon_list_file_path)


    # main func
    def run(self):
        # create a new index if not exist
        if not self.client.es_client.indices.exists(index=self.index_name):
            self.client.create_index(index_name=self.index_name, mapping=self.index_mapping, settings=self.index_settings)

        # insert all the docs
        self.client.insert_docs(index_name=self.index_name, docs=self.data)

        # get the whole docs to process them
        all_docs = self.es_obj.search(index=self.index_name, size=10000)["hits"]["hits"]

        # update the docs by a function - process #1
        self.client.update_docs(index_name=self.index_name, docs=all_docs, process_col=self.sentiment_col, callback=self.processor.identify_sentiment)

        # update the docs by a function - process #2
        self.processor.identify_weapons(es_obj=self.client, index_name=self.index_name, searched_col=self.text_col, assigning_col=self.weapons_col)

        # delete docs by query
        self.client.delete_docs(index_name=self.index_name, query=self.delete_query)
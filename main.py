import os

from manager import Manager

WEAPON_LIST_FILE_PATH = os.environ.get('WEAPON_LIST_FILE_PATH', r'data\weapon_list.txt')
DATA_FILE_PATH = os.environ.get('DATA_FILE_PATH', r'data\tweets_injected 3.csv')
INDEX_NAME = os.environ.get('INDEX_NAME', 'sentiment_weapons_analyzer')
ES_HOST = os.environ.get('ES_HOST', 'localhost')
ES_PORT = os.environ.get('ES_PORT', '9200')
TEXT_COL = os.environ.get('TEXT_COL', 'text')
SENTIMENT_COL = os.environ.get('SENTIMENT_COL', 'sentiment')
WEAPONS_COL = os.environ.get('WEAPONS_COL', 'weapons_detected')
ANTISEMITIC_COL = os.environ.get('ANTISEMITIC_COL', 'Antisemitic')

index_mapping = {
    "properties": {
        "TweetID": {"type": "float"},
        "CreateDate": {"type": "text"},
        "Antisemitic": {"type": "integer"},
        "text": {"type": "text"},
        "sentiment": {"type": "keyword"},
        "weapons_detected": {"type": "text"}
    }
}

index_settings = {
    "index": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    }
}


delete_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {f"{WEAPONS_COL}": "None"}}
            ],
            "should": [
                {"match": {f"{ANTISEMITIC_COL}": 0}}
            ],
            "must_not": [
                {"term": {f"{SENTIMENT_COL}": "Negative"}}
            ]
        }
    }
}



manager = Manager(data_file_path=DATA_FILE_PATH,
                  index_name=INDEX_NAME,
                  index_mapping=index_mapping,
                  index_settings=index_settings,
                  es_host=ES_HOST,
                  es_port=ES_PORT,
                  text_col=TEXT_COL,
                  sentiment_col=SENTIMENT_COL,
                  weapons_col=WEAPONS_COL,
                  weapon_list_file_path=WEAPON_LIST_FILE_PATH,
                  delete_query=delete_query)

# manager.es_obj.indices.delete(index=INDEX_NAME, ignore_unavailable=True)

manager.run()
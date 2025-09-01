import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import es


class Process:
    def __init__(self, weapon_list_file_path:str):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.weapon_list_file_path = weapon_list_file_path
        self.weapons_list = ' '.join(self.get_weapons_black_list())

    # get the weapon black list
    def get_weapons_black_list(self):
        with open(self.weapon_list_file_path, 'r', encoding='utf-8') as f:
            weapons_black_list = {f.readline().strip()}

            for weapon in f:
                weapons_black_list.add(weapon.strip())

            return weapons_black_list

    # return the sentiment of a text
    def identify_sentiment(self, txt:str):
        sentiment_dict = self.sentiment_analyzer.polarity_scores(txt)

        if sentiment_dict['compound'] >= 0.05:
            return "Positive"
        elif sentiment_dict['compound'] <= -0.05:
            return "Negative"
        else:
            return "Neutral"
        
    # find the weapons in a text
    def identify_weapons(self, es_obj:es.ElasticConnector, index_name:str, searched_col:str, assigning_col:str):
        if not es_obj.es_client.ping():
            print('failed to connect to es.')
            return

        body = {
            "query": {
                "match": {
                    searched_col: self.weapons_list
                        }
                    },
            "highlight": {
                "fields": {
                    "text": {}
                        }
                    }
                }

        docs_found = es_obj.es_client.search(index=index_name, body=body, size=10000)['hits']['hits']

        weapons_detected_list = []
        for doc in docs_found:
            weapons_txt = doc['highlight']['text']

            weapons_detected = ''
            for weapon in weapons_txt:
                weapon_detect = ' '.join(re.findall(r"<em>(.*?)</em>", weapon))
                weapons_detected += ' ' + weapon_detect

            weapons_detected_list.append(weapons_detected)

        es_obj.update_docs(index_name=index_name, docs=docs_found, process_col=assigning_col, updated_val=weapons_detected_list)

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class Process:
    def __init__(self, weapon_list_file_path:str):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.weapon_list_file_path = weapon_list_file_path
        self.weapons_list = self.get_weapons_black_list()

    # return the sentiment of a text
    def identify_sentiment(self, txt:str):
        sentiment_dict = self.sentiment_analyzer.polarity_scores(txt)

        if sentiment_dict['compound'] >= 0.05:
            return "Positive"
        elif sentiment_dict['compound'] <= -0.05:
            return "Negative"
        else:
            return "Neutral"

    # get the weapon black list
    def get_weapons_black_list(self):
        with open(self.weapon_list_file_path, 'r', encoding='utf-8') as f:

            weapons_black_list = {f.readline().strip()}

            for weapon in f:
                weapons_black_list.add(weapon.strip())

            return weapons_black_list
        
    # find the weapons in a text
    def find_weapons(self, txt:str):
        txt = txt.split(' ')
        
        if self.weapons_black_list:
            weapons_lst = [weapon for weapon in txt if weapon in self.weapons_black_list]

            return weapons_lst
        return None
import csv

class Loader:
    def __init__(self, data_file_path:str):
        self.data_file_path=data_file_path

    # return a csv file as a dictionary
    def load(self):
        data = []

        with open(self.data_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)    
            for row in reader:
                data.append(row)
        
        return data
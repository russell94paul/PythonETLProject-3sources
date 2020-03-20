import pandas as pd
import requests # this package is used for fetching data from API
import json

class Extract:

    def __init__(self):
        # Loading our json here to use it across different class methods
        self.data_sources = json.load(open('data_config.json'))
        self.api = self.data_sources['data_sources']['api']
        self.csv_path = self.data_sources['data_sources']['csv']

    
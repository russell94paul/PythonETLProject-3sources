from DataSources import Extract
from DataLoad import MongoDB
import urllib
from pandas import as pd
import numpy as numpy


class Transform:

    def __init__(self, dataSource, dataSet)

    # Creating Extract class object here, to fetch data using its generic methods for APIs and CSV data sources
    extractObj = Extract()

    if dataSource == 'api':
        self.data = extractObj.getAPISData(dataSet)
        funcName = dataSource + dataSet

        # getattr function take in function name of class and calls it
        getattr(self, funcName)()
    elif dataSource == 'csv':
        self.data = extractObj.getCSVData(dataSet)
        funcName = dataSource + dataSet
        getattr(self, funcName) ()
    else:
        print('Unknown Data Source. Please Try Again ..')

    # Economy Data Transformation
    def apiEconomy(self):
        gdp_india = {}
        
        for record in self.data['records']:
            gdp = {}

            # Taking our yearly GDP value from records
            gdp ['GDP_in_rs_cr'] = int(record['gross_domestic_product_in_rs_cr_at_2004_05_prices'])
            gdp_india[record['financial_year']] = gdp
            gdp_india_yrs = list(gdp_india)

        for i in range(len(gdp_india_yrs)):
            if i == 0:
                pass
            else:
                key = 'GDP_Growth_' + gdp_india_yrs[i]
                # Calculating GDP growth on yearly basis
                gdp_india[gdp_india_yrs[i]][key] = round(((gdp_india[gdp_india_yrs[i]]['GDP_in_rs_cr'] -gdp_india[gdp_india_yrs[i-1]]['GDP_in_rs_cr'])/gdp_india[gdp_india_yrs[i-1]]['GDP_in_rs_cr'])*100,2)

        # Connection to Mongo DB
        mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('password'), 'host', 'GDP')

        # Insert Data into MongoDB 
        mongoDB_obj.insert_into_db(gdp_india, 'India_GDP')

    # Pollution Data Transformation
    def apiPollution(self):
        air_data = self.data['results']

        # Converting nested data into linear structure
        air_list = []

        for data in air_data:
            for measurement in data['measurements']:
                air_dict = {}
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['unit'] = measurement['unit']
                air_list.append(air_dict)

        # Convert list of dict into pandas df
        df = pd.DataFrame(air_list, columns = air_dict.keys())

        # Connection to Mongo DB
         mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('password'), 'host', 'Pollution_Data')
        # Insert Data into MongoDB
        mongoDB_obj.insert_into_db(df, 'Air_Quality_India')

    # Crypto Market Data Transformation
    def csvCryptoMarkets(self):
        assetsCode = ['BTC', 'ETH', 'XRP', 'LTC']

    # Converting open, close, high and low price of crypto currencies into GBP values since current price is in Dollars
    # if currency belong to this list ['BTC','ETH','XRP','LTC']    
    self.csv_df['open'] = self.csv_df[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
    self.csv_df['close'] = self.csv_df[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
    self.csv_df['high'] = self.csv_df[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)
    self.csv_df['low'] = self.csv_df[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.75) if x[1] in assetsCode else np.nan, axis=1)

    # Dropping rows with null values by asset column
    self.csv_df.dropna(inplace = True)

    # Saving new CSV file


           
"""
Now, transformation class’s 3 methods are as follow:
apiEconomy(): It takes economy data and calculates GDP growth on a yearly basis.
apiPollution(): this functions simply read the nested dictionary data, takes out relevant data and dump it into MongoDB.
csvCryptomarkets(): this function reads data from a CSV file and converts the cryptocurrencies price into Great Britain Pound(GBP) and dumps into another CSV.
"""



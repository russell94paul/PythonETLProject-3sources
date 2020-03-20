from pymongo import MongoClient
import pandas as pd


class MongoDB:

    # Initialize the common usable variable in below function:
    def __init__(self, user, password, host, db_name, port='27017, authSource='admin'):
        self.user = user
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.authSource = authSource
        self.uri = 'mongodb://' + self.user + ':' + self.password + '@'+ self.host + ':' + self.port + '/' + self.db_name + '?authSource=' + self.authSource

        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print('MongoDB Connection Successful. CHEERS!!')
        except Exception as e:
            print('Connection Unsuccessful!! Error!!')
            print(e)

    # Function to insert data in DB, could handle PYthon dictionary and Pandas DataFrames
    def insert_into_db(self, data, collection):
        if isinstance(data, pd.DataFrame):
            try:
                self.db[collection].insert_many(data.to_dict('records'))
                print('Data Inserted Successfully')
            except Exception as e:
                print('OOPS!! Some ERROR Occurred')
                print(e)
        else:
            try:
                self.db[collection].insert_many(data)
                print('Data Inserted Successfully')
            except Exception as e:
                print('OOPS!! Some ERROR Occurred')
                print(e)

    # Function to insert data in DB, could handle PYthon dictionary and Pandas DataFrames
    def read_from_db(self, collection):
        try:
            data = pd.DataFrame(list(self.db[collection].find()))
            print('Data Fetched Successfully')
            return data
        except Exception as e:
            print('OOPS!! Some ERROR Occurred')
            print(e)

    
    



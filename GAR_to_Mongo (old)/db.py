from pymongo import MongoClient

def get_database(region_code):
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb://localhost:27017/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['GAR'][region_code]


def write_update(region_code, arr):
        region = get_database(region_code)
        region.bulk_write(arr, ordered=False)
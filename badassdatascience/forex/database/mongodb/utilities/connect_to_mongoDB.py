from pymongo import MongoClient

#
# Connect to MongoDB
#
def connect_to_mongoDB():
    client = MongoClient()
    return client.forex

#
# get access to the candlestick documents
#
def get_candlestick_documents():
    db = connect_to_mongoDB()
    return db.candlesticks
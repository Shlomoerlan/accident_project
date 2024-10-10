from pymongo import MongoClient

client = MongoClient('mongodb://172.19.10.104:27017/')
db = client['car_accidents_db']

daily_collection = db['daily_accidents']
monthly_collection = db['monthly_accidents']
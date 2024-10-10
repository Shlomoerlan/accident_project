from database.connect import daily_collection, monthly_collection
from repository.csv_repository import insert_data_to_mongo, insert_data_to_mongo_bulk


def test_insert_data_to_mongo():
    insert_data_to_mongo_bulk()
    assert daily_collection.count_documents({}) > 0
    assert monthly_collection.count_documents({}) > 0
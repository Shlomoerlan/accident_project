import pytest
from database.connect import client, db
from repository.csv_repository import insert_data_to_mongo


@pytest.fixture(scope="function")
def mongodb_client():
   yield client
   client.close()


@pytest.fixture(scope="function")
def daily_db_test(mongodb_client):
   db_name = 'test_daily_db'
   db = mongodb_client[db_name]
   yield db
   mongodb_client.drop_database(db_name)


@pytest.fixture(scope="function")
def init_test_data(daily_db_test):
    if db['daily_collection'].count_documents({}) == 0:
       insert_data_to_mongo()

    for collection_name in db.list_collection_names():
       daily_db_test[collection_name].drop()
       daily_db_test[collection_name].insert_many(db[collection_name].find())

    yield daily_db_test

    for collection_name in daily_db_test.list_collection_names():
       daily_db_test[collection_name].drop()


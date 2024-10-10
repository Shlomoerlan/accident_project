def test_insert_data_to_mongo():
    insert_data_to_mongo('data.csv')

    assert daily_collection.count_documents({}) > 0
    assert monthly_collection.count_documents({}) > 0
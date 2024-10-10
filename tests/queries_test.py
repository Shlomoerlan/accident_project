import pytest
from database.connect import client
from service.queries_service import get_total_accidents_by_area

@pytest.fixture(scope="function")
def daily_test_collection(init_test_data):
   return init_test_data['daily_collection']

def test_get_total_accidents_by_area():
    total_accidents = get_total_accidents_by_area('123')
    assert total_accidents > 0



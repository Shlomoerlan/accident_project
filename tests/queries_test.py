import pytest
from database.connect import client
from service.queries_service import get_total_accidents_by_area, get_total_accidents_by_area_and_period

@pytest.fixture(scope="function")
def daily_test_collection(init_test_data):
   return init_test_data['daily_collection']

def test_get_total_accidents_by_area():
    total_accidents = get_total_accidents_by_area('123')
    assert total_accidents > 0

def test_get_total_accidents_by_area_and_period_day():
    area = "1654"
    date = "2023-01-01"
    period = "day"
    result = get_total_accidents_by_area_and_period(area, date, period)
    assert result > 0

def test_get_total_accidents_by_area_and_period_week():
    area = "1111"
    date = "2022-12-20"
    period = "week"
    result = get_total_accidents_by_area_and_period(area, date, period)
    assert result > 0

def test_get_total_accidents_by_area_and_period_month():
    area = "1654"
    date = "2019-07-11"
    period = "month"
    result = get_total_accidents_by_area_and_period(area, date, period)
    assert result > 0

def test_get_total_accidents_by_area_and_period_no_accidents():
    area = "1654"
    date = "2025-01-01"
    period = "month"
    result = get_total_accidents_by_area_and_period(area, date, period)
    assert result == 0

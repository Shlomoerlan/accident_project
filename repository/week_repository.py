from datetime import datetime, timedelta
from database.connect import daily_collection

def get_week_range(date):
    start = date - timedelta(days=date.weekday())
    end = start + timedelta(days=6)
    return start, end

def get_accidents_for_week(date):
    start, end = get_week_range(date)
    accidents = list(daily_collection.find({
        'CRASH_DATE': {
            '$gte': start,
            '$lte': end
        }
    }))
    return accidents

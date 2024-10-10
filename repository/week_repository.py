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


# sample_date = datetime(2024, 10, 10)
# weekly_accidents = get_accidents_for_week(sample_date)
# for accident in weekly_accidents:
#     print(accident)
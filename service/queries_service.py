from datetime import datetime, timedelta

from toolz import pipe

from database.connect import daily_collection
from repository.week_repository import get_week_range


def get_total_accidents_by_area(area):
    pipeline = [
        {"$match": {"AREA": area}},
        {"$group": {"_id": "$AREA", "total_accidents": {"$sum": "$TOTAL_ACCIDENT"}}}
    ]
    result = list(daily_collection.aggregate(pipeline))
    if result:
        return result[0]['total_accidents']
    else:
        return 0

period_dispatch = {
    'day': lambda date: (datetime(date.year, date.month, date.day), datetime(date.year, date.month, date.day) + timedelta(days=1)),
    'week': get_week_range,
    'month': lambda date: (datetime(date.year, date.month, 1),
                           datetime(date.year + 1, 1, 1) if date.month == 12 else datetime(date.year, date.month + 1, 1))
}

def get_accidents_by_period_and_area(area, date, period):
    start, end = pipe(date, period_dispatch[period])

    pipeline = [
        {"$match": {"AREA": area, "CRASH_DATE": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": "$AREA", "total_accidents": {"$sum": "$TOTAL_ACCIDENT"}}}
    ]

    result = list(daily_collection.aggregate(pipeline))

    return result[0]['total_accidents'] if result else 0
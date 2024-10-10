from datetime import datetime, timedelta

from toolz import pipe

from database.connect import daily_collection, monthly_collection
from repository.week_repository import get_week_range


def get_total_accidents_by_area(area):
    pipeline = [
        {"$match": {f"AREA.{area}": {"$exists": True}}},
        {"$group": {"_id": None, "total_accidents": {"$sum": f"$AREA.{area}"}}}
    ]

    result = list(daily_collection.aggregate(pipeline))

    if result:
        return result[0]['total_accidents']
    else:
        return 0


def calculate_period(date, period):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    periods = {
        'day': lambda d: (d, d + timedelta(days=1)),
        'week': lambda d: get_week_range(d),
        'month': lambda d: (d.replace(day=1), (d.replace(month=d.month % 12 + 1) if d.month != 12 else d.replace(year=d.year + 1, month=1)))
    }
    return periods.get(period, lambda d: (None, None))(date_obj)

def get_total_accidents_by_area_and_period(area, date, period):
    start, end = calculate_period(date, period)
    collection = daily_collection if period in ['day', 'week'] else monthly_collection
    date_field = 'CRASH_DATE' if period in ['day', 'week'] else 'CRASH_MONTH'
    pipeline = [
        {"$match": {date_field: {"$gte": start, "$lt": end}}},
        {"$project": {f"AREA.{area}": 1}},
        {"$group": {"_id": None, "total_accidents": {"$sum": f"$AREA.{area}"}}}
    ]
    result = list(collection.aggregate(pipeline))
    return pipe(result, lambda r: r[0]['total_accidents'] if r else 0)






















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
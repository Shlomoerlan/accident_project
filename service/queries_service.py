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
        'month': lambda d: (d.replace(day=1), (d.replace(month=d.month % 12 + 1, day=1) if d.month != 12 else d.replace(year=d.year + 1, month=1, day=1)))
    }
    return periods.get(period, lambda d: (None, None))(date_obj)

def get_total_accidents_by_area_and_period(area, date, period):
    start, end = calculate_period(date, period)

    if period == 'month':
        start = start.strftime('%Y-%m')
        end = end.strftime('%Y-%m')
        collection = monthly_collection
        date_field = 'CRASH_MONTH'
    else:
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        collection = daily_collection
        date_field = 'CRASH_DATE'

    pipeline = [
        {"$match": {date_field: {"$gte": start, "$lt": end}}},
        {"$project": {f"AREA.{area}": 1}},
        {"$group": {"_id": None, "total_accidents": {"$sum": f"$AREA.{area}"}}}
    ]

    result = list(collection.aggregate(pipeline))
    return pipe(result, lambda r: r[0]['total_accidents'] if r else 0)


def get_accidents_by_cause(area):
    pipeline = [
        {"$match": {f"AREA.{area}": {"$exists": True}}},  # סינון לפי אזור
        {"$unwind": "$PRIM_CONTRIBUTORY_CAUSE"},  # פתיחת הסיבות העיקריות
        {"$group": {
            "_id": "$PRIM_CONTRIBUTORY_CAUSE",  # קיבוץ לפי סיבה עיקרית
            "total_accidents": {"$sum": f"$AREA.{area}"}  # סיכום התאונות לכל סיבה
        }},
        {"$sort": {"total_accidents": -1}}  # מיון לפי סך התאונות בסדר יורד
    ]

    result = list(daily_collection.aggregate(pipeline))

    # התאמה לפורמט מצומצם
    simplified_result = [
        {"cause": entry["_id"], "total_accidents": entry["total_accidents"]}
        for entry in result
    ]

    return simplified_result







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
        {"$match": {f"AREA.{area}": {"$exists": True}}},
        {"$unwind": "$PRIM_CONTRIBUTORY_CAUSE"},
        {"$group": {
            "_id": "$PRIM_CONTRIBUTORY_CAUSE",
            "total_accidents": {"$sum": f"$AREA.{area}"}
        }},
        {"$sort": {"total_accidents": -1}}
    ]
    result = list(daily_collection.aggregate(pipeline))
    simplified_result = [
        {"cause": entry["_id"], "total_accidents": entry["total_accidents"]}
        for entry in result
    ]
    return simplified_result

def get_injury_statistics_by_area(area_code):
    pipeline = [
        { "$match": { f"AREA.{area_code}": {"$exists": True}}},
        { "$group": { "_id": None, "total_injuries": {"$sum": {
                        "$add": [
                            "$INJURIES.FATAL_INJURIES",
                            "$INJURIES.INCAPACITATING_INJURIES",
                            "$INJURIES.NON_INCAPACITATING_INJURIES"
                        ]}}, "fatal_injuries": { "$sum": "$INJURIES.FATAL_INJURIES" },
        "non_fatal_injuries": { "$sum": { "$add": ["$INJURIES.INCAPACITATING_INJURIES", "$INJURIES.NON_INCAPACITATING_INJURIES"] }},
            }
        }
    ]

    result = list(daily_collection.aggregate(pipeline))
    if result:
        data = result[0]
        return data
    return {
        "total_injuries": 0,
        "fatal_injuries": 0,
        "non_fatal_injuries": 0,
        "fatal_events": [],
        "non_fatal_events": []
    }


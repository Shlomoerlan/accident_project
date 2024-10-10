from datetime import datetime
import csv
from database.connect import daily_collection, monthly_collection
from utils.useable_functions import safe_int

csv_file_path = './data/accident_data.csv'


def insert_data_to_mongo(csv_file_path=csv_file_path):
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        daily_collection.drop()
        monthly_collection.drop()
        for row in reader:
            crash_date = datetime.strptime(row['CRASH_DATE'].split()[0], '%m/%d/%Y').date()
            crash_date_str = crash_date.strftime('%Y-%m-%d')

            daily_accident = {
                'CRASH_DATE': crash_date_str,
                'AREA': row['BEAT_OF_OCCURRENCE'],
                'TOTAL_ACCIDENT': safe_int(row['INJURIES_TOTAL']),
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE'],
                'CAUSE_CONTRIBUTORY_SEC': row['SEC_CONTRIBUTORY_CAUSE']
            }

            monthly_accident = {
                'CRASH_MONTH': crash_date.strftime('%Y-%m'),
                'AREA': row['BEAT_OF_OCCURRENCE'],
                'TOTAL_ACCIDENT': safe_int(row['INJURIES_TOTAL']),
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE'],
                'CAUSE_CONTRIBUTORY_SEC': row['SEC_CONTRIBUTORY_CAUSE']
            }

            daily_collection.update_one(
                {'CRASH_DATE': daily_accident['CRASH_DATE'], 'AREA': daily_accident['AREA']},
                {'$set': daily_accident},
                upsert=True
            )

            monthly_collection.update_one(
                {'CRASH_MONTH': monthly_accident['CRASH_MONTH'], 'AREA': monthly_accident['AREA']},
                {'$set': monthly_accident},
                upsert=True
            )

def insert_data_to_mongo_bulk(csv_file_path=csv_file_path):
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        daily_collection.drop()
        monthly_collection.drop()

        daily_bulk_ops = []
        monthly_bulk_ops = []

        for row in reader:
            crash_date = datetime.strptime(row['CRASH_DATE'].split()[0], '%m/%d/%Y').date()
            crash_date_str = crash_date.strftime('%Y-%m-%d')

            # Build the area and causes as counters
            area_counter = {row['BEAT_OF_OCCURRENCE']: 1}
            prim_contributory_cause_counter = {row['PRIM_CONTRIBUTORY_CAUSE']: 1}
            cause_contributory_sec_counter = {row['SEC_CONTRIBUTORY_CAUSE']: 1}

            daily_accident = {
                'CRASH_DATE': crash_date_str,
                'AREA': area_counter,
                'TOTAL_ACCIDENT': 1,
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': prim_contributory_cause_counter,
                'CAUSE_CONTRIBUTORY_SEC': cause_contributory_sec_counter
            }

            # Upsert logic for daily data
            daily_bulk_ops.append(UpdateOne(
                {'CRASH_DATE': crash_date_str},  # Filter by crash date
                {
                    '$inc': {'TOTAL_ACCIDENT': 1},  # Increment total accident count
                    '$setOnInsert': daily_accident,  # Insert if doesn't exist
                    '$inc': {
                        f"AREA.{row['BEAT_OF_OCCURRENCE']}": 1,  # Increment area count
                        f"PRIM_CONTRIBUTORY_CAUSE.{row['PRIM_CONTRIBUTORY_CAUSE']}": 1,  # Increment cause count
                        f"CAUSE_CONTRIBUTORY_SEC.{row['SEC_CONTRIBUTORY_CAUSE']}": 1
                        # Increment secondary cause count
                    }
                },
                upsert=True
            ))

            monthly_accident = {
                'CRASH_MONTH': crash_date.strftime('%Y-%m'),
                'AREA': {row['BEAT_OF_OCCURRENCE']: 1},
                'TOTAL_ACCIDENT': safe_int(row['INJURIES_TOTAL']),
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': {row['PRIM_CONTRIBUTORY_CAUSE']: 1},
                'CAUSE_CONTRIBUTORY_SEC': {row['SEC_CONTRIBUTORY_CAUSE']: 1}
            }

            # Upsert logic for monthly data (similar logic can be applied for month)
            monthly_bulk_ops.append(UpdateOne(
                {'CRASH_MONTH': monthly_accident['CRASH_MONTH'], 'AREA': monthly_accident['AREA']},
                {'$setOnInsert': monthly_accident},
                upsert=True
            ))

        if daily_bulk_ops:
            daily_collection.bulk_write(daily_bulk_ops)
        if monthly_bulk_ops:
            monthly_collection.bulk_write(monthly_bulk_ops)
    # daily_collection.create_index([('CRASH_DATE', 1)])
    # daily_collection.create_index([('AREA', 1)])
    # daily_collection.create_index([('PRIM_CONTRIBUTORY_CAUSE', 1)])
from datetime import datetime
import csv
from pymongo import UpdateOne
from database.connect import daily_collection, monthly_collection
from utils.useable_functions import safe_int

csv_file_path = './data/accident_data.csv'

def insert_data_to_mongo_bulk(csv_file_path=csv_file_path):
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)

        daily_bulk_ops = []
        monthly_bulk_ops = []

        for row in reader:
            crash_date = datetime.strptime(row['CRASH_DATE'].split()[0], '%m/%d/%Y').date()
            crash_date_str = crash_date.strftime('%Y-%m-%d')
            crash_month_str = crash_date.strftime('%Y-%m')

            beat_of_occurrence = row.get('BEAT_OF_OCCURRENCE', '').strip()
            prim_contributory_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', '').strip()
            sec_contributory_cause = row.get('SEC_CONTRIBUTORY_CAUSE', '').strip()

            if not beat_of_occurrence:
                continue

            area_field = f"AREA.{beat_of_occurrence}"
            prim_contributory_cause_field = f"PRIM_CONTRIBUTORY_CAUSE.{prim_contributory_cause}" if prim_contributory_cause else None
            cause_contributory_sec_field = f"CAUSE_CONTRIBUTORY_SEC.{sec_contributory_cause}" if sec_contributory_cause else None

            daily_accident = {
                'CRASH_DATE': crash_date_str,
                'TOTAL_ACCIDENT': 1,
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                }
            }

            update_operations = {
                '$inc': {
                    'TOTAL_ACCIDENT': 1,
                    area_field: 1,
                },
                '$setOnInsert': {
                    'CRASH_DATE': crash_date_str,
                    'INJURIES': daily_accident['INJURIES']
                }
            }

            if prim_contributory_cause_field:
                update_operations['$inc'][prim_contributory_cause_field] = 1
            if cause_contributory_sec_field:
                update_operations['$inc'][cause_contributory_sec_field] = 1

            daily_bulk_ops.append(UpdateOne(
                {'CRASH_DATE': crash_date_str},
                update_operations,
                upsert=True
            ))

            monthly_accident = {
                'CRASH_MONTH': crash_month_str,
                'TOTAL_ACCIDENT': 1,
                'INJURIES': {
                    'FATAL_INJURIES': safe_int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': safe_int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': safe_int(row['INJURIES_NON_INCAPACITATING'])
                }
            }

            monthly_update_operations = {
                '$inc': {
                    'TOTAL_ACCIDENT': 1,
                    area_field: 1,
                },
                '$setOnInsert': {
                    'CRASH_MONTH': crash_month_str,
                    'INJURIES': monthly_accident['INJURIES']
                }
            }
            if prim_contributory_cause_field:
                monthly_update_operations['$inc'][prim_contributory_cause_field] = 1
            if cause_contributory_sec_field:
                monthly_update_operations['$inc'][cause_contributory_sec_field] = 1

            monthly_bulk_ops.append(UpdateOne(
                {'CRASH_MONTH': crash_month_str},
                monthly_update_operations,
                upsert=True
            ))
        if daily_bulk_ops:
            daily_collection.bulk_write(daily_bulk_ops)
        if monthly_bulk_ops:
            monthly_collection.bulk_write(monthly_bulk_ops)


    # daily_collection.create_index([('CRASH_DATE', 1)])
    # daily_collection.create_index([('AREA', 1)])
    # daily_collection.create_index([('PRIM_CONTRIBUTORY_CAUSE', 1)])
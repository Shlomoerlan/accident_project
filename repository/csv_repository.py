from datetime import datetime
import csv
from database.connect import daily_collection, monthly_collection
from utils.useable_functions import safe_int

csv_file_path = './data/accident_data.csv'

def insert_data_to_mongo(csv_file_path=csv_file_path):
    daily_collection.drop()
    monthly_collection.drop()
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)

        daily_data = []
        monthly_data = []

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
            daily_data.append(daily_accident)

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
            monthly_data.append(monthly_accident)

        if daily_data:
            daily_collection.insert_many(daily_data)
        if monthly_data:
            monthly_collection.insert_many(monthly_data)

    # daily_collection.create_index([('CRASH_DATE', 1)])
    # daily_collection.create_index([('AREA', 1)])
    # daily_collection.create_index([('PRIM_CONTRIBUTORY_CAUSE', 1)])
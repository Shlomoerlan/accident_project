from datetime import datetime
import csv

from database.connect import daily_collection, monthly_collection


def insert_data_to_mongo(csv_file_path):
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)

        daily_data = []
        monthly_data = []

        for row in reader:
            crash_date = datetime.strptime(row['CRASH_DATE'], '%m/%d/%Y %H:%M:%S %p')

            # יצירת מסמכים יומיים
            daily_accident = {
                'CRASH_DATE': crash_date.date(),
                'AREA': row['BEAT_OF_OCCURRENCE'],
                'TOTAL_ACCIDENT': int(row['INJURIES_TOTAL']),
                'INJURIES': {
                    'FATAL_INJURIES': int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE'],
                'CAUSE_CONTRIBUTORY_SEC': row['SEC_CONTRIBUTORY_CAUSE']
            }
            daily_data.append(daily_accident)

            # יצירת מסמכים חודשיים
            monthly_accident = {
                'CRASH_MONTH': crash_date.strftime('%Y-%m'),
                'AREA': row['BEAT_OF_OCCURRENCE'],
                'TOTAL_ACCIDENT': int(row['INJURIES_TOTAL']),
                'INJURIES': {
                    'FATAL_INJURIES': int(row['INJURIES_FATAL']),
                    'INCAPACITATING_INJURIES': int(row['INJURIES_INCAPACITATING']),
                    'NON_INCAPACITATING_INJURIES': int(row['INJURIES_NON_INCAPACITATING'])
                },
                'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE'],
                'CAUSE_CONTRIBUTORY_SEC': row['SEC_CONTRIBUTORY_CAUSE']
            }
            monthly_data.append(monthly_accident)

        if daily_data:
            daily_collection.insert_many(daily_data)
        if monthly_data:
            monthly_collection.insert_many(monthly_data)







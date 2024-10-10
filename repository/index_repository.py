from database.connect import db, daily_collection

def create_index_to_get_total_accidents():
    daily_collection.create_index({'AREA': 1})
    executionStats = (daily_collection
    .find({'AREA': '883'})
    .hint({'AREA': 1})
    .explain()['executionStats'])
    executionStats_without_index = (daily_collection
    .find({'AREA': '883'})
    .hint({'$natural': 1})
    .explain()['executionStats'])
    return executionStats, executionStats_without_index

def create_index_get_total_accidents_by_area_and_period():
    daily_collection.create_index({'CRASH_DATE': 1})
    executionStats = (daily_collection
    .find({'CRASH_DATE': '2022-12-20'})
    .hint({'CRASH_DATE': 1})
    .explain()['executionStats'])
    executionStats_without_index = (daily_collection
    .find({'CRASH_DATE': '2022-12-20'})
    .hint({'$natural': 1})
    .explain()['executionStats'])
    return executionStats, executionStats_without_index

def create_index_get_accidents_by_cause():
    daily_collection.create_index({'PRIM_CONTRIBUTORY_CAUSE': 1})
    executionStats = (daily_collection
    .find({'PRIM_CONTRIBUTORY_CAUSE': 'FOLLOWING TOO CLOSELY'})
    .hint({'PRIM_CONTRIBUTORY_CAUSE': 1})
    .explain()['executionStats'])
    executionStats_without_index = (daily_collection
    .find({'PRIM_CONTRIBUTORY_CAUSE': 'FOLLOWING TOO CLOSELY'})
    .hint({'$natural': 1})
    .explain()['executionStats'])
    return executionStats, executionStats_without_index

def create_index_get_injury_statistics_by_area():
    daily_collection.create_index({'FATAL_INJURIES': 1})
    executionStats = (daily_collection
    .find({'FATAL_INJURIES': 3})
    .hint({'FATAL_INJURIES': 1})
    .explain()['executionStats'])
    executionStats_without_index = (daily_collection
    .find({'FATAL_INJURIES': 3})
    .hint({'$natural': 1})
    .explain()['executionStats'])
    return executionStats, executionStats_without_index

res1, res2 = create_index_get_injury_statistics_by_area()
print(res1)
print(res2)

def create_index():
    db.daily_collection.createIndex({ "AREA": 1 })
    db.daily_collection.createIndex({ "INJURIES.FATAL_INJURIES": 1 })
    db.daily_collection.createIndex({
      "INJURIES.INCAPACITATING_INJURIES": 1,
      "INJURIES.NON_INCAPACITATING_INJURIES": 1
    })

daily_collection.create_index({ 'AREA': 1 })

executionStats = (daily_collection
      .find({ 'AREA': '883' })
      .hint({ 'AREA': 1})
      .explain()['executionStats'])


executionStats_without_index = (daily_collection
      .find({ 'AREA': '883' })
      .hint({ '$natural': 1})
      .explain()['executionStats'])


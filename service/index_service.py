from database.connect import db, daily_collection


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

print(executionStats)
print(executionStats_without_index)
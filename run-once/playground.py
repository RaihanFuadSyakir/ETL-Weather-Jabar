import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
timestamp = datetime.strptime("2025-09-22", "%Y-%m-%d")  # "2025-09-22"
# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
API_KEY=os.getenv("API_KEY")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")
client : MongoClient= pymongo.MongoClient(MONGO_URI)
db = client["weather"]

master_location_cursor = db["master_location"].find({}, {"_id": 0, "id": 1})
# Convert to a list of ids
location_ids = [doc["id"] for doc in master_location_cursor]
print(f'location to be fetched : {location_ids}')
day = timestamp.strftime("%Y-%m-%d")
print(f'regex day : {day}')
filter = {
        "current.time": {"$regex": "2025-09-22"},   # matches "2025-09-24 00" to "2025-09-24 23"
        "location.id": {"$in": location_ids}     # only specific location IDs
}
projection = {"_id": 0, "current.time": 1, "location.id": 1}
cursor = db["raw_weather"].find(filter, projection)
list_result = [] 
day_locs = {}
for doc in cursor:
    print(doc)
    if doc["location"]["id"] not in day_locs:
        day_locs[doc["location"]["id"]] = []
    day_locs[doc["location"]["id"]].append(doc["current"]["time"])
print(f'current stored data : {list_result}')
print(f'hashmap stored data : {day_locs}')

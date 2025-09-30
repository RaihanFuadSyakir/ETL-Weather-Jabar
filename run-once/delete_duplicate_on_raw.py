from pymongo import MongoClient, DeleteMany
import os
from dotenv import load_dotenv
from typing import Any, Mapping, Sequence, cast
# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()
# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")

client : MongoClient = MongoClient(MONGO_URI)
db = client["weather"]   # replace with your DB name
raw_collection = db["raw_weather"]
pipeline = [
    {
        "$group": {
            "_id": {"location_id": "$location.id", "end": "$dag_times.end"},
            "ids": {"$push": "$_id"},
            "count": {"$sum": 1}
        }
    },
    {"$match": {"count": {"$gt": 1}}}
]

dupes = list(db.raw_weather.aggregate(cast(Sequence[Mapping[str, Any]], pipeline)))
print(f"Found {len(dupes)} duplicate groups")
bulk_ops = []
for d in dupes:
    ids = d["ids"]
    # keep the first one, delete the rest
    to_delete = ids[1:]
    if to_delete:
        bulk_ops.append(DeleteMany({"_id": {"$in": to_delete}}))

if bulk_ops:
    result = db.raw_weather.bulk_write(bulk_ops)
    print("Deleted duplicates:", result.deleted_count)
else:
    print("No duplicates found")

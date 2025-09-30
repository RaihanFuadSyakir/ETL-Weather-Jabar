import os
import json,gzip
from bson import json_util
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()

# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")

client : MongoClient = MongoClient(MONGO_URI)
db = client["weather"]   # replace with your DB name
daily_collection = db["daily_recap"]
weather_collection = db["weather_data"]
# assuming date
date = "2025-09-26"

# Find the document with the specified date
docs = list(daily_collection.find({"date": date}))

def json_size_bytes(obj) -> tuple[int, int]:
    json_str = json.dumps(obj, default=json_util.default, ensure_ascii=False)
    return len(json_str.encode("utf-8")), len(gzip.compress(json_str.encode("utf-8")))

if docs:
    json_size,compressed_size = json_size_bytes(docs)
    print(f"JSON size for date {date}: {json_size} bytes")
    print(f"JSON size for date {date}: {json_size/1024} KB")
    print(f"compressed JSON size for date {date}: {compressed_size/1024} KB")
else:
    print(f"No document found for date: {date}")


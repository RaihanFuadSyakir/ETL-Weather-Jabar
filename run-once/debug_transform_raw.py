from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from tqdm import tqdm
import os
from dotenv import load_dotenv
# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()
# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")

client : MongoClient = MongoClient(MONGO_URI)
db = client["weather"]   # replace with your DB name
raw_collection = db["raw_weather"]
data_collection = db["weather_data"]
data_collection.create_index(
    [("location_id", 1), ("timestamp", 1)],
    unique=True,
    name="locationid_timestamp_index"
)
def transform_raw_to_weather(docs):
    bulk_ops = []
    for d in tqdm(docs):
        c = d["current"]
        ts = None
        if d["fetch_method"] == "history":
            ts = datetime.strptime(d["dag_times"]["logical_date"], "%Y-%m-%d %H:%M:%S")
        else:
            ts = datetime.strptime(d["dag_times"]["end"], "%Y-%m-%d %H:%M:%S")
        if "id" not in d["location"] or d["location"]["id"] is None:
            tqdm.write(f"No id in location {d['_id']}")
            continue
        clean_doc = {
            "_id": ObjectId(d["_id"]),
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "date": ts.strftime("%Y-%m-%d"),
            "hour": ts.strftime("%H:00"),
            "minute": ts.strftime("%H:%M"),
            "location_id": d["location"]["id"],
            "location_name": d["location"]["name"],
            "lat": d["location"]["lat"],
            "lon": d["location"]["lon"],

            "temp_c": c["temp_c"],
            "feelslike_c": c["feelslike_c"],
            "humidity": c["humidity"],
            "wind_kph": c["wind_kph"],
            "wind_dir": c["wind_dir"],
            "wind_degree": c["wind_degree"],
            "precip_mm": c["precip_mm"],
            "is_day": c["is_day"],
            "uv": c["uv"],
            "cloud": c["cloud"],
            "condition": c["condition"]["text"]
        }

        bulk_ops.append(clean_doc)

    if bulk_ops:
        data_collection.insert_many(bulk_ops)
        tqdm.write(f"Inserted {len(bulk_ops)} cleaned records into weather_data")


# Example usage
if __name__ == "__main__":
    raw_collection = db["raw_weather"]
    data_collection = db["weather_data"]
    # 2. Fetch unprocessed docs
    existing_ids = [doc["_id"] for doc in data_collection.find({}, {"_id": 1})]
    unprocessed = list(raw_collection.find({"_id": {"$nin": existing_ids}}))

    if not unprocessed:
        print("No new raw docs to process.")
        exit(0)

    print(f"Found {len(unprocessed)} unprocessed docs")
    transform_raw_to_weather(unprocessed)

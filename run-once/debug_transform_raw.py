
from pymongo import MongoClient
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
def transform_raw_to_weather(docs):
    bulk_ops = []
    for d in tqdm(docs):
        c = d["current"]
        ts = None
        if d["fetch_method"] == "history":
            ts = datetime.strptime(c["time"], "%Y-%m-%d %H:%M")
        else:
            ts = datetime.strptime(d["dag_times"]["end"], "%Y-%m-%d %H:%M:%S")

        clean_doc = {
            "timestamp": ts,
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
        print(f"Inserted {len(bulk_ops)} cleaned records into weather_data")


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

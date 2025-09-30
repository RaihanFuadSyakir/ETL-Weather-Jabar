from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm
import os
from dotenv import load_dotenv
import statistics
# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()

# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")

client : MongoClient = MongoClient(MONGO_URI)
db = client["weather"]   # replace with your DB name
data_collection = db["weather_data"]
daily_recap_collection = db["daily_recap"]
daily_recap_collection.create_index([("date",1),("location_id",1)], unique=True,name="recap_index")
master_location = list(db["master_location"].find({}, {"_id": 0, "id": 1, "central_city": 1}))
# Convert to a list of ids
location_ids = [doc["id"] for doc in master_location]
location_names = {}
for doc in master_location:
    location_names[doc["id"]] = doc["central_city"]
def build_summary(date: str, location_id: int):
    records = list(data_collection.find({
        "date": date,
        "location_id": location_id
    }))

    if not records:
        print("No Record")
        return None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_full_recap = False
    if date < now:
        is_full_recap = True
    hourly_data : dict = {}
    for record in records:
        hour = record["hour"]
        if hour not in hourly_data:
            hourly_data[hour] = []
        hourly_data[hour].append(record)

    # ---- Hourly aggregates ----

    hourly_list = []
    # Create fixed structure: 24 hours × 6 slots (00,10,20,30,40,50)
    for hour in range(24):
        hour_doc = {}
        hour_str = f"{hour:02d}:00"
        values = hourly_data.get(hour_str, [])
        # Always fill 00,10,20,30,40,50 minutes
        minutes_data_points = []
        for m in range(0, 60, 10):
            minute_str = f"{hour:02d}:{m:02d}"
            match = next((v for v in values if v["minute"] == minute_str), None)
            if match:
                dp = {
                    "minute": minute_str,
                    "temp": match["temp_c"],
                    "humidity": match["humidity"],
                    "wind_kph": match["wind_kph"],
                    "wind_dir": match["wind_dir"],
                    "precip_mm": match["precip_mm"],
                }
            else:
                dp = {
                    "minute": minute_str,
                    "temp": None,
                    "humidity": None,
                    "wind_kph": None,
                    "wind_dir": None,
                    "precip_mm": None,
                }
            minutes_data_points.append(dp) 

        if values:
            temps = [v["temp"] for v in minutes_data_points if v["temp"] is not None]
            humidities = [v["humidity"] for v in minutes_data_points if v["humidity"] is not None] 
            winds = [v["wind_kph"] for v in minutes_data_points if v["wind_kph"] is not None] 
            wind_dir = [v["wind_dir"] for v in minutes_data_points if v["wind_dir"] is not None]
            precip_mm = [v["precip_mm"] for v in minutes_data_points if v["precip_mm"] is not None]
            hour_doc = {
                "hour": hour_str,
                "full_recap": is_full_recap,
                "temp_avg": round(statistics.mean(temps), 2),
                "temp_min": min(temps),
                "temp_max": max(temps),
                "humidity_avg": round(statistics.mean(humidities), 2),
                "wind_avg_kph": round(statistics.mean(winds), 2),
                "dominant_wind_dir": statistics.mode(wind_dir),
                "precip_mm": sum(precip_mm),
                "data_points": minutes_data_points
            }
        else:
            # no data for this hour
            hour_doc = {
                "hour": hour_str,
                "temp_avg": None,
                "temp_min": None,
                "temp_max": None,
                "humidity_avg": None,
                "wind_avg_kph": None,
                "dominant_wind_dir": None,
                "precip_mm": None,
                "data_points": []
            }
        hourly_list.append(hour_doc)
    # ---- Final summary doc ----
    doc = {
        "date" : date,
        "location_id": location_id,
        "location_name": location_names[location_id],
        "hourly": hourly_list
    }
    tqdm.write(f"Built summary for {doc['location_name']} {date}")
    current_doc = daily_recap_collection.find_one({"date": date, "location_id": location_id})
    if current_doc:
        tqdm.write(f"Updating summary for {doc['location_name']} {date}")
        daily_recap_collection.update_one({"_id": current_doc["_id"]}, {"$set": doc})
    else:
        tqdm.write(f"Inserting summary for {doc['location_name']} {date}")
        daily_recap_collection.insert_one(doc)
    return doc
# Example usage
if __name__ == "__main__":
    # using date from airflow later
    dates = ["2025-09-26"]
    for date in tqdm(dates):
        for loc in location_ids:
            build_summary(date, loc)

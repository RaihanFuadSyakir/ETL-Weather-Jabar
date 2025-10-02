
# midnight_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import statistics
import pytz
from script_config import fetch_date_start
# --- INIT ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
API_KEY=os.getenv("API_KEY")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")
client : MongoClient= pymongo.MongoClient(MONGO_URI)
db = client["weather"]
data_collection = db["weather_data"]
daily_recap_collection = db["daily_recap"]
daily_recap_collection.create_index([("date",1),("location_id",1)], unique=True,name="recap_index")
master_location = list(db["master_location"].find({}, {"_id": 0, "id": 1, "central_city": 1}))
# Convert to a list of ids
location_ids = [doc["id"] for doc in master_location]
location_names = {}
for doc in master_location:
    location_names[doc["id"]] = doc["central_city"]

# ---- Functions ----

def generate_minutes_data_points(hourly_data):
    minutes_data_points = []
    for m in range(0, 60, 10):
        minute_str = f"{m:02d}"
        match = next((v for v in hourly_data if v["minute"] == minute_str), None)
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
    return minutes_data_points

def generate_hourly_data_point(hourly_data, hour_str, is_full_recap, minutes_data_points):
    hour_doc = None
    if hourly_data:
        temps = [v["temp"] for v in minutes_data_points if v["temp"] is not None]
        humidities = [v["humidity"] for v in minutes_data_points if v["humidity"] is not None] 
        winds = [v["wind_kph"] for v in minutes_data_points if v["wind_kph"] is not None] 
        wind_dir = [v["wind_dir"] for v in minutes_data_points if v["wind_dir"] is not None]
        precip_mm = [v["precip_mm"] for v in minutes_data_points if v["precip_mm"] is not None]
        hour_doc = {
            "hour": hour_str,
            "full_recap": is_full_recap,
            "temp_avg": round(statistics.mean(temps), 2) if len(temps) > 0 else None,
            "temp_min": min(temps) if len(temps) > 0 else None,
            "temp_max": max(temps) if len(temps) > 0 else None,
            "humidity_avg": round(statistics.mean(humidities), 2) if len(humidities) > 0 else None,
            "wind_avg_kph": round(statistics.mean(winds), 2) if len(winds) > 0 else None,
            "dominant_wind_dir": statistics.mode(wind_dir)if len(wind_dir) > 0 else None,

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
    return hour_doc

def build_summary(date: str, location_id: int):
    records = list(data_collection.find({
        "date": date,
        "location_id": location_id
    }))

    if not records:
        print("No Record")
        return None
    is_full_recap = False
    hourly_data : dict = {}
    for hour in range(24):
        hour_doc : dict = {}
        hour_str = f"{hour:02d}"
        data_records = [v for v in records if v["hour"] == hour_str]
        if len(data_records) > 0:
            hourly_data[hour_str] = data_records
    
    if  "23" in hourly_data:
        is_full_recap = True

    # ---- Hourly aggregates ----
    hourly_list = []
    # Create fixed structure: 24 hours × 6 slots (00,10,20,30,40,50)
    for hour in range(24):
        hour_doc = {}
        hour_str = f"{hour:02d}"
        if hour_str not in hourly_data:
            continue
        minutes_data_points = generate_minutes_data_points(hourly_data[hour_str])
        hour_doc = generate_hourly_data_point(hourly_data[hour_str], hour_str, is_full_recap, minutes_data_points)
        hourly_list.append(hour_doc)
    # ---- Final summary doc ----
    doc = {
        "date" : date,
        "location_id": location_id,
        "location_name": location_names[location_id],
        "hourly": hourly_list
    }
    print(f"Built summary for {doc['location_name']} {date}")
    current_doc = daily_recap_collection.find_one({"date": date, "location_id": location_id})
    if current_doc:
        print(f"Updating summary for {doc['location_name']} {date}")
        daily_recap_collection.update_one({"_id": current_doc["_id"]}, {"$set": doc})
    else:
        print(f"Inserting summary for {doc['location_name']} {date}")
        daily_recap_collection.insert_one(doc)
    return doc

def process(**context):
    logical_date = context["dag_run"].logical_date.astimezone(pytz.timezone("Asia/Jakarta"))
    # using date from airflow later
    date = logical_date.strftime("%Y-%m-%d")
    for loc in location_ids:
        build_summary(date, loc)
date_start = fetch_date_start

def map_to_midnight(execution_date, **context):
    # move forward 1 day to match the transform at 00:00 of the same day
    return execution_date + timedelta(days=1)
with DAG(
    "generate_daily_recap_dag",
    schedule="0 0 * * *",  # run daily at midnight
    start_date= datetime(date_start["year"], 
                         date_start["month"],
                         date_start["day"],
                         date_start["hour"],
                         date_start["minute"],
                         date_start["second"]
    ),
    catchup=True,
    max_active_runs=2
) as dag:

    # Wait for the 23:50 → 00:00 fetch_weather run to finish
    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_last_transform",
        external_dag_id="transform_weather_dag",
        external_task_id="transform_weather",
        # execution_delta=timedelta(minutes=10),  # same logical date
        execution_date_fn=map_to_midnight,
        timeout=600,       # give up after 10 minutes if not done
        poke_interval=20,  # check every 60s
        mode="poke",       # or "reschedule" to free up workers
    )

    midnight_task = PythonOperator(
        task_id="generate_daily_recap",
        python_callable=process,
        provide_context=True,
    )

    wait_for_transform >> midnight_task

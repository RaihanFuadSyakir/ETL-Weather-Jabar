from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from pymongo import MongoClient, DeleteMany
from typing import Any, Mapping, Sequence, cast
from bson import ObjectId
from datetime import datetime, timedelta
import os
import pytz
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
    for d in docs:
        c = d["current"]
        ts = None
        if d["fetch_method"] == "history":
            ts = datetime.strptime(d["dag_times"]["logical_date"], "%Y-%m-%d %H:%M:%S")
        else:
            ts = datetime.strptime(d["dag_times"]["end"], "%Y-%m-%d %H:%M:%S")
        if "id" not in d["location"] or d["location"]["id"] is None:
            print(f"No id in location {d['_id']}")
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
        print(f"Inserted {len(bulk_ops)} cleaned records into weather_data")

def delete_duplicate_on_raw():
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

    dupes = list(raw_collection.aggregate(cast(Sequence[Mapping[str, Any]], pipeline)))
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
def process(**context):
    dag_interval_end = context["data_interval_end"].astimezone(pytz.timezone("Asia/Jakarta"))
    time_gap = timedelta(minutes=20)
    start_time = dag_interval_end - time_gap
    delete_duplicate_on_raw()
    data_collection.create_index(
        [("location_id", 1), ("timestamp", 1)],
        unique=True,
        name="locationid_timestamp_index"
    )
    # 2. Fetch unprocessed docs
    unprocessed = list(raw_collection.find(
        {
            "dag_times.end":{
                "$gte": start_time.strftime("%Y-%m-%d %H:%M:%S"), 
                "$lt": dag_interval_end.strftime("%Y-%m-%d %H:%M:%S")
            }
        }, 
        {"_id": 1}
    ))

    if not unprocessed:
        print("No new raw docs to process.")
        exit(0)

    print(f"Found {len(unprocessed)} unprocessed docs")
    transform_raw_to_weather(unprocessed)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# HARD CODED date_start
date_start = {
    "year": 2025,
    "month": 8,
    "day": 1,
    "hour": 0,
    "minute": 0,
    "second": 0
}
with DAG(
    "transform_weather",
    default_args=default_args,
    description="process data from raw_weather and save to MongoDB",
    schedule="*/10 * * * *",  # run exactly every 10 minutes
    #schedule="@daily", #for debug purpose
    start_date= datetime(date_start["year"], 
                         date_start["month"],
                         date_start["day"],
                         date_start["hour"],
                         date_start["minute"],
                         date_start["second"]
    ),
    catchup=True
) as dag:

    wait_for_fetch = ExternalTaskSensor(
        task_id="wait_for_fetch_weather",
        external_dag_id="fetch_weather_dag",
        external_task_id="fetch_weather",
        timeout=600,       # give up after 10 minutes if not done
        poke_interval=20,  # check every 60s
        mode="poke",       # or "reschedule" to free up workers
    )
    transform_data = PythonOperator(
        task_id="transform_weather",
        python_callable=process,
        provide_context=True,
    )
    wait_for_fetch >> transform_data


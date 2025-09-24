from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import requests
import pymongo
from pymongo import MongoClient
import concurrent.futures
from dotenv import load_dotenv
import os
import pytz
import itertools
from pprint import pprint
import logging

logging.getLogger("pymongo").setLevel(logging.INFO)
jakarta_tz = pytz.timezone("Asia/Jakarta")

# set max concurrent worker
max_workers = os.cpu_count() or 1
# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()

# --- MongoDB connection ---
MONGO_URI = os.getenv("MONGO_URI")
API_KEY=os.getenv("API_KEY")
if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")
client : MongoClient= pymongo.MongoClient(MONGO_URI)
db = client["weather"]
collection = db["raw_weather"]
# Example API function
def fetch_current_weather(id):
    url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q=id:{id}"
    try:
        response = requests.get(url, timeout=5)
        return response.json(),id
    except Exception as e:
        print(e)
    return None,id


def fetch_history_weather(id,timestamp,hour):
    unix_timestamp = int(timestamp.timestamp())
    url = f"https://api.weatherapi.com/v1/history.json?key={API_KEY}"
    url += f"&q=id:{id}"
    url += f"&unixdt={unix_timestamp}"
    url += f"&unixend_dt={unix_timestamp}"
    url += f"&hour={timestamp.hour}"
    print(f'url : {url}')
    try:
        response = requests.get(url, timeout=5)
        return response.json(),id,timestamp
    except Exception as e:
        print(e)
    return None,id,timestamp


def process_result_history(dag_times,result,ts,loc_id,day_locs):
    if result is None:
        return None
    if "error" in result:
        print(f"Error: {result['error'].get('message', 'Unknown error')}")
        return None
    # Add created_at timestamp (Asia/Jakarta, UTC+7)
    datas = []
    hour_datas = []
    try:
        hour_datas = result["forecast"]["forecastday"][0]["hour"]
    except Exception as e:
        print(e)
        return None
    now = datetime.now(jakarta_tz).strftime("%Y-%m-%d %H:%M:%S")
    print("timestamp : ",ts)
    for hour_data in hour_datas:
        if hour_data["time"] > now:
            break
        if loc_id in day_locs and hour_data["time"] in day_locs[loc_id]:
            continue
        new_data = {}
        new_data["created_at"] = datetime.now(jakarta_tz).strftime("%Y-%m-%d %H:%M:%S")
        new_data["dag_times"] = dag_times
        new_data["fetch_method"] = "history"
        new_data["location"] = result["location"]
        new_data["location"]["id"] = loc_id
        new_data["current"] = hour_data
        datas.append(new_data)
    return datas

def fetch_and_store_history_data(dag_times,timestamp,hour):
    master_location_cursor = db["master_location"].find({}, {"_id": 0, "id": 1})
    # Convert to a list of ids
    location_ids = [doc["id"] for doc in master_location_cursor]
    print(f'location to be fetched : {location_ids}')
    day = timestamp.strftime("%Y-%m-%d %H")
    print(f'regex day : {day}')
    filter = {
            "current.time": {"$regex": f"^{day}"},   # matches "2025-09-24 00" to "2025-09-24 23"
            "location.id": {"$in": location_ids}     # only specific location IDs
    }
    projection = {"_id": 0, "current.time": 1, "location.id": 1}
    cursor = db["raw_weather"].find(filter,projection)
    list_result = [] 
    day_locs = {}
    for doc in cursor:
        if doc["location"]["id"] not in day_locs:
            day_locs[doc["location"]["id"]] = []
        day_locs[doc["location"]["id"]].append(doc["current"]["time"])
    print(f'current stored data : {list_result}')
    print(f'hashmap stored data : {day_locs}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result,loc,ts in executor.map(fetch_history_weather, 
                                        location_ids,
                                          itertools.repeat(timestamp),
                                          itertools.repeat(hour)):
            processed = process_result_history(dag_times,result,ts,loc,day_locs)
            if processed is not None and len(processed) > 0:
                print(f'inserted : {len(processed)} at : {timestamp.strftime("%Y-%m-%d %H:%M:%S")}')
                collection.insert_many(processed)
            else:
                print("inserted : 0")

def process_result_current(dag_times,result,ts,loc_id):
    # Add created_at timestamp (Asia/Jakarta, UTC+7)
    new_data = {}
    new_data["created_at"] = datetime.now(jakarta_tz).strftime("%Y-%m-%d %H:%M:%S")
    new_data["dag_times"] = dag_times
    new_data["fetch_method"] = "current"
    new_data["location"] = result["location"]
    new_data["location"]["id"] = loc_id
    new_data["current"] = result["current"]
    # Check if result contains an error
    if "error" in result:
        print(f"Error: {result['error'].get('message', 'Unknown error')}")
        return None
    return new_data
# Concurrent fetch + save
def fetch_and_store(dag_times,timestamp):
    master_location_cursor = db["master_location"].find({}, {"_id": 0, "id": 1})
    # Convert to a list of ids
    location_ids = [doc["id"] for doc in master_location_cursor]
    print(f'location to be fetched : {location_ids}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for result,loc in executor.map(fetch_current_weather, location_ids):
            processed = process_result_current(dag_times,result,timestamp,loc)
            if processed is not None:
                collection.insert_one(processed)

def is_catchup_run(context):
    """
    Return True if this dag_run is a catchup (backfill) run.
    """
    logical_date = context["dag_run"].logical_date.astimezone(pytz.timezone("Asia/Jakarta"))
    data_interval_start = context["data_interval_start"].astimezone(pytz.timezone("Asia/Jakarta"))
    data_interval_end = context["data_interval_end"].astimezone(pytz.timezone("Asia/Jakarta"))
    print("Logical date:", logical_date)
    now = datetime.now(pytz.timezone("Asia/Jakarta"))
    print("Current date:", now)
    print("Date Interval start:", data_interval_start)
    print("Date Interval end:", data_interval_end)
    print("Run type:", context["dag_run"].run_type)  # scheduled / manual / backfill
    # if this run's logical_date is "in the past" compared to now,
    # we treat it as catchup
    status = False
    try:
        if logical_date > data_interval_end:
            status = now - logical_date > timedelta(minutes=10)
        else:
            status = now - data_interval_end > timedelta(minutes=10)
        print("status catchup:", status)
    except Exception as e:
        print("Unexpected error:", type(e).__name__, e)
        status = False
    return status


def process(**context):
    is_follow_up_run = is_catchup_run(context)
    dag_times = {}
    dag_times["start"] = context["data_interval_start"].astimezone(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
    dag_times["end"] = context["data_interval_end"].astimezone(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
    date = context["dag_run"].logical_date.astimezone(pytz.timezone("Asia/Jakarta")) 
    dag_times["logical_date"] = date.strftime("%Y-%m-%d %H:%M:%S")
    if is_follow_up_run:
        if date.minute != 0:
            print(f"no execute at {dag_times['logical_date']} it should be done hourly")
            return
        print(f"executed at {dag_times['logical_date']}")
        fetch_and_store_history_data(dag_times,date,date.hour)
    else:
        fetch_and_store(dag_times,date)

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
    "fetch_weather_dag",
    default_args=default_args,
    description="Fetch weather concurrently and save to MongoDB",
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

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=process,
        provide_context=True,
    )

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pymongo
from pymongo import MongoClient
import concurrent.futures
from dotenv import load_dotenv
import os
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
def fetch_weather(id):
    url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q=id:{id}"
    response = requests.get(url, timeout=5)
    return response.json()

def process_result(result):
    # Check if result contains an error
    if "error" in result:
        print(f"Error: {result['error'].get('message', 'Unknown error')}")
        return None
    return result
# Concurrent fetch + save
def fetch_and_store():
    master_location_cursor = db["master_location"].find({}, {"_id": 0, "id": 1})
    # Convert to a list of ids
    location_ids = [doc["id"] for doc in master_location_cursor]
    print(f'location to be fetched : {location_ids}')
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for result in executor.map(fetch_weather, location_ids):
            processed = process_result(result)
            if processed is not None:
                results.append(processed)
                collection.insert_one(processed)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetch_weather_dag",
    default_args=default_args,
    description="Fetch weather concurrently and save to MongoDB",
    schedule=timedelta(minutes=10),  # run every 10 min
    start_date=datetime(2025, 9, 22),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_and_store,
    )

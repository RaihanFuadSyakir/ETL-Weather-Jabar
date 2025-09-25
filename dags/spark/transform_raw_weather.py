from datetime import datetime, timedelta, timezone
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pytz
import itertools
from pprint import pprint
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

jakarta_tz = pytz.timezone("Asia/Jakarta")

# set max concurrent worker
max_workers = os.cpu_count() or 1
# Load environment variables from .env (optional, if you use a .env file)
load_dotenv()


# --- Functions ---

def preprocess_weather(doc):
    """
    Preprocess a raw weather document into a normalized format.
    
    Args:
        doc (dict): raw weather document from MongoDB
        fetch_method (str): "history" or "current"
        dag_times (dict): {"logical_date": datetime, "end": datetime} from Airflow context

    Returns:
        dict: new weather document
    """
    fetch_method = doc["fetch_method"]
    dag_times = doc["dag_times"]
    # Set created_at depending on fetch_method
    if fetch_method == "history":
        created_at = dag_times["logical_date"]
    else:  # fetch_method == "current"
        created_at = dag_times["end"]

    # Compute heat index (simple version using temp + humidity)
    def compute_heat_index(temp_c, humidity):
        temp_f = temp_c * 9/5 + 32
        hi_f = (
            -42.379
            + 2.04901523 * temp_f
            + 10.14333127 * humidity
            - 0.22475541 * temp_f * humidity
            - 6.83783e-3 * temp_f**2
            - 5.481717e-2 * humidity**2
            + 1.22874e-3 * temp_f**2 * humidity
            + 8.5282e-4 * temp_f * humidity**2
            - 1.99e-6 * temp_f**2 * humidity**2
        )
        return (hi_f - 32) * 5/9  # back to Celsius

    # Build new_data dict
    new_data = {
        "_id": doc["_id"],  # keep original ID
        "created_at": created_at,
        "location": doc.get("location"),
        "temp_c": doc.get("temp_c"),
        "humidity": doc.get("humidity"),
        "heat_index": compute_heat_index(doc.get("temp_c", 0), doc.get("humidity", 0)),
        "fetch_method": fetch_method,
    }

    return new_data

# --------------------------
# Main Spark Job
# --------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformRawWeather").getOrCreate()

    # 1. Connect to Mongo
    MONGO_URI = os.getenv("MONGO_URI")
    API_KEY=os.getenv("API_KEY")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not set in environment variables")
    client : MongoClient= pymongo.MongoClient(MONGO_URI)
    db = client["weather"]
    raw_collection = db["raw_weather"]
    data_collection = db["weather_data"]

    # 2. Fetch unprocessed docs
    existing_ids = [doc["_id"] for doc in data_collection.find({}, {"_id": 1})]
    unprocessed = list(raw_collection.find({"_id": {"$nin": existing_ids}}))

    if not unprocessed:
        print("No new raw docs to process.")
        exit(0)

    print(f"Found {len(unprocessed)} unprocessed docs")

    # 3. Parallelize with Spark
    rdd = spark.sparkContext.parallelize(unprocessed)

    # 4. Apply transformation
    processed_rdd = rdd.map(preprocess_weather)

    # 5. Collect results back to driver
    processed_docs = processed_rdd.collect()

    # 6. Insert into Mongo
    if processed_docs:
        data_collection.insert_many(processed_docs)
        print(f"Inserted {len(processed_docs)} docs into weather_data")

    spark.stop()

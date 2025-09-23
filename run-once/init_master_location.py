
import os
import json
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
collection = db["master_location"]

# --- Step 1: Clear collection ---
collection.delete_many({})  # removes all documents

# --- Step 2: Load JSON file ---
with open("master_data/master_location.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Ensure it's a list of documents
if isinstance(data, dict):
    data = [data]

# --- Step 3: Insert into MongoDB ---
if data:
    collection.insert_many(data)

print(f"Inserted {len(data)} documents into master_location.")

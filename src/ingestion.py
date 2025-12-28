import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# Cities
CITIES = [
    {"name": "Nairobi", "country": "KE"},
    {"name": "Lagos", "country": "NG"},
    {"name": "Accra", "country": "GH"},
    {"name": "Johannesburg", "country": "ZA"}
]

def fetch_weather(city_name):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json()

def run_ingestion():
    """Main function to fetch weather data and store in MongoDB"""
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client.skylogix
    collection = db.weather_raw
    
    operations = []
    for city in CITIES:
        raw = fetch_weather(city["name"])
        doc_id = f"{city['name']}_{raw['dt']}"  # Unique key per city + observation timestamp
        operations.append(
            UpdateOne(
                {"_id": doc_id},
                {"$set": {"raw_json": raw, "updatedAt": datetime.now(timezone.utc)}},
                upsert=True
            )
        )
    
    if operations:
        result = collection.bulk_write(operations)
        print(f"Inserted/Updated {result.upserted_count + result.modified_count} documents")
    
    client.close()
    return result if operations else None

# Only run if script is executed directly
if __name__ == "__main__":
    run_ingestion()
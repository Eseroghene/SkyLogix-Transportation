import psycopg2
from datetime import datetime, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
POSTGRES_URI = os.getenv("POSTGRES_URI")

def transform(raw_doc):
    """Transform MongoDB document to PostgreSQL format"""
    r = raw_doc['raw_json']
    main = r.get("main", {})
    wind = r.get("wind", {})
    clouds = r.get("clouds", {})
    rain = r.get("rain", {})
    snow = r.get("snow", {})
    weather = r.get("weather", [{}])[0]

    return {
        "city": r.get("name"),
        "country": r.get("sys", {}).get("country"),
        "observed_at": datetime.utcfromtimestamp(r.get("dt")),
        "lat": r.get("coord", {}).get("lat"),
        "lon": r.get("coord", {}).get("lon"),
        "temp_c": main.get("temp"),
        "feels_like_c": main.get("feels_like"),
        "pressure_hpa": main.get("pressure"),
        "humidity_pct": main.get("humidity"),
        "wind_speed_ms": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "cloud_pct": clouds.get("all"),
        "visibility_m": r.get("visibility"),
        "rain_1h_mm": rain.get("1h", 0.0),
        "snow_1h_mm": snow.get("1h", 0.0),
        "condition_main": weather.get("main"),
        "condition_description": weather.get("description")
    }

def run_transformation():
    """Main function to transform and load data to PostgreSQL"""
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    collection = client.skylogix.weather_raw
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()
    
    # Fetch new documents
    count = 0
    for doc in collection.find():
        record = transform(doc)
        cur.execute("""
            INSERT INTO weather_readings (
                city, country, observed_at, lat, lon, temp_c, feels_like_c,
                pressure_hpa, humidity_pct, wind_speed_ms, wind_deg, cloud_pct,
                visibility_m, rain_1h_mm, snow_1h_mm, condition_main, condition_description
            ) VALUES (
                %(city)s, %(country)s, %(observed_at)s, %(lat)s, %(lon)s,
                %(temp_c)s, %(feels_like_c)s, %(pressure_hpa)s, %(humidity_pct)s,
                %(wind_speed_ms)s, %(wind_deg)s, %(cloud_pct)s, %(visibility_m)s,
                %(rain_1h_mm)s, %(snow_1h_mm)s, %(condition_main)s, %(condition_description)s
            )
            ON CONFLICT (city, observed_at) DO NOTHING;
        """, record)
        count += 1
    
    conn.commit()
    print(f"Processed {count} records")
    
    cur.close()
    conn.close()
    client.close()
    
    return count

# Only run if script is executed directly
if __name__ == "__main__":
    run_transformation()
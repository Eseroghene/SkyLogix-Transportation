CREATE TABLE weather_readings (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    country VARCHAR(5),
    observed_at TIMESTAMP,
    lat NUMERIC,
    lon NUMERIC,
    temp_c NUMERIC,
    feels_like_c NUMERIC,
    pressure_hpa INTEGER,
    humidity_pct INTEGER,
    wind_speed_ms NUMERIC,
    wind_deg INTEGER,
    cloud_pct INTEGER,
    visibility_m INTEGER,
    rain_1h_mm NUMERIC,
    snow_1h_mm NUMERIC,
    condition_main VARCHAR(50),
    condition_description VARCHAR(100),
    ingested_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, observed_at)
);

CREATE INDEX idx_city_observed_at ON weather_readings(city, observed_at);
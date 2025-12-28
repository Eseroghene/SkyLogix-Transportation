-- ============================================================
-- WEATHER ANALYTICS QUERIES
-- ============================================================

-- 1. WEATHER TRENDS PER CITY
-- Average temperature, humidity, and wind speed by city over time
SELECT 
    city,
    DATE(observed_at) as date,
    AVG(temp_c) as avg_temp_c,
    MIN(temp_c) as min_temp_c,
    MAX(temp_c) as max_temp_c,
    AVG(humidity_pct) as avg_humidity_pct,
    AVG(wind_speed_ms) as avg_wind_speed_ms,
    COUNT(*) as reading_count
FROM weather_readings
WHERE observed_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, DATE(observed_at)
ORDER BY city, date DESC;


-- 2. HOURLY WEATHER TRENDS
-- Weather patterns by hour of day for each city
SELECT 
    city,
    EXTRACT(HOUR FROM observed_at) as hour_of_day,
    AVG(temp_c) as avg_temp_c,
    AVG(humidity_pct) as avg_humidity_pct,
    AVG(wind_speed_ms) as avg_wind_speed_ms,
    COUNT(*) as reading_count
FROM weather_readings
WHERE observed_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY city, EXTRACT(HOUR FROM observed_at)
ORDER BY city, hour_of_day;


-- 3. WEATHER CONDITION FREQUENCY
-- Most common weather conditions per city
SELECT 
    city,
    condition_main,
    condition_description,
    COUNT(*) as occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 2) as percentage
FROM weather_readings
WHERE observed_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, condition_main, condition_description
ORDER BY city, occurrence_count DESC;


-- ============================================================
-- EXTREME CONDITION DETECTION
-- ============================================================

-- 4. HIGH WIND ALERTS (Wind speed > 10 m/s ≈ 36 km/h)
SELECT 
    city,
    observed_at,
    temp_c,
    wind_speed_ms,
    wind_deg,
    condition_main,
    condition_description,
    CASE 
        WHEN wind_speed_ms >= 15 THEN 'Severe'
        WHEN wind_speed_ms >= 10 THEN 'High'
        ELSE 'Moderate'
    END as wind_severity
FROM weather_readings
WHERE wind_speed_ms >= 10
    AND observed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY wind_speed_ms DESC, observed_at DESC;


-- 5. HEAVY RAIN DETECTION (Rain > 5mm in 1 hour)
SELECT 
    city,
    observed_at,
    temp_c,
    rain_1h_mm,
    wind_speed_ms,
    humidity_pct,
    visibility_m,
    condition_description,
    CASE 
        WHEN rain_1h_mm >= 10 THEN 'Heavy Rain'
        WHEN rain_1h_mm >= 5 THEN 'Moderate Rain'
        WHEN rain_1h_mm > 0 THEN 'Light Rain'
    END as rain_intensity
FROM weather_readings
WHERE rain_1h_mm > 0
    AND observed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY rain_1h_mm DESC, observed_at DESC;


-- 6. EXTREME TEMPERATURE ALERTS
SELECT 
    city,
    observed_at,
    temp_c,
    feels_like_c,
    humidity_pct,
    condition_main,
    CASE 
        WHEN temp_c >= 35 THEN 'Extreme Heat'
        WHEN temp_c >= 30 THEN 'Very Hot'
        WHEN temp_c <= 5 THEN 'Very Cold'
        WHEN temp_c <= 0 THEN 'Freezing'
    END as temp_severity
FROM weather_readings
WHERE (temp_c >= 30 OR temp_c <= 5)
    AND observed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY city, observed_at DESC;


-- 7. LOW VISIBILITY CONDITIONS (< 1000m)
SELECT 
    city,
    observed_at,
    visibility_m,
    condition_main,
    condition_description,
    rain_1h_mm,
    humidity_pct,
    CASE 
        WHEN visibility_m < 200 THEN 'Severe - Dense Fog'
        WHEN visibility_m < 1000 THEN 'Poor - Fog/Mist'
        ELSE 'Reduced'
    END as visibility_level
FROM weather_readings
WHERE visibility_m < 1000
    AND observed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY visibility_m ASC, observed_at DESC;


-- 8. COMBINED EXTREME CONDITIONS
-- Multiple adverse conditions at once (high risk situations)
SELECT 
    city,
    observed_at,
    temp_c,
    wind_speed_ms,
    rain_1h_mm,
    visibility_m,
    condition_description,
    ARRAY_AGG(
        CASE 
            WHEN wind_speed_ms >= 10 THEN 'High Wind'
            WHEN rain_1h_mm >= 5 THEN 'Heavy Rain'
            WHEN visibility_m < 1000 THEN 'Low Visibility'
            WHEN temp_c >= 35 THEN 'Extreme Heat'
        END
    ) FILTER (WHERE 
        wind_speed_ms >= 10 OR 
        rain_1h_mm >= 5 OR 
        visibility_m < 1000 OR 
        temp_c >= 35
    ) as risk_factors
FROM weather_readings
WHERE (
    wind_speed_ms >= 10 OR 
    rain_1h_mm >= 5 OR 
    visibility_m < 1000 OR 
    temp_c >= 35
)
AND observed_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY city, observed_at, temp_c, wind_speed_ms, rain_1h_mm, visibility_m, condition_description
HAVING COUNT(CASE 
    WHEN wind_speed_ms >= 10 THEN 1
    WHEN rain_1h_mm >= 5 THEN 1
    WHEN visibility_m < 1000 THEN 1
    WHEN temp_c >= 35 THEN 1
END) >= 2  -- At least 2 extreme conditions
ORDER BY observed_at DESC;

-- ============================================================
-- WEATHER SUMMARY DASHBOARD
-- ============================================================

-- 9. CURRENT WEATHER SNAPSHOT (Latest reading per city)
WITH latest_readings AS (
    SELECT 
        city,
        observed_at,
        temp_c,
        feels_like_c,
        humidity_pct,
        wind_speed_ms,
        rain_1h_mm,
        condition_main,
        condition_description,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY observed_at DESC) as rn
    FROM weather_readings
)
SELECT 
    city,
    observed_at as last_updated,
    temp_c,
    feels_like_c,
    humidity_pct,
    wind_speed_ms,
    rain_1h_mm,
    condition_main,
    condition_description
FROM latest_readings
WHERE rn = 1
ORDER BY city;
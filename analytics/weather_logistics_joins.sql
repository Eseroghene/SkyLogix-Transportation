-- ============================================================
-- WEATHER-LOGISTICS DATA INTEGRATION
-- ============================================================

-- ASSUMPTION: You have a logistics table structure like:
-- 
-- trips (
--     trip_id, 
--     origin_city, 
--     destination_city, 
--     departure_time, 
--     arrival_time, 
--     actual_arrival_time,
--     driver_id, 
--     vehicle_id,
--     status
-- )
--
-- delays (
--     delay_id,
--     trip_id,
--     delay_minutes,
--     delay_reason,
--     recorded_at
-- )

-- ============================================================
-- 1. TRIPS WITH WEATHER CONDITIONS AT DEPARTURE
-- ============================================================
SELECT 
    t.trip_id,
    t.origin_city,
    t.destination_city,
    t.departure_time,
    t.arrival_time,
    w.temp_c as departure_temp,
    w.condition_main as departure_condition,
    w.rain_1h_mm as departure_rain,
    w.wind_speed_ms as departure_wind,
    w.visibility_m as departure_visibility,
    CASE 
        WHEN w.rain_1h_mm >= 5 OR w.wind_speed_ms >= 10 OR w.visibility_m < 1000 
        THEN 'Adverse Weather'
        ELSE 'Normal Weather'
    END as weather_risk_level
FROM trips t
LEFT JOIN LATERAL (
    SELECT *
    FROM weather_readings wr
    WHERE wr.city = t.origin_city
        AND wr.observed_at <= t.departure_time
    ORDER BY ABS(EXTRACT(EPOCH FROM (wr.observed_at - t.departure_time)))
    LIMIT 1
) w ON true
WHERE t.departure_time >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY t.departure_time DESC;

-- ============================================================
-- 2. DELAYED TRIPS CORRELATED WITH WEATHER
-- ============================================================
SELECT 
    t.trip_id,
    t.origin_city,
    t.destination_city,
    t.departure_time,
    t.actual_arrival_time - t.arrival_time as delay_duration,
    EXTRACT(EPOCH FROM (t.actual_arrival_time - t.arrival_time))/60 as delay_minutes,
    d.delay_reason,
    w.condition_main as weather_condition,
    w.rain_1h_mm,
    w.wind_speed_ms,
    w.visibility_m,
    CASE 
        WHEN w.rain_1h_mm >= 5 THEN true
        WHEN w.wind_speed_ms >= 10 THEN true
        WHEN w.visibility_m < 1000 THEN true
        ELSE false
    END as extreme_weather_present
FROM trips t
INNER JOIN delays d ON t.trip_id = d.trip_id
LEFT JOIN LATERAL (
    SELECT *
    FROM weather_readings wr
    WHERE wr.city = t.origin_city
        AND wr.observed_at BETWEEN t.departure_time - INTERVAL '1 hour' 
                               AND t.departure_time + INTERVAL '1 hour'
    ORDER BY ABS(EXTRACT(EPOCH FROM (wr.observed_at - t.departure_time)))
    LIMIT 1
) w ON true
WHERE t.actual_arrival_time IS NOT NULL
    AND t.actual_arrival_time > t.arrival_time  -- Only delayed trips
    AND t.departure_time >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY delay_minutes DESC;


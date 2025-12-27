# SkyLogix Transportation - Real-Time Weather Data Pipeline

A production-ready data pipeline that ingests real-time weather data from OpenWeatherMap API, stages it in MongoDB, transforms it, and loads it into PostgreSQL for analytics—all orchestrated by Apache Airflow.

## Project Overview

SkyLogix Transportation operates a fleet of 1,200+ delivery trucks across major African cities (Nairobi, Lagos, Accra, and Johannesburg). This pipeline enables data-driven operational decisions by providing real-time weather insights to optimize routing, minimize delays, and reduce safety risks.

### Business Impact
- **Proactive Route Optimization**: Adjust delivery routes based on current and forecasted weather conditions
- **Risk Mitigation**: Reduce accident rates and insurance costs through weather-aware dispatch
- **Operational Efficiency**: Minimize delays by integrating weather data with logistics operations
- **Trend Analysis**: Historical weather data enables predictive modeling and seasonal planning

## Architecture

```
OpenWeather API
      ↓
Python Ingestion Script (raw JSON)
      ↓
MongoDB (weather_raw collection)
      ↓
Apache Airflow DAG (orchestration)
      ↓
Transformation Layer (Python)
      ↓
PostgreSQL (weather_readings table)
      ↓
Analytics & Dashboards
```

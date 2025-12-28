# SkyLogix Transportation - Real-Time Weather Data Pipeline

A production-ready data pipeline that ingests real-time weather data from OpenWeatherMap API, stages it in MongoDB, transforms it, and loads it into PostgreSQL for analytics—all orchestrated by Apache Airflow.

## Project Overview

SkyLogix Transportation operates a fleet of 1,200+ delivery trucks across major African cities (Nairobi, Lagos, Accra, and Johannesburg). This pipeline enables data-driven operational decisions by providing real-time weather insights to optimize routing, minimize delays, and reduce safety risks.

### Business Impact
- **Proactive Route Optimization**: Adjust delivery routes based on current and forecasted weather conditions
- **Risk Mitigation**: Reduce accident rates and insurance costs through weather-aware dispatch
- **Operational Efficiency**: Minimize delays by integrating weather data with logistics operations
- **Trend Analysis**: Historical weather data enables predictive modeling and seasonal planning

# SkyLogix Transportation - Real-Time Weather Data Pipeline

A production-ready data pipeline that ingests real-time weather data from OpenWeatherMap API, stages it in MongoDB, transforms it, and loads it into PostgreSQL for analytics—all orchestrated by Apache Airflow.


##  Architecture

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

##  Prerequisites

- Python 3.8+
- MongoDB 4.4+
- PostgreSQL 12+
- Apache Airflow 2.5+
- OpenWeatherMap API key ([Get one free here](https://openweathermap.org/api))

##  Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/skyLogix- Transportation.git
cd skylogix-weather-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Airflow DAG (`dags/weather_pipeline_dag.py`)

Orchestrates the entire pipeline with two main tasks:

- **Task 1: `fetch_and_upsert_raw`** - Triggers ingestion script
- **Task 2: `transform_and_load_postgres`** - Triggers ETL script

**Schedule:** Every 15 minutes (configurable via `schedule_interval`)


## Project Structure
```
skylogix-weather-pipeline/
├── src/
│   ├── ingestion.py              
│   └── mongo_to_postgres.py      
├── dags/
│   └── weather_pipeline_dag.py   
├── analytics/
│   ├── weather_queries.sql       
│   ├── weather_logistics_joins.sql  
│   └── README.md                 
├── database/
│   └── schema.sql                
├── .env.example                  
├── requirements.txt              
└── README.md                     
```

### API Rate Limiting
The free tier of OpenWeatherMap allows 60 calls/minute. If you hit rate limits:
- Increase the DAG schedule interval
- Implement exponential backoff in ingestion script
- Consider upgrading to a paid API plan

##  Security Best Practices

- Never commit `.env` file or API keys to version control
- Use Airflow Connections/Variables for sensitive credentials
- Implement database user with minimal required privileges
- Enable SSL/TLS for database connections in production
- Rotate API keys regularly

## Monitoring & Maintenance

### Key Metrics to Monitor
- Pipeline execution success rate (via Airflow UI)
- Data freshness (time since last successful ingestion)
- MongoDB collection size growth
- PostgreSQL table row count and query performance
- API call success rate and latency

### Recommended Maintenance Tasks
- **Daily**: Review Airflow task logs for errors
- **Weekly**: Check database disk usage and optimize indexes
- **Monthly**: Archive old weather data (>90 days) to cold storage
- **Quarterly**: Review and optimize DAG schedule based on usage patterns


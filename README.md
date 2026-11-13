# Airflow ETL Weather API

An end-to-end Apache Airflow ETL pipeline that extracts daily weather data for Mount Olympus (Mytikas) from the Open-Meteo API, performs automated Data Quality (DQ) checks, and loads the processed data into an SQL Server Data Warehouse.

## Architecture Overview

The pipeline follows a Medallion-like architecture (Bronze/Silver) combined with a robust Dead Letter Queue (DLQ) pattern:

1. Extract: Fetches daily weather metrics (max/min temperature, precipitation, wind speed) for the Mytikas coordinates.
2. Bronze Layer (MinIO): Stores the raw JSON response in the `bronze-raw-weather` S3 bucket.
3. Transform & DQ (Pandas): 
   - Validates the JSON schema.
   - Casts data types.
   - Applies strict data quality rules (e.g., extreme outliers, negative precipitation/wind, min > max temp logic).
4. Silver Layer & Quarantine (MinIO): Splits the data and saves valid records to `silver-clean-weather` and invalid records to `rejected-weather` as CSV files.
5. Load (SQL Server): Performs an idempotent load into the `WeatherDWH` database, routing clean data to the main table and failed data to a dedicated DLQ table.

## Tech Stack

- Orchestration: Apache Airflow (Dockerized)
- Processing: Python (Pandas, Numpy, Requests)
- Data Lake / Storage: MinIO (S3-compatible)
- Data Warehouse: Microsoft SQL Server (SSMS)

## Key Features

- Automated Data Quality Checks: Prevents corrupted or physically impossible API data from entering the Data Warehouse.
- Dead Letter Queue (DLQ): Safely isolates rejected records into separate files and tables with their specific `rejection_reason` for easy debugging.
- Idempotency: Uses targeted `DELETE` statements before `INSERT` based on the Airflow logical date (`ds`), ensuring backfills or reruns never create duplicate data.

## Project Structure

```text
airflow_etl_weather_api/
├── dags/
│   └── dag_etl_weather.py     # Main Airflow DAG defining the ETL tasks
├── docker-compose.yaml        # Docker infrastructure for Airflow
├── requirements.txt           # Python dependencies (providers, pandas, etc.)
├── .gitignore                 # Excludes logs, cache, and sensitive files
└── README.md                  # Project documentation

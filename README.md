# CS2 Esports Data - Event-Driven ETL Pipeline

An automated Data Engineering pipeline orchestrated by Apache Airflow. This project monitors an S3-compatible data lake (MinIO) for new esports datasets, performs rigorous data quality validation, and loads the processed records into a Microsoft SQL Server Data Warehouse.

## Project Architecture

The pipeline is built using the Airflow TaskFlow API and consists of six primary stages:

1. **Trigger Mechanism (S3KeySensor)** 
   Acts as an event listener poking the `trigger-for-cs` MinIO bucket. The DAG remains in a reschedule state until the `player_stats.csv` file arrives, triggering the downstream tasks.

2. **Data Extraction** 
   Connects to MinIO via `S3Hook`, reads the object directly into memory using `io.BytesIO`, and loads it into a Pandas DataFrame without writing intermediate files to the local disk.

3. **Transformation and Data Quality Validation**
   * **Data Cleansing:** Strips whitespace from categorical strings.
   * **Type Casting:** Safely converts data types handling Null values (utilizing Pandas' `Int64` for nullable integers to maintain SQL Server compatibility).
   * **Validation Routing (Dead Letter Queue logic):** Evaluates 9 strict data quality rules using `numpy.select()`. Records violating rules (e.g., negative K/D ratios, negative earnings, or illogical career dates) are flagged with a specific `rejection_reason`.
   * **Dataset Splitting:** The data is bifurcated into `valid_data` and `invalid_data` dataframes.

4. **Data Lake Storage (Silver Layer)** 
   Saves the processed outputs (`clean` and `rejected` datasets) back into the `silver-cs` MinIO bucket with dynamic, execution-date-based filenames for historical tracking.

5. **Data Warehouse Loading** 
   Establishes a connection to MS SQL Server using SQLAlchemy. The task truncates the previous day's load and appends the new valid records to the `CS2_DWH.dbo.cs2_player_stats` table, and the invalid records to the `CS2_DWH.dbo.rejected_cs2_player_stats` table.

6. **Cleanup** 
   Deletes the initial source file from the landing bucket to reset the sensor and prevent duplicate trigger events on subsequent runs.

## Technology Stack

* **Orchestration:** Apache Airflow
* **Data Storage:** MinIO (S3 API compatible)
* **Data Warehouse:** Microsoft SQL Server (SSMS)
* **Data Processing:** Python (Pandas, NumPy)
* **Database Integration:** SQLAlchemy, pymssql

## Repository Structure

* `dags/dag_etl_trigger_CS.py`: The main Airflow DAG script containing the TaskFlow logic, S3 connections, and Pandas transformations.
* `sql/create_tables.sql`: The DDL statements used to create the destination schema and tables in SQL Server (Identity keys, primary keys, and data types).

-- Databricks notebook source
-- Raw trips table populated from Auto Loader
CREATE OR REFRESH STREAMING LIVE TABLE raw_trips
COMMENT "Raw Yellow Taxi data ingested incrementally from parquet files with file name and ingestion timestamp"
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
SELECT
  *, 
  substring_index(_metadata.file_path, '/', -1) AS file_name -- Capture the source file's basename
FROM cloud_files(
  "dbfs:/raw_data_files/*.parquet",         
  "parquet",
  map(
    "cloudFiles.inferColumnTypes", "true",           -- Schema inference for Parquet files
    "cloudFiles.schemaLocation",   "dbfs:/schemas/yellow_taxi_trips/",  -- Store schema location
    "cloudFiles.partitionColumns", "year,month"      -- Optional: Add partitioning for optimized reads
  )
);

-- COMMAND ----------

-- Taxi Zones table
CREATE OR REFRESH LIVE TABLE raw_taxi_zones
COMMENT "Taxi Zones"
AS
SELECT
   *
FROM  read_files('dbfs:/raw_data_files/taxi_zone_lookup.csv', format => 'csv', header => true, inferSchema => true);

-- COMMAND ----------

-- Create a live table for processed trips in the Silver layer
CREATE OR REFRESH STREAMING LIVE TABLE processed_trips
COMMENT "Cleaned and enriched trip data with pickup and dropoff zone information, joined from the Bronze layer."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
WITH filtered_raw_trips AS (
  -- Apply data quality filters to the raw trips stream
  SELECT *
  FROM STREAM(live.raw_trips)
  WHERE passenger_count > 0
    AND trip_distance > 0
    AND total_amount > 0
    AND tpep_pickup_datetime < tpep_dropoff_datetime
    AND tip_amount >= 0
    AND payment_type = 1
)
-- Join with taxi zone information and add temporal features
SELECT
  s.*,
  pu_zone.Borough AS pickup_borough,
  do_zone.Borough AS dropoff_borough,
  pu_zone.Zone AS pickup_zone,
  do_zone.Zone AS dropoff_zone,
  DATE(s.tpep_pickup_datetime) AS pickup_date,
  HOUR(s.tpep_pickup_datetime) AS pickup_hour,
  HOUR(s.tpep_dropoff_datetime) AS dropoff_hour,
  DAYOFWEEK(s.tpep_pickup_datetime) AS pickup_day_of_week
FROM filtered_raw_trips s
LEFT JOIN live.raw_taxi_zones pu_zone 
  ON s.PULocationID = pu_zone.LocationID
LEFT JOIN live.raw_taxi_zones do_zone 
  ON s.DOLocationID = do_zone.LocationID;

-- COMMAND ----------

-- Create the final aggregated gold table
CREATE OR REFRESH LIVE TABLE aggregated_data
COMMENT "Aggregated taxi trip data showing popular routes and revenue, for use in dashboards and reporting."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
SELECT 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    CONCAT(pickup_borough, ' - ', dropoff_borough) AS route,
    pickup_hour,          
    AVG(trip_distance) AS avg_trip_distance,  
    COUNT(*) AS number_trips,
    SUM(total_amount) AS total_revenue
FROM live.processed_trips
WHERE pickup_borough IS NOT NULL 
  AND dropoff_borough IS NOT NULL
  AND pickup_borough NOT IN ('N/A', 'Unknown')
  AND dropoff_borough NOT IN ('N/A', 'Unknown')
GROUP BY 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    pickup_hour;
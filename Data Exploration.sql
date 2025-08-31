-- Databricks notebook source
SELECT
   *
FROM  read_files('dbfs:/raw_data_files/taxi_zone_lookup.csv', format => 'csv', header => true, inferSchema => true);

-- COMMAND ----------

SELECT
  *, 
  substring_index(_metadata.file_path, '/', -1) AS file_name
FROM parquet.`dbfs:/raw_data_files/*.parquet`;

-- COMMAND ----------

SELECT COUNT(*) FROM parquet.`dbfs:/raw_data_files/*.parquet`;

-- COMMAND ----------

SELECT * FROM tripsdata.raw_trips;

-- COMMAND ----------

SELECT COUNT(*) FROM tripsdata.raw_trips;

-- COMMAND ----------

SELECT DISTINCT file_name FROM tripsdata.raw_trips ORDER BY file_name;

-- COMMAND ----------

SELECT * FROM tripsdata.processed_trips;

-- COMMAND ----------

SELECT COUNT(*) FROM tripsdata.processed_trips;

-- COMMAND ----------

SELECT * FROM tripsdata.aggregated_data;

-- COMMAND ----------

SELECT COUNT(*) FROM tripsdata.aggregated_data;

-- COMMAND ----------

SELECT COUNT(*) FROM FROM parquet.`dbfs:/raw_data_files/yellow_tripdata_2025-02.parquet`;

-- COMMAND ----------

SELECT 3475226 + 3577543;

-- COMMAND ----------

SELECT COUNT(*) FROM parquet.`dbfs:/raw_data_files/yellow_tripdata_2025-03.parquet`;

-- COMMAND ----------

SELECT 7052769 + 4145257;
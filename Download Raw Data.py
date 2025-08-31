# Databricks notebook source
# Download taxi_zone_lookup.csv from https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
import requests
import os
import io

# Destination in DBFS
DBFS_FOLDER = "dbfs:/raw_data_files/"
FILE_NAME = "taxi_zone_lookup.csv"
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Temporary DBFS path
DBFS_TMP = f"{DBFS_FOLDER}{FILE_NAME}"

def download_taxi_zone_lookup():
    """Download taxi_zone_lookup.csv and save it to DBFS."""
    dbfs_path = os.path.join(DBFS_FOLDER, FILE_NAME)

    # Skip if file already exists
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    try:
        dbutils.fs.ls(dbfs_path)
        print(f"✅ {FILE_NAME} already exists in DBFS, skipping...")
        return
    except Exception:
        pass

    print(f"⬇️ Downloading {FILE_NAME}...")
    response = requests.get(URL, stream=True)

    if response.status_code == 200:
        # Ensure response.content is properly decoded to a string
        decoded_content = response.content.decode('utf-8')
        # Save directly to DBFS
        dbutils.fs.put(DBFS_TMP, decoded_content, overwrite=True)
        print(f"✅ Saved {FILE_NAME} to {dbfs_path}")
    else:
        print(f"❌ Failed to download {FILE_NAME}. HTTP status {response.status_code}")

# Run
download_taxi_zone_lookup()

# COMMAND ----------

# Check if the file exists
dbutils.fs.ls("dbfs:/raw_data_files/taxi_zone_lookup.csv")

# COMMAND ----------

import requests
from pyspark.dbutils import DBUtils
import tempfile
import os
from datetime import datetime, timedelta
import time

# Destination in DBFS
DBFS_FOLDER = "dbfs:/raw_data_files/"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

dbutils = DBUtils(spark)

def download_yellow_tripdata(year: int, month: int):
    """
    Downloads a Parquet file for a given year-month if it does not already exist in DBFS.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    dbfs_path = DBFS_FOLDER + file_name
    url = BASE_URL.format(year=year, month=month)

    # 1. Skip if file already exists
    try:
        dbutils.fs.ls(dbfs_path)
        print(f"✅ {file_name} already exists in DBFS, skipping...")
        return
    except Exception:
        pass

    # 2. Create a local temporary directory under Workspace
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        local_temp_dir = os.path.join(os.path.dirname("/Workspace" + notebook_path), ".tmp_downloads")
        os.makedirs(local_temp_dir, exist_ok=True)
    except Exception as e:
        print(f"❌ Could not create a temporary directory in the workspace. Error: {e}")
        return

    temp_file_path = None
    print(f"⬇️ Downloading {file_name}...")
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        try:
            with tempfile.NamedTemporaryFile(dir=local_temp_dir, delete=False, suffix=".parquet") as temp_file:
                temp_file_path = temp_file.name
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    temp_file.write(chunk)

            # Copy to DBFS
            dbutils.fs.cp(f'file:{temp_file_path}', dbfs_path)
            print(f"✅ Saved {file_name} to {dbfs_path}")

        except Exception as e:
            print(f"❌ Failed to save file {file_name} to DBFS: {e}")
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            try:
                if not os.listdir(local_temp_dir):
                    os.rmdir(local_temp_dir)
            except:
                pass
    else:
        print(f"❌ Failed to download {file_name}. HTTP status {response.status_code}")


# -------- MAIN LOOP --------
start_date = datetime(2025, 1, 1)
today = datetime.today()

year, month = start_date.year, start_date.month

while (year < today.year) or (year == today.year and month <= today.month):
    download_yellow_tripdata(year, month)

    # Move to next month
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    # Pause 15 minutes before next iteration
    print("⏸️ Waiting 15 minutes before next download...")
    time.sleep(15 * 60)
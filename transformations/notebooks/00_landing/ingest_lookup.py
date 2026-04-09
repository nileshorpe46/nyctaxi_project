# Databricks notebook source

import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)


import urllib.request
import shutil
from modules.data_loader.file_downloader import download_file


# COMMAND ----------


try:
    # construct URL for parquet file for this month
    url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Open a connection and stream the remote file
    response = urllib.request.urlopen(url)

    # Define and create the local directory for this date's data
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"
    os.makedirs(dir_path, exist_ok=True)

    # Define the full path for downloaded file
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Download the file
    download_file(url, dir_path, local_path)

    # Save the streamed content to the local file in binary mode
    with open(local_path, "wb") as f:
        shutil.copyfileobj(response, f) # Copy data from response to file

    dbutils.jobs.taskValues.set(key='continue_downstream', value='yes')
    print('File successfully uploaded')

except Exception as e:
    dbutils.jobs.taskValues.set(key='continue_downstream', value='no')
    print(f'File download failed: {str(e)}')


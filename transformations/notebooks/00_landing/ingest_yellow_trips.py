# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime
from datetime import date,datetime,timezone
from dateutil.relativedelta import relativedelta


# Obtain the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime('%Y-%m')

# Define the local directory for this date's data
dir_path = f'/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}'

# Define the full path for the downloaded file
local_path = f'{dir_path}/yellow_tripdata_{formatted_date}.parquet'


try:
    # Check if file already exists
    dbutils.fs.ls(local_path)

    # if the file already exist then set continue_downstream to NO
    dbutils.jobs.taskValues.set(key='continue_downstream', value='no')
    print(f'file already downloaded aborting downstream tasks...')

except:
    try:
        # Construct URL for parquet file corresponding to this month
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet'

        # Open a connection and stream the remote file
        response = urllib.request.urlopen(url)

        # Create a local directory for this date's data
        os.makedirs(dir_path, exist_ok=True)

        # Save the streamed content to local file in binary mode
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f) # copy data from response to file

        # Set continue_downstream to YES
        dbutils.jobs.taskValues.set(key='continue_downstream', value='yes')
        print(f'file successfully uploaded in current run')

    except Exception as e:
        # Set continue downstream to NO
        dbutils.jobs.taskValues.set(key='continue_downstream', value='no')
        print(f'file download failed: {str(e)}')


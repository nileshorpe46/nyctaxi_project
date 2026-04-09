# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table('nyctaxi.02_silver.yellow_trips_enriched')

# COMMAND ----------

df.groupBy('vendor').agg(
    round(sum('total_amount'),1).alias('total_revenue')
).orderBy('total_revenue', ascending=False).select('vendor').limit(1).display()

# COMMAND ----------

df.groupBy('pu_borough').agg(
    count('*').alias('total_trips')
).orderBy('total_trips', ascending=False).select('pu_borough').limit(1).display()

# COMMAND ----------

df.groupBy('pu_borough','do_borough').agg(
    count('*').alias('total_trips')
).orderBy('total_trips', ascending=False).select('pu_borough','do_borough').limit(1).display()

# COMMAND ----------


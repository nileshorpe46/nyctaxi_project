# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

dt = DeltaTable.forName(spark, 'workspace.default.country_regions')

# COMMAND ----------

dt2 = DeltaTable.forPath(spark, '/Volumes/workspace/default/datasets/countries_dataset/delta_data/countries_population/')

# COMMAND ----------

df = spark.read.table('workspace.default.country_regions')
df.display()

# COMMAND ----------

dt.delete("name = 'America'")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dt.update(
    condition="name = 'Asia'",
    set = {
        'id': lit(100),
        'name': upper('name')
    }
)

# COMMAND ----------


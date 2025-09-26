# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferschema',True)\
        .load('abfss://bronze@olympicsdatalake7.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('BMI',col('weight')/((col('height')/100)*(col('height')/100)))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumnRenamed('team','country')

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('country').agg(count('medal').alias('total_medals')).sort('total_medals',ascending=False).limit(10).display()

# COMMAND ----------

df.groupBy('sport').agg(countDistinct('event').alias('total_events')).sort('total_events',ascending=False).display()

# COMMAND ----------

df = df.sort(['Day','id'],ascending=[1,1])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path','abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data')\
        .save()

# COMMAND ----------


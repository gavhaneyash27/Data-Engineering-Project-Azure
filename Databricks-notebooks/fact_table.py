# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

df_silver = spark.sql('select * from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`')


# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_date = spark.sql('select * from olympic_catalog.gold.dim_date')
df_athlete = spark.sql('select * from olympic_catalog.gold.dim_athlete')
df_event = spark.sql('select * from olympic_catalog.gold.dim_event')
df_country = spark.sql('select * from olympic_catalog.gold.dim_country')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing keys to fact table

# COMMAND ----------

df_fact = df_silver.join(df_athlete,df_silver['id'] == df_athlete['id'],'left')\
                   .join(df_date,df_silver['Date_ID'] == df_date['Date_ID'],'left')\
                   .join(df_event,df_silver['event'] == df_event['event'],'left')\
                   .join(df_country,df_silver['country'] == df_country['country'],'left')\
                   .select(df_silver['medal'],df_athlete['dim_athlete_key'],df_date['dim_date_key'],df_event['dim_event_key'],df_country['dim_country_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('olympic_catalog.gold.fact_tbl'):
    delta_tbl = DeltaTable.forPath(spark,'abfss://gold@olympicsdatalake7.dfs.core.windows.net/fact_tbl')

    delta_tbl.alias('trg').merge(df_fact.alias('src'),'trg.dim_athlete_key = src.dim_athlete_key AND trg.dim_date_key = src.dim_date_key AND trg.dim_event_key = src.dim_event_key AND trg.dim_country_key = src.dim_country_key')\
                                 .whenMatchedUpdateAll()\
                                 .whenNotMatchedInsertAll()\
                                 .execute()
    


else:
    df_fact.write.format('delta')\
                  .mode('overwrite')\
                  .option('path','abfss://gold@olympicsdatalake7.dfs.core.windows.net/fact_tbl')\
                  .saveAsTable('olympic_catalog.gold.fact_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olympic_catalog.gold.fact_tbl
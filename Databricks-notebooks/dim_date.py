# Databricks notebook source
# MAGIC %md
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display transformed data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating dim_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### create source df for fetching required columns

# COMMAND ----------

df_src = spark.sql('''
                   select distinct(Date_ID) as Date_ID, Day, Month, year
                   from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date sink - Initial and Incremental run

# COMMAND ----------

if  spark.catalog.tableExists('olympic_catalog.gold.dim_date'):
  df_sink = spark.sql('''
                      select dim_date_key, Date_ID, Day, Month, year
                      from olympic_catalog.gold.dim_date
                      ''')
else:
  df_sink = spark.sql('''
                      select 1 as dim_date_key, Date_ID, Day, Month, year
                      from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`
                      where 1 = 0
                      ''')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import*

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new and old data

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['Date_ID'] == df_sink['Date_ID'],'left')\
                  .select(df_src['Date_ID'],df_src['Day'],df_src['Month'],df_src['year'],df_sink['dim_date_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_date_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_key').isNull())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch the max surrogate key

# COMMAND ----------

if (incremental_flag == '0'):
  max_value = 0
else:
  max_value_df = spark.sql('''
                           select max(dim_date_key) 
                           from olympic_catalog.gold.dim_date
                           ''')
  max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add max surrogate key to our surrogate key column

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_key', max_value + monotonically_increasing_id()+1)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final DF = old df + new df

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)    

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('olympic_catalog.gold.dim_date'):
  delta_tbl = DeltaTable.forPath(spark,'abfss://gold@olympicsdatalake7.dfs.core.windows.net/dim_date')

  delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.Date_ID = src.Date_ID')\
                        .whenMatchedUpdateAll()\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:
  df_final.write.format('delta')\
                .mode('overwrite')\
                .option('path','abfss://gold@olympicsdatalake7.dfs.core.windows.net/dim_date')\
                .saveAsTable('olympic_catalog.gold.dim_date')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olympic_catalog.gold.dim_date
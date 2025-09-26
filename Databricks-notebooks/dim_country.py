# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create incremental flag

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')  
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query data for reference

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create source df

# COMMAND ----------

df_src = spark.sql('''
                   select distinct(country) as country, noc
                   from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`
                   ''')



# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create sink df - For initial and incremental run

# COMMAND ----------

if spark.catalog.tableExists('olympic_catalog.gold.dim_country'):
    df_sink = spark.sql('''
                        select dim_country_key, country, noc
                        from olympic_catalog.gold.dim_country
                        ''')
else:
    df_sink = spark.sql('''
                        select 1 as dim_country_key, country, noc
                        from parquet.`abfss://silver@olympicsdatalake7.dfs.core.windows.net/transformed_data`
                        where 1 = 0
                        ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter old and new data

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['country'] == df_sink['country'],'left').select(df_src['country'],df_src['noc'],\
                   df_sink['dim_country_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_country_key'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter['dim_country_key'].isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch max surrogate key

# COMMAND ----------

if incremental_flag == '0':
    max_value = 0

else:
    max_value_df = spark.sql('''
                             select max(dim_country_key) from olympic_catalog.gold.dim_country
                             ''')
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the max surrogate key in surrogate key column

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_country_key',max_value + monotonically_increasing_id()+1)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final_df

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('olympic_catalog.gold.dim_country'):
    delta_tbl = DeltaTable.forPath(spark,'abfss://gold@olympicsdatalake7.dfs.core.windows.net/dim_country')

    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.country = src.country')\
                          .whenMatchedUpdateAll()\
                          .whenNotMatchedInsertAll()\
                          .execute()

else:
    df_final.write.format('delta')\
                  .mode('overwrite')\
                  .option('path','abfss://gold@olympicsdatalake7.dfs.core.windows.net/dim_country')\
                  .saveAsTable('olympic_catalog.gold.dim_country')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olympic_catalog.gold.dim_country
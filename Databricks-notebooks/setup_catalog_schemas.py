# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog olympic_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema olympic_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema olympic_catalog.gold;

# COMMAND ----------


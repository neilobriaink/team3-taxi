# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Project Configuration
# MAGIC Shared constants and setup for the Team 3 NYC Taxi Lakehouse project.
# MAGIC
# MAGIC **Usage**: Run this notebook from other notebooks using `%run ./00_config`

# COMMAND ----------

# ---- Catalog & Schema ----
CATALOG = "students_data"
SCHEMA = "team3_taxi"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# ---- Source Data ----
SOURCE_TABLE = "data_academy_resources.nyc_taxi.yellow_tripdata"
# Fallback if primary source doesn't exist
SOURCE_TABLE_FALLBACK = "samples.nyc_tlc.trips"

# ---- Bronze ----
BRONZE_TRIPS = f"{FULL_SCHEMA}.bronze_trips"

# ---- Silver ----
SILVER_TRIPS = f"{FULL_SCHEMA}.silver_trips"

# ---- Gold ----
GOLD_TRIPS = f"{FULL_SCHEMA}.gold_trips"
GOLD_DAILY_METRICS = f"{FULL_SCHEMA}.gold_daily_metrics"
GOLD_HOURLY_METRICS = f"{FULL_SCHEMA}.gold_hourly_metrics"

# ---- ML ----
ML_EXPERIMENT_NAME = "/Shared/team3_taxi_fare_prediction"

# ---- RAG ----
RAG_DOCUMENTS = f"{FULL_SCHEMA}.rag_documents"
RAG_EMBEDDINGS = f"{FULL_SCHEMA}.rag_embeddings"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✓ Using: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Source Data Access

# COMMAND ----------

ACTIVE_SOURCE = None

try:
    count = spark.table(SOURCE_TABLE).count()
    print(f"✓ Source table: {SOURCE_TABLE} ({count:,} rows)")
    ACTIVE_SOURCE = SOURCE_TABLE
except Exception as e:
    print(f"✗ Primary source unavailable: {e}")
    try:
        count = spark.table(SOURCE_TABLE_FALLBACK).count()
        print(f"✓ Using fallback: {SOURCE_TABLE_FALLBACK} ({count:,} rows)")
        ACTIVE_SOURCE = SOURCE_TABLE_FALLBACK
    except Exception as e2:
        print(f"✗ Fallback also unavailable: {e2}")
        print("ERROR: No source data found. Please check table access.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Source Schema

# COMMAND ----------

if ACTIVE_SOURCE:
    print(f"Source: {ACTIVE_SOURCE}")
    spark.table(ACTIVE_SOURCE).printSchema()
    display(spark.table(ACTIVE_SOURCE).limit(5))

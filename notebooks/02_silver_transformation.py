# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC **Purpose**: Clean, validate, and enrich taxi trip data for the Silver layer.
# MAGIC
# MAGIC **Transformations applied**:
# MAGIC - Remove records with null critical fields
# MAGIC - Filter invalid/implausible values
# MAGIC - Add derived columns (duration, speed, time features)
# MAGIC - Apply quality filters with documented thresholds
# MAGIC
# MAGIC | Input | Output |
# MAGIC |-------|--------|
# MAGIC | `students_data.team3_taxi.bronze_trips` | `students_data.team3_taxi.silver_trips` |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

df_bronze = spark.table(BRONZE_TRIPS)
bronze_count = df_bronze.count()
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Remove Null Critical Fields

# COMMAND ----------

df_clean = df_bronze.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
])

after_nulls = df_clean.count()
print(f"Dropped {bronze_count - after_nulls:,} rows with null critical fields")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Derived Columns

# COMMAND ----------

df_enriched = (
    df_clean
    # Trip duration in minutes
    .withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    # Time-based features
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin([1, 7]))
)

# Average speed (mph)
df_enriched = df_enriched.withColumn(
    "avg_speed_mph",
    F.when(
        F.col("trip_duration_minutes") > 0,
        F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
    ).otherwise(None)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Quality Filters

# COMMAND ----------

df_silver = (
    df_enriched
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_duration_minutes") > 0)
    .filter(F.col("trip_duration_minutes") < 720)       # Max 12 hours
    .filter(F.col("passenger_count") > 0)
    .filter(
        (F.col("avg_speed_mph").isNull()) |
        (F.col("avg_speed_mph") < 100)                  # Sanity: max 100 mph
    )
)

silver_count = df_silver.count()
filtered_count = bronze_count - silver_count

print(f"Bronze rows:   {bronze_count:,}")
print(f"Silver rows:   {silver_count:,}")
print(f"Filtered out:  {filtered_count:,} ({filtered_count/bronze_count*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Drop Bronze Metadata & Write Silver

# COMMAND ----------

df_silver_final = df_silver.drop("_ingested_at", "_source")

(
    df_silver_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TRIPS)
)

print(f"✓ Silver table written: {SILVER_TRIPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data Profile

# COMMAND ----------

display(spark.table(SILVER_TRIPS).describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

df_s = spark.table(SILVER_TRIPS)

quality_report = df_s.select(
    F.count("*").alias("total_rows"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
    F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
    F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    F.min("tpep_pickup_datetime").alias("earliest_pickup"),
    F.max("tpep_pickup_datetime").alias("latest_pickup"),
)

display(quality_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assumptions & Data Quality Notes
# MAGIC
# MAGIC | Rule | Threshold | Rationale |
# MAGIC |------|-----------|-----------|
# MAGIC | Trip distance | > 0 miles | Zero/negative distances indicate meter errors |
# MAGIC | Fare amount | > $0 | Zero/negative fares are invalid or cancelled trips |
# MAGIC | Trip duration | > 0 min, < 720 min | Must be positive; >12 hours is likely a meter error |
# MAGIC | Passenger count | > 0 | Null or zero indicates a data entry problem |
# MAGIC | Average speed | < 100 mph | Physically implausible in NYC traffic |
# MAGIC
# MAGIC **Missing values**: Rows with null pickup/dropoff timestamps, distance, or fare are dropped.
# MAGIC Non-critical nulls (e.g., `_rescued_data`) are retained.
# MAGIC
# MAGIC **Next step**: Run `03_gold_analytics` to build the analytics layer.

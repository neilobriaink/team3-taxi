# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC **Purpose**: Ingest raw NYC Yellow Taxi data into the Bronze layer.
# MAGIC
# MAGIC The Bronze layer stores data **exactly as received** from the source,
# MAGIC with only metadata columns added for traceability.
# MAGIC
# MAGIC | Input | Output |
# MAGIC |-------|--------|
# MAGIC | `data_academy_resources.nyc_taxi.yellow_tripdata` | `students_data.team3_taxi.bronze_trips` |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

df_source = spark.table(ACTIVE_SOURCE)
source_count = df_source.count()

print(f"Source: {ACTIVE_SOURCE}")
print(f"Rows:   {source_count:,}")
print(f"Columns: {len(df_source.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile Source Data

# COMMAND ----------

display(df_source.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Inspection

# COMMAND ----------

df_source.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Data Quality Check
# MAGIC
# MAGIC Quick scan for nulls and anomalies in key fields — no filtering at Bronze, just observing.

# COMMAND ----------

quality_checks = df_source.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("tpep_pickup_datetime").isNull(), 1).otherwise(0)).alias("null_pickup_dt"),
    F.sum(F.when(F.col("tpep_dropoff_datetime").isNull(), 1).otherwise(0)).alias("null_dropoff_dt"),
    F.sum(F.when(F.col("trip_distance").isNull(), 1).otherwise(0)).alias("null_distance"),
    F.sum(F.when(F.col("fare_amount").isNull(), 1).otherwise(0)).alias("null_fare"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("zero_or_neg_distance"),
    F.sum(F.when(F.col("fare_amount") <= 0, 1).otherwise(0)).alias("zero_or_neg_fare"),
)

display(quality_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Metadata

# COMMAND ----------

df_bronze = (
    df_source
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(ACTIVE_SOURCE))
)

print("Bronze schema:")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TRIPS)
)

print(f"✓ Bronze table written: {BRONZE_TRIPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Table

# COMMAND ----------

bronze_count = spark.table(BRONZE_TRIPS).count()

print(f"Source rows:  {source_count:,}")
print(f"Bronze rows:  {bronze_count:,}")
assert bronze_count == source_count, f"Row count mismatch! Source={source_count}, Bronze={bronze_count}"
print(f"✓ Row counts match")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Summary
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Source | `ACTIVE_SOURCE` (printed above) |
# MAGIC | Rows ingested | See validation above |
# MAGIC | Transformations | None (raw data + metadata) |
# MAGIC | Metadata added | `_ingested_at`, `_source` |
# MAGIC
# MAGIC **Next step**: Run `02_silver_transformation` to clean and enrich the data.

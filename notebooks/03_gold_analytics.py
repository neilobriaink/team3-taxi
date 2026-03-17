# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Analytics & Dashboard
# MAGIC **Purpose**: Build curated analytical tables and visualise business KPIs.
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Gold trips table (enriched fact table with derived metrics)
# MAGIC - Daily metrics aggregation table
# MAGIC - Hourly metrics aggregation table
# MAGIC - Dashboard with 5 KPI visualisations and business narrative
# MAGIC
# MAGIC | Input | Outputs |
# MAGIC |-------|---------|
# MAGIC | `silver_trips` | `gold_trips`, `gold_daily_metrics`, `gold_hourly_metrics` |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

df_silver = spark.table(SILVER_TRIPS)
print(f"Silver rows: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Build Gold Trips Table (Enriched Fact Table)
# MAGIC
# MAGIC Adds business-relevant calculated columns to every trip.

# COMMAND ----------

df_gold = (
    df_silver
    .withColumn(
        "fare_per_mile",
        F.when(F.col("trip_distance") > 0, F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
        .otherwise(None)
    )
    .withColumn(
        "fare_per_minute",
        F.when(F.col("trip_duration_minutes") > 0, F.round(F.col("fare_amount") / F.col("trip_duration_minutes"), 2))
        .otherwise(None)
    )
    .withColumn(
        "tip_percentage",
        F.when(F.col("fare_amount") > 0, F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2))
        .otherwise(None)
    )
    .withColumn(
        "time_of_day",
        F.when(F.col("pickup_hour").between(6, 11), "Morning")
        .when(F.col("pickup_hour").between(12, 16), "Afternoon")
        .when(F.col("pickup_hour").between(17, 21), "Evening")
        .otherwise("Night")
    )
)

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TRIPS)
)

print(f"✓ Gold trips table: {GOLD_TRIPS} ({df_gold.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Daily Metrics Table

# COMMAND ----------

df_daily = (
    df_gold
    .groupBy("pickup_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    )
    .orderBy("pickup_date")
)

(
    df_daily
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAILY_METRICS)
)

print(f"✓ Daily metrics: {GOLD_DAILY_METRICS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Hourly Metrics Table

# COMMAND ----------

df_hourly = (
    df_gold
    .groupBy("pickup_hour", "pickup_day_of_week", "is_weekend")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    )
    .orderBy("pickup_day_of_week", "pickup_hour")
)

(
    df_hourly
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_HOURLY_METRICS)
)

print(f"✓ Hourly metrics: {GOLD_HOURLY_METRICS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Dashboard: NYC Yellow Taxi KPIs
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Summary

# COMMAND ----------

df_kpi = spark.table(GOLD_TRIPS)

kpis = df_kpi.select(
    F.count("*").alias("total_trips"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
    F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
    F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
).collect()[0]

print("=" * 60)
print("       NYC YELLOW TAXI — KEY PERFORMANCE INDICATORS")
print("=" * 60)
print(f"  Total Trips:           {kpis['total_trips']:>12,}")
print(f"  Total Revenue:         ${kpis['total_revenue']:>12,.2f}")
print(f"  Avg Fare:              ${kpis['avg_fare']:>12,.2f}")
print(f"  Avg Distance:          {kpis['avg_distance_mi']:>12,.2f} mi")
print(f"  Avg Duration:          {kpis['avg_duration_min']:>12,.2f} min")
print(f"  Avg Fare/Mile:         ${kpis['avg_fare_per_mile']:>12,.2f}")
print(f"  Avg Tip %:             {kpis['avg_tip_pct']:>12,.1f}%")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1: Trip Volume by Hour of Day
# MAGIC **Why this matters**: Understanding peak demand hours helps optimise fleet allocation and surge pricing strategies.

# COMMAND ----------

pdf_hourly = (
    spark.table(GOLD_HOURLY_METRICS)
    .groupBy("pickup_hour")
    .agg(F.sum("total_trips").alias("total_trips"))
    .orderBy("pickup_hour")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(pdf_hourly["pickup_hour"], pdf_hourly["total_trips"], color="#1f77b4", edgecolor="white")
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Total Trips")
ax.set_title("Trip Volume by Hour of Day")
ax.set_xticks(range(0, 24))
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 2: Average Fare by Time of Day
# MAGIC **Why this matters**: Fare variations by time period reveal pricing dynamics and rider behaviour patterns.

# COMMAND ----------

pdf_tod = (
    df_gold
    .groupBy("time_of_day")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    )
    .orderBy("avg_fare")
    .toPandas()
)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

colors = ["#2ca02c", "#ff7f0e", "#1f77b4", "#d62728"]
axes[0].barh(pdf_tod["time_of_day"], pdf_tod["avg_fare"], color=colors)
axes[0].set_xlabel("Average Fare ($)")
axes[0].set_title("Average Fare by Time of Day")

axes[1].barh(pdf_tod["time_of_day"], pdf_tod["trips"], color=colors)
axes[1].set_xlabel("Number of Trips")
axes[1].set_title("Trip Volume by Time of Day")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3: Daily Revenue Trend
# MAGIC **Why this matters**: Revenue trends over time show seasonality and demand patterns crucial for business planning.

# COMMAND ----------

pdf_daily = spark.table(GOLD_DAILY_METRICS).orderBy("pickup_date").toPandas()

fig, ax1 = plt.subplots(figsize=(14, 5))
ax1.plot(pdf_daily["pickup_date"], pdf_daily["total_revenue"], color="#1f77b4", linewidth=1.5, label="Revenue")
ax1.set_xlabel("Date")
ax1.set_ylabel("Total Revenue ($)", color="#1f77b4")
ax1.tick_params(axis="y", labelcolor="#1f77b4")

ax2 = ax1.twinx()
ax2.plot(pdf_daily["pickup_date"], pdf_daily["total_trips"], color="#ff7f0e", linewidth=1.5, alpha=0.7, label="Trips")
ax2.set_ylabel("Total Trips", color="#ff7f0e")
ax2.tick_params(axis="y", labelcolor="#ff7f0e")

ax1.set_title("Daily Revenue & Trip Volume")
fig.legend(loc="upper left", bbox_to_anchor=(0.12, 0.95))
ax1.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 4: Payment Type Distribution
# MAGIC **Why this matters**: The shift towards cashless payments affects transaction processing, reconciliation, and tip behaviour.

# COMMAND ----------

payment_labels = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown"}

pdf_payment = (
    df_gold
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    )
    .toPandas()
)
pdf_payment["payment_label"] = pdf_payment["payment_type"].map(payment_labels).fillna("Other")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].pie(pdf_payment["trips"], labels=pdf_payment["payment_label"], autopct="%1.1f%%", startangle=90)
axes[0].set_title("Trips by Payment Type")

axes[1].bar(pdf_payment["payment_label"], pdf_payment["avg_tip_pct"], color="#2ca02c", edgecolor="white")
axes[1].set_ylabel("Average Tip %")
axes[1].set_title("Average Tip % by Payment Type")
axes[1].grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 5: Weekday vs Weekend Patterns
# MAGIC **Why this matters**: Understanding demand differences between weekdays and weekends informs driver scheduling and pricing strategy.

# COMMAND ----------

pdf_weekend = (
    df_gold
    .groupBy("is_weekend")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    )
    .toPandas()
)
pdf_weekend["label"] = pdf_weekend["is_weekend"].map({True: "Weekend", False: "Weekday"})

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

axes[0].bar(pdf_weekend["label"], pdf_weekend["avg_fare"], color=["#1f77b4", "#ff7f0e"], edgecolor="white")
axes[0].set_ylabel("Average Fare ($)")
axes[0].set_title("Avg Fare: Weekday vs Weekend")

axes[1].bar(pdf_weekend["label"], pdf_weekend["avg_distance"], color=["#1f77b4", "#ff7f0e"], edgecolor="white")
axes[1].set_ylabel("Average Distance (mi)")
axes[1].set_title("Avg Distance: Weekday vs Weekend")

axes[2].bar(pdf_weekend["label"], pdf_weekend["avg_tip_pct"], color=["#1f77b4", "#ff7f0e"], edgecolor="white")
axes[2].set_ylabel("Average Tip %")
axes[2].set_title("Avg Tip %: Weekday vs Weekend")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Business Narrative
# MAGIC
# MAGIC ### Key Insights
# MAGIC
# MAGIC 1. **Peak Demand**: Evening hours (5–9 PM) show the highest trip volumes, driven by commuter demand. Late night and early morning have the lowest activity.
# MAGIC
# MAGIC 2. **Fare Economics**: Average fare per mile provides a consistent profitability metric. Longer trips (typically to airports) show lower per-mile rates due to distance-based pricing tiers.
# MAGIC
# MAGIC 3. **Payment Trends**: Credit card payments dominate and correlate with significantly higher tip percentages — likely due to the automatic tip prompt on card terminals.
# MAGIC
# MAGIC 4. **Revenue Patterns**: Daily revenue trends reveal weekly seasonality (lower weekends for business-oriented routes) and potential holiday effects.
# MAGIC
# MAGIC 5. **Weekday vs Weekend**: Weekday trips tend to be shorter but more frequent (commuters), while weekend trips are longer on average (leisure, airport runs).
# MAGIC
# MAGIC ### Recommendations
# MAGIC - **Fleet optimisation**: Increase vehicle availability during evening peak hours (5–9 PM)
# MAGIC - **Dynamic pricing**: Consider time-of-day aware pricing models to balance supply and demand
# MAGIC - **Payment incentives**: Encourage credit card usage to boost tip revenue for drivers
# MAGIC - **Weekend strategy**: Target leisure/tourism segments with promotions on weekends

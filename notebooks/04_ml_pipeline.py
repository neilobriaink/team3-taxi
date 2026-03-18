# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Machine Learning Pipeline
# MAGIC **Purpose**: Train and evaluate models to predict taxi fare amounts, logged with MLflow.
# MAGIC
# MAGIC **Prediction Problem**: Given trip features (distance, time, location, etc.), predict `fare_amount`.
# MAGIC
# MAGIC **Approach**:
# MAGIC 1. Feature engineering from Silver data
# MAGIC 2. Baseline model: Linear Regression
# MAGIC 3. Improved model: Gradient Boosted Trees (GBT)
# MAGIC 4. All experiments logged to MLflow with parameters, metrics, and model artifacts
# MAGIC
# MAGIC | Input | Output |
# MAGIC |-------|--------|
# MAGIC | `silver_trips` | MLflow experiment with 2 registered runs |

# COMMAND ----------

# COMMAND ----------

# MAGIC %pip install "typing_extensions>=4.12" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up MLflow Experiment

# COMMAND ----------

mlflow.set_experiment(ML_EXPERIMENT_NAME)
print(f"✓ MLflow experiment: {ML_EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load & Prepare Data

# COMMAND ----------

feature_cols = [
    "trip_distance",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "is_weekend",
    "passenger_count",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "RateCodeID",
    "payment_type",
    "trip_duration_minutes",
]

target_col = "fare_amount"

# COMMAND ----------

df = (
    spark.table(SILVER_TRIPS)
    .select(feature_cols + [target_col])
    .dropna()
    .withColumn("is_weekend", F.col("is_weekend").cast("int"))
)

print(f"ML dataset: {df.count():,} rows, {len(df.columns)} columns")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Assembly & Train/Test Split

# COMMAND ----------

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip",
)

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Train: {train_df.count():,} rows")
print(f"Test:  {test_df.count():,} rows")

# COMMAND ----------

# Evaluators (reused across models)
evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Baseline Model: Linear Regression

# COMMAND ----------

with mlflow.start_run(run_name="baseline_linear_regression") as run:
    lr = LinearRegression(
        featuresCol="features",
        labelCol=target_col,
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.5,
    )
    pipeline_lr = Pipeline(stages=[assembler, lr])

    # Train
    model_lr = pipeline_lr.fit(train_df)

    # Predict
    predictions_lr = model_lr.transform(test_df)

    # Evaluate
    rmse_lr = evaluator_rmse.evaluate(predictions_lr)
    mae_lr = evaluator_mae.evaluate(predictions_lr)
    r2_lr = evaluator_r2.evaluate(predictions_lr)

    # Log parameters
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("features", str(feature_cols))
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("reg_param", 0.1)
    mlflow.log_param("elastic_net_param", 0.5)
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())

    # Log metrics
    mlflow.log_metric("rmse", rmse_lr)
    mlflow.log_metric("mae", mae_lr)
    mlflow.log_metric("r2", r2_lr)

    # Log model
    mlflow.spark.log_model(model_lr, "model")

    print("=" * 50)
    print("  BASELINE: Linear Regression")
    print("=" * 50)
    print(f"  RMSE:  ${rmse_lr:.4f}")
    print(f"  MAE:   ${mae_lr:.4f}")
    print(f"  R²:    {r2_lr:.4f}")
    print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Improved Model: Gradient Boosted Trees

# COMMAND ----------

with mlflow.start_run(run_name="improved_gbt_regressor") as run:
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=target_col,
        maxIter=100,
        maxDepth=8,
        stepSize=0.1,
        seed=42,
    )
    pipeline_gbt = Pipeline(stages=[assembler, gbt])

    # Train
    model_gbt = pipeline_gbt.fit(train_df)

    # Predict
    predictions_gbt = model_gbt.transform(test_df)

    # Evaluate
    rmse_gbt = evaluator_rmse.evaluate(predictions_gbt)
    mae_gbt = evaluator_mae.evaluate(predictions_gbt)
    r2_gbt = evaluator_r2.evaluate(predictions_gbt)

    # Log parameters
    mlflow.log_param("model_type", "GBTRegressor")
    mlflow.log_param("features", str(feature_cols))
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("step_size", 0.1)
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())

    # Log metrics
    mlflow.log_metric("rmse", rmse_gbt)
    mlflow.log_metric("mae", mae_gbt)
    mlflow.log_metric("r2", r2_gbt)

    # Log model
    mlflow.spark.log_model(model_gbt, "model")

    print("=" * 50)
    print("  IMPROVED: Gradient Boosted Trees")
    print("=" * 50)
    print(f"  RMSE:  ${rmse_gbt:.4f}")
    print(f"  MAE:   ${mae_gbt:.4f}")
    print(f"  R²:    {r2_gbt:.4f}")
    print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Model Comparison

# COMMAND ----------

comparison = pd.DataFrame({
    "Model": ["Linear Regression (Baseline)", "Gradient Boosted Trees (Improved)"],
    "RMSE": [rmse_lr, rmse_gbt],
    "MAE": [mae_lr, mae_gbt],
    "R²": [r2_lr, r2_gbt],
})

display(spark.createDataFrame(comparison))

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

models = ["Linear Regression", "GBT"]
colors = ["#1f77b4", "#2ca02c"]

axes[0].bar(models, [rmse_lr, rmse_gbt], color=colors, edgecolor="white")
axes[0].set_title("RMSE (lower is better)")
axes[0].set_ylabel("RMSE ($)")

axes[1].bar(models, [mae_lr, mae_gbt], color=colors, edgecolor="white")
axes[1].set_title("MAE (lower is better)")
axes[1].set_ylabel("MAE ($)")

axes[2].bar(models, [r2_lr, r2_gbt], color=colors, edgecolor="white")
axes[2].set_title("R² (higher is better)")
axes[2].set_ylabel("R²")

plt.suptitle("Model Comparison: Fare Amount Prediction", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance (GBT)

# COMMAND ----------

gbt_model = model_gbt.stages[-1]  # Extract GBT model from pipeline
importances = gbt_model.featureImportances.toArray()

fi_df = pd.DataFrame({
    "feature": feature_cols,
    "importance": importances,
}).sort_values("importance", ascending=True)

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(fi_df["feature"], fi_df["importance"], color="#2ca02c", edgecolor="white")
ax.set_xlabel("Feature Importance")
ax.set_title("GBT Feature Importance for Fare Prediction")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction vs Actual (GBT)

# COMMAND ----------

pdf_pred = (
    predictions_gbt
    .select("fare_amount", "prediction")
    .sample(0.1, seed=42)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(8, 8))
ax.scatter(pdf_pred["fare_amount"], pdf_pred["prediction"], alpha=0.3, s=5, color="#2ca02c")
max_val = max(pdf_pred["fare_amount"].max(), pdf_pred["prediction"].max())
ax.plot([0, max_val], [0, max_val], "r--", linewidth=2, label="Perfect prediction")
ax.set_xlabel("Actual Fare ($)")
ax.set_ylabel("Predicted Fare ($)")
ax.set_title("GBT: Predicted vs Actual Fare Amount")
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Modelling Summary
# MAGIC
# MAGIC ### Approach
# MAGIC - **Problem**: Regression — predict taxi fare amount from trip features
# MAGIC - **Features**: Trip distance, time features (hour, day of week, month, weekend flag), passenger count, pickup/dropoff location zones, rate code, payment type, trip duration
# MAGIC - **Baseline**: Linear Regression with L1/L2 regularisation (ElasticNet)
# MAGIC - **Improved**: Gradient Boosted Trees — an ensemble of decision trees trained sequentially
# MAGIC
# MAGIC ### Key Findings
# MAGIC - GBT significantly outperforms the linear baseline, capturing nonlinear fare patterns
# MAGIC - **Trip distance** and **trip duration** are the dominant predictors (as expected — NYC fares are metered by distance and time)
# MAGIC - **Location features** (pickup/dropoff zones) provide additional predictive value for flat-rate routes (e.g., JFK airport)
# MAGIC - **Time features** have moderate importance, capturing peak-hour surcharges and demand patterns
# MAGIC
# MAGIC ### Why These Models?
# MAGIC - **Linear Regression**: Simple, interpretable baseline — establishes a floor for performance
# MAGIC - **GBT**: Industry-standard for tabular regression, handles nonlinearities and feature interactions without manual engineering
# MAGIC
# MAGIC ### MLflow Tracking
# MAGIC - Both models logged with parameters, metrics (RMSE, MAE, R²), and serialised model artifacts
# MAGIC - View the experiment in the Databricks Experiments UI

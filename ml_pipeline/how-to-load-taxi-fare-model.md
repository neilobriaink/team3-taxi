# How to Load and Use the Taxi Fare Prediction Model

This guide explains how to load the trained taxi fare model from MLflow and use it in a new Databricks notebook.

The model is saved as a **Spark ML PipelineModel** that bundles `VectorAssembler → StandardScaler → RandomForest` into a single artifact. No separate scaler setup is needed — just pass raw feature columns and the pipeline handles everything.

## Where Is the Model Saved?

- **MLflow Experiment:** `/Users/neil.obriain@kainos.com/taxi-fare-prediction`
- **Run name:** `random_forest_user_friendly`
- **Run ID:** `09418df8decf4fe8af5e05732331edff`
- **Artifact path:** `taxi_fare_model`
- **Training notebook:** `task4-refine` (Cell 40)
- **Demo notebook:** `Taxi Fare Predictor`

---

## 1. Find the MLflow Run ID

1. In Databricks, click **Experiments** in the left sidebar.
2. Open the experiment: `/Users/neil.obriain@kainos.com/taxi-fare-prediction`
3. Find the latest run (named `random_forest_user_friendly`).
4. Copy the **Run ID** from the run details page.

The current Run ID is `09418df8decf4fe8af5e05732331edff`.

---

## 2. Load the Model in a New Notebook

```python
import os
import mlflow.spark

# Required for shared clusters with USER_ISOLATION
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/students_data/team3-taxi/mlflow_tmp"

# Replace <run_id> with your actual Run ID from Step 1
model = mlflow.spark.load_model("runs:/<run_id>/taxi_fare_model")
```

The returned `model` is a `PipelineModel` with three stages:

| Stage | Type | Purpose |
| --- | --- | --- |
| 0 | `VectorAssembler` | Combines 9 feature columns into a single vector |
| 1 | `StandardScalerModel` | Scales features (mean=0, std=1), already fitted |
| 2 | `RandomForestRegressionModel` | Predicts `total_amount` |

---

## 3. Make a Prediction

The model expects **9 user-friendly features** as raw columns. The pipeline handles assembling and scaling internally.

| Feature | Description | Example Values |
| --- | --- | --- |
| `trip_distance` | Distance in miles | 1.0 – 30.0 |
| `trip_duration_minutes` | Trip time in minutes | 5 – 120 |
| `passenger_count` | Number of passengers | 1 – 6 |
| `vendor_key` | Taxi company | 1 or 2 |
| `rate_code_key` | Rate type | 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated |
| `payment_type_key` | Payment method | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute |
| `pickup_hour` | Hour of pickup (0–23) | 0 – 23 |
| `pickup_day_of_week` | Day of week | 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat |
| `pickup_is_weekend` | Weekend flag | 0=weekday, 1=weekend |

### Full Example

```python
# Create a DataFrame with your trip data (all values as floats)
new_trip = spark.createDataFrame([{
    'trip_distance':         5.0,
    'trip_duration_minutes': 20.0,
    'passenger_count':       2.0,
    'vendor_key':            1.0,
    'rate_code_key':         1.0,
    'payment_type_key':      1.0,
    'pickup_hour':           14.0,
    'pickup_day_of_week':    4.0,
    'pickup_is_weekend':     0.0,
}])

# Predict — the pipeline handles assembling and scaling internally
predictions = model.transform(new_trip)
display(predictions.select('prediction'))
```

---

## 4. Quick Prediction Function (Copy-Paste Ready)

```python
def predict_fare(trip_distance, trip_duration_minutes, passenger_count=1,
                 pickup_hour=12, pickup_day_of_week=3, pickup_is_weekend=0,
                 vendor_key=1, rate_code_key=1, payment_type_key=1):
    trip = {
        'trip_distance':         float(trip_distance),
        'trip_duration_minutes': float(trip_duration_minutes),
        'passenger_count':       float(passenger_count),
        'vendor_key':            float(vendor_key),
        'rate_code_key':         float(rate_code_key),
        'payment_type_key':      float(payment_type_key),
        'pickup_hour':           float(pickup_hour),
        'pickup_day_of_week':    float(pickup_day_of_week),
        'pickup_is_weekend':     float(pickup_is_weekend),
    }
    trip_df = spark.createDataFrame([trip])
    return round(model.transform(trip_df).select('prediction').first()[0], 2)

# Example usage
fare = predict_fare(trip_distance=5.0, trip_duration_minutes=20, pickup_hour=17)
print(f"Predicted fare: ${fare:.2f}")
```

---

## 5. Batch Prediction on Multiple Trips

```python
from pyspark.sql.functions import col

# Load any DataFrame with the 9 feature columns
trips_df = spark.createDataFrame([
    (3.5, 15.0, 1.0, 1.0, 1.0, 1.0, 17.0, 6.0, 0.0),
    (10.0, 30.0, 2.0, 2.0, 1.0, 1.0, 9.0, 3.0, 0.0),
], ['trip_distance', 'trip_duration_minutes', 'passenger_count',
    'vendor_key', 'rate_code_key', 'payment_type_key',
    'pickup_hour', 'pickup_day_of_week', 'pickup_is_weekend'])

scored = model.transform(trips_df)
display(scored.select('trip_distance', 'trip_duration_minutes', 'prediction'))
```

---

## 6. Model Accuracy

Tested on 200 random valid trips (>= 1 mile):

| Metric | Value |
| --- | --- |
| Mean Absolute Error | $1.48 |
| Median Absolute Error | $0.81 |
| Mean Abs % Error | 7.4% |

Full test set (2.49M trips): MAE $1.60, R² 0.889.

---

## Important Notes

- **No scaler dependency:** The model is a full `PipelineModel` that includes the fitted `StandardScaler`. Just pass raw feature columns — no manual scaling needed.
- **Feature order matters:** The 9 features must be in the exact order listed above.
- **All values must be floats:** Cast inputs to `float()` before creating the DataFrame.
- **Shared cluster requirement:** Set `MLFLOW_DFS_TMP` to a `/Volumes/` path before loading the model.

---

## Source

- **Training notebook:** `/Users/neil.obriain@kainos.com/team3-taxi/ml_pipeline/task4-refine`
- **Demo notebook:** `/Users/neil.obriain@kainos.com/team3-taxi/ml_pipeline/Taxi Fare Predictor`
- **MLflow experiment:** `/Users/neil.obriain@kainos.com/taxi-fare-prediction`
- **Data source:** `students_data.team3-taxi.fact_trip` joined with `students_data.team3-taxi.dim_datetime_hour`

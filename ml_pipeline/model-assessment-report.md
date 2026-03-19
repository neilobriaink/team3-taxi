# Taxi Fare Prediction Model — Assessment Report

**Team:** Team 3  
**Date:** 19 March 2026  
**Notebook:** `task4-refine` (Cells 40–42)  
**Demo Notebook:** `Taxi Fare Predictor`  
**Platform:** Databricks (Azure), Spark 17.3, Photon Runtime  
**MLflow Run ID:** `09418df8decf4fe8af5e05732331edff`

---

## 1. Project Overview

We built an end-to-end machine learning pipeline to predict NYC yellow taxi total fares using **only information a passenger would know** before or during a trip — no internal system data, no fare breakdowns, and no location IDs.

The model is trained on **46.4 million taxi trips** from the `students_data.team3-taxi.fact_trip` table, enriched with time-of-day features from `dim_datetime_hour`.

---

## 2. Data Summary

| Metric | Value |
| --- | --- |
| Total trips in dataset | 46,388,197 |
| Training set size | 9,980,001 (80%) |
| Test set size | 2,495,045 (20%) |
| Train/test split seed | 42 |
| Source table | `students_data.team3-taxi.fact_trip` |
| Dimension table joined | `students_data.team3-taxi.dim_datetime_hour` |

---

## 3. Feature Engineering

### Design Philosophy

The original dataset contains 30 columns including fare breakdowns (`fare_amount`, `tip_amount`), derived ratios (`revenue_per_mile`, `tip_pct`), and opaque surrogate keys (`pickup_location_key`). We deliberately excluded all of these to create a model that works with **user-friendly inputs only**.

The surrogate datetime keys (`pickup_datetime_hour_key`, `pickup_datetime_day_key`) were resolved by joining the `dim_datetime_hour` dimension table to extract actual hour-of-day and day-of-week values.

### Final Feature Set (9 Features)

| # | Feature | Type | Description | User Knows? |
| --- | --- | --- | --- | --- |
| 1 | `trip_distance` | double | Trip distance in miles | Yes — from map/estimate |
| 2 | `trip_duration_minutes` | long | Trip duration in minutes | Yes — from map/estimate |
| 3 | `passenger_count` | long | Number of passengers | Yes |
| 4 | `vendor_key` | int | Taxi company (1 or 2) | Yes — visible on cab |
| 5 | `rate_code_key` | int | Rate type (1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated) | Yes — selected at start |
| 6 | `payment_type_key` | int | Payment method (1=Card, 2=Cash) | Yes |
| 7 | `pickup_hour` | int | Hour of pickup (0–23) | Yes |
| 8 | `pickup_day_of_week` | int | Day of week (1=Sun … 7=Sat) | Yes |
| 9 | `pickup_is_weekend` | int | Weekend flag (0=weekday, 1=weekend) | Yes |

### Features Excluded (and Why)

| Excluded Feature | Reason |
| --- | --- |
| `fare_amount`, `tip_amount`, `tolls_amount`, `extra`, `mta_tax`, `improvement_surcharge` | Components of `total_amount` (target) — circular/leakage |
| `revenue_per_mile`, `tip_pct`, `fare_per_minute`, `fare_per_passenger`, `effective_rate` | Derived from fare data — not available pre-trip |
| `total_charges` | Nearly identical to target |
| `pickup_location_key`, `dropoff_location_key` | Opaque surrogate keys — not user-friendly |
| `pickup_datetime_day_key`, `pickup_datetime_hour_key`, `pickup_datetime_minute_key` | Surrogate keys — replaced with actual values from dim tables |
| `dropoff_datetime_*` keys | User doesn't know dropoff time in advance |
| `store_and_fwd_flag`, `trip_id`, `avg_speed_mph` | Identifiers or post-trip metrics |

---

## 4. Model Architecture

The model is saved as a **Spark ML PipelineModel** that bundles all preprocessing and prediction into a single artifact:

| Stage | Component | Detail |
| --- | --- | --- |
| 1 | `VectorAssembler` | Combines 9 feature columns into a single vector (`features_raw`) |
| 2 | `StandardScaler` | Scales features (withMean=True, withStd=True) → `features` |
| 3 | `RandomForestRegressor` | 100 trees, predicts `total_amount` (total fare in USD) |

| Property | Value |
| --- | --- |
| Saved as | Spark ML `PipelineModel` (all stages in one artifact) |
| Random seed | 42 |
| Framework | PySpark MLlib (distributed) |
| Target variable | `total_amount` (total fare in USD) |

Because the entire pipeline is saved as one MLflow artifact, consumers only need to call `model.transform(raw_df)` — no separate scaler re-fitting is required.

---

## 5. Model Performance — Full Test Set

Evaluated on the held-out **2.49 million trip** test set:

| Metric | Value | Interpretation |
| --- | --- | --- |
| **MAE** | **$1.60** | On average, predictions are within $1.60 of actual fare |
| **R²** | **0.889** | Model explains 88.9% of fare variance |

---

## 6. Model Performance — 200 Random Real Trips (>= 1 mile)

To validate practical accuracy, we scored 200 randomly sampled valid trips (distance >= 1 mile, fare > $0, duration > 0).

### 6.1 Headline Metrics

| Metric | Value |
| --- | --- |
| Mean Absolute Error (MAE) | **$1.48** |
| Median Absolute Error | **$0.81** |
| Mean Absolute % Error (MAPE) | **7.4%** |

### 6.2 Error Distribution

| Accuracy Band | Count | Percentage |
| --- | --- | --- |
| Within $0.50 | 64 / 200 | **32.0%** |
| Within $1.00 | 120 / 200 | **60.0%** |
| Within $2.00 | 171 / 200 | **85.5%** |
| Within $5.00 | 191 / 200 | **95.5%** |
| Over $5.00 | 9 / 200 | 4.5% |
| Over $10.00 | 3 / 200 | 1.5% |

**Key takeaway:** 85.5% of predictions are within $2 of the actual fare, and 95.5% within $5.

### 6.3 Error Percentiles

| Percentile | Absolute Error |
| --- | --- |
| 25th | $0.39 |
| 50th (Median) | $0.81 |
| 75th | $1.42 |
| 90th | $2.47 |
| 95th | $4.33 |
| Max | $38.77 |
| Std Dev | $3.26 |

### 6.4 Accuracy by Trip Distance

| Distance Range | Trips | MAE | Avg Fare | MAPE |
| --- | --- | --- | --- | --- |
| 1–2 mi | 90 | **$0.76** | $10.13 | 8.0% |
| 2–5 mi | 81 | **$0.93** | $16.28 | 5.5% |
| 5–10 mi | 15 | $5.03 | $31.32 | 11.8% |
| 10+ mi | 14 | $5.50 | $50.77 | 10.7% |

The model performs best on short-to-medium trips (1–5 miles), which represent the vast majority of NYC taxi rides. Longer trips (10+ miles) show higher absolute errors but are still within ~11% of the actual fare.

### 6.5 Test Sample Data Range

| Metric | Min | Max | Average |
| --- | --- | --- | --- |
| Fare | $6.80 | $68.66 | $17.05 |
| Distance | 1.0 mi | 21.6 mi | 3.4 mi |

---

## 7. Best and Worst Predictions

### Top 5 Most Accurate

| Distance | Actual Fare | Predicted | Error |
| --- | --- | --- | --- |
| 1.1 mi | $9.45 | $9.46 | **$0.01** |
| 2.0 mi | $11.80 | $11.79 | **$0.01** |
| 2.2 mi | $10.80 | $10.79 | **$0.01** |
| 2.2 mi | $12.30 | $12.28 | **$0.02** |
| 2.7 mi | $13.80 | $13.82 | **$0.02** |

### Top 5 Least Accurate

| Distance | Actual Fare | Predicted | Error | Likely Cause |
| --- | --- | --- | --- | --- |
| 8.0 mi | $68.66 | $29.90 | $38.77 | Outlier — fare far above normal for distance |
| 21.6 mi | $63.30 | $47.10 | $16.20 | Very long trip — outside typical range |
| 14.1 mi | $59.46 | $45.78 | $13.68 | High fare for distance — possible tolls/surcharges |
| 15.2 mi | $41.80 | $32.99 | $8.81 | Long-distance variability |
| 16.2 mi | $55.80 | $48.56 | $7.24 | Long-distance variability |

**Analysis:** The worst predictions are on long-distance trips or fare outliers. The $68.66 fare for an 8-mile trip is anomalous (likely includes large tolls/tips the model cannot see). Excluding this single outlier, the max error drops to $16.20.

---

## 8. Example Predictions (Cell 41)

The `predict_fare()` function demonstrates the model with realistic scenarios:

| Scenario | Distance | Duration | Hour | Day | Predicted Fare |
| --- | --- | --- | --- | --- | --- |
| Short weekday morning (8am Tue) | 1 mi | 5 min | 8 | Tue | **$9.25** |
| Medium weekday afternoon (2pm Wed) | 5 mi | 20 min | 14 | Wed | **$19.83** |
| Long weekend evening (9pm Sat) | 15 mi | 45 min | 21 | Sat | **$51.90** |
| JFK flat-rate weekday (6am Mon) | 18 mi | 50 min | 6 | Mon | **$52.99** |
| Late-night weekend short hop (1am Sun) | 2 mi | 10 min | 1 | Sun | **$12.01** |

These predictions align with real NYC taxi fare expectations.

---

## 9. MLflow Experiment Tracking

| Item | Value |
| --- | --- |
| Experiment path | `/Users/neil.obriain@kainos.com/taxi-fare-prediction` |
| Run name | `random_forest_user_friendly` |
| Run ID | `09418df8decf4fe8af5e05732331edff` |
| Artifact path | `taxi_fare_model` |
| Artifact type | Spark ML `PipelineModel` (VectorAssembler → StandardScaler → RandomForest) |
| Model signature | Inferred from predictions (9 input features → 1 output) |
| Input example | Included for serving compatibility |
| Logged parameters | model_type, features, num_features, train_size, test_size, pipeline_stages |
| Logged metrics | MAE ($1.60), R² (0.889) |

### How to Load the Saved Model

The saved artifact is a full `PipelineModel` — it includes the fitted `VectorAssembler`, `StandardScaler`, and `RandomForestRegressionModel`. No separate scaler re-fitting is needed.

```python
import os, mlflow.spark
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/students_data/team3-taxi/mlflow_tmp"
model = mlflow.spark.load_model("runs:/09418df8decf4fe8af5e05732331edff/taxi_fare_model")

# Predict directly from raw feature columns — pipeline handles assembling and scaling
trip = spark.createDataFrame([{'trip_distance': 5.0, 'trip_duration_minutes': 20.0,
    'passenger_count': 2.0, 'vendor_key': 1.0, 'rate_code_key': 1.0,
    'payment_type_key': 1.0, 'pickup_hour': 14.0, 'pickup_day_of_week': 4.0,
    'pickup_is_weekend': 0.0}])
prediction = model.transform(trip)
```

See `how-to-load-taxi-fare-model.md` and the `Taxi Fare Predictor` notebook for full usage examples.

---

## 10. Technical Infrastructure

| Component | Detail |
| --- | --- |
| Cloud | Azure |
| Databricks Runtime | 17.3 (Photon) |
| Spark version | 17.3.x-scala2.13 |
| Compute | Shared Compute 17.3 (Standard_D4pds_v6) |
| Data security mode | USER_ISOLATION |
| Catalog | Unity Catalog (`students_data`) |
| Schema | `team3-taxi` |
| Model storage | MLflow + `/Volumes/students_data/team3-taxi/mlflow_tmp` |

---

## 11. Strengths

1. **User-friendly design** — Only requires 9 inputs a passenger would naturally know
2. **High accuracy for typical trips** — 85.5% of predictions within $2, median error just $0.81
3. **Scalable** — Trained on 46M+ trips using distributed Spark MLlib
4. **Reproducible** — Full experiment tracked in MLflow with signature and input examples
5. **Strong R²** — 0.889 indicates the model captures 88.9% of fare variance with only user-friendly features
6. **Time-aware** — Incorporates hour-of-day, day-of-week, and weekend effects via dimension table joins
7. **Self-contained artifact** — Saved as a Spark ML `PipelineModel` (assembler + scaler + model). Consumers just call `model.transform(raw_df)` — no separate scaler re-fitting or preprocessing required

---

## 12. Known Limitations & Future Improvements

| Limitation | Potential Improvement |
| --- | --- |
| Higher error on 10+ mile trips (MAE $5.50) | Add distance buckets or train separate models for long trips |
| Cannot account for tolls, surcharges, tips | Add `tolls_amount` as a user input (passengers can estimate) |
| No location awareness | Add borough or zone-level features (less granular than lat/long) |
| Single model type | Try GBTRegressor, CrossValidator for hyperparameter tuning |
| Outlier sensitivity | Filter extreme fares during training (e.g. cap at $200) |

---

## 13. Conclusion

The Random Forest model achieves strong predictive performance (**MAE $1.48, R² 0.889**) using only passenger-known features. It is practical for real-world fare estimation — a user simply provides distance, duration, passengers, time of day, and payment method to receive an accurate fare prediction.

The model is saved as a self-contained Spark ML `PipelineModel` in MLflow, bundling all preprocessing (feature assembly and scaling) with the trained model. This means consumers can load and use the model with a single `model.transform()` call — no separate scaler setup required.

See `how-to-load-taxi-fare-model.md` for loading instructions and the `Taxi Fare Predictor` notebook for a ready-to-run demo.

---

*Report updated 19 March 2026 from `task4-refine` notebook, Cells 40–42.*  
*Team 3 — Taxi Fare Prediction Project*

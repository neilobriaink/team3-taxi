# Data Dictionary — NYC Yellow Taxi

## Source Data: `yellow_tripdata`

| Column | Type | Description |
|--------|------|-------------|
| VendorID | int | TPEP provider (1=Creative Mobile Technologies, 2=VeriFone Inc.) |
| tpep_pickup_datetime | timestamp | Date and time when the meter was engaged |
| tpep_dropoff_datetime | timestamp | Date and time when the meter was disengaged |
| passenger_count | int | Number of passengers in the vehicle (driver-entered) |
| trip_distance | double | Elapsed trip distance in miles reported by the taximeter |
| RatecodeID | int | Rate code in effect (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride) |
| store_and_fwd_flag | string | Y/N — whether the trip record was held in vehicle memory before sending to vendor (Y=store and forward, N=not) |
| PULocationID | int | TLC Taxi Zone where the meter was engaged |
| DOLocationID | int | TLC Taxi Zone where the meter was disengaged |
| payment_type | int | How the passenger paid (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip) |
| fare_amount | double | Time-and-distance fare calculated by the meter ($) |
| extra | double | Miscellaneous extras and surcharges ($), e.g., $0.50/$1.00 rush hour/overnight |
| mta_tax | double | $0.50 MTA tax triggered based on metered rate |
| tip_amount | double | Tip amount ($) — automatically populated for credit card tips, cash tips not included |
| tolls_amount | double | Total amount of all tolls paid in trip ($) |
| improvement_surcharge | double | $0.30 improvement surcharge assessed on hailed trips |
| total_amount | double | Total amount charged to passengers, does not include cash tips ($) |
| congestion_surcharge | double | Congestion surcharge for trips in Manhattan below 96th St ($) |

## Bronze Layer: `students_data.team3_taxi.bronze_trips`

All source columns preserved as-is, plus metadata:

| Column | Type | Description |
|--------|------|-------------|
| *(all source columns)* | — | Raw values, unmodified |
| _ingested_at | timestamp | UTC timestamp when record was ingested into Bronze |
| _source | string | Fully qualified source table name |

**Row count**: Same as source (no filtering at Bronze)

## Silver Layer: `students_data.team3_taxi.silver_trips`

Cleaned and enriched data:

| Column | Type | Description |
|--------|------|-------------|
| *(all source columns)* | — | Cleaned versions of original columns |
| trip_duration_minutes | double | Calculated: (dropoff - pickup) in minutes |
| avg_speed_mph | double | Calculated: distance / (duration / 60) |
| pickup_hour | int | Hour of pickup datetime (0-23) |
| pickup_day_of_week | int | Day of week of pickup (1=Sunday, 7=Saturday) |
| pickup_month | int | Month of pickup (1-12) |
| pickup_date | date | Date of pickup (date only, no time) |
| is_weekend | boolean | True if pickup is Saturday or Sunday |

### Filters Applied at Silver Layer

| Rule | Rationale |
|------|-----------|
| `trip_distance > 0` | Zero/negative distances indicate meter errors |
| `fare_amount > 0` | Zero/negative fares are invalid or cancelled trips |
| `trip_duration_minutes > 0` | Trips must have positive duration |
| `trip_duration_minutes < 720` | Trips over 12 hours are likely meter errors |
| `passenger_count > 0` | Required for analysis; null/zero indicates bad data |
| `avg_speed_mph < 100` | Physically implausible speeds in NYC |

## Gold Layer

### `students_data.team3_taxi.gold_trips` (Enriched Fact Table)

All Silver columns plus:

| Column | Type | Description |
|--------|------|-------------|
| fare_per_mile | double | fare_amount / trip_distance |
| fare_per_minute | double | fare_amount / trip_duration_minutes |
| tip_percentage | double | (tip_amount / fare_amount) × 100 |
| time_of_day | string | Morning (6-11), Afternoon (12-16), Evening (17-21), Night (22-5) |

### `students_data.team3_taxi.gold_daily_metrics` (Daily Aggregation)

| Column | Type | Description |
|--------|------|-------------|
| pickup_date | date | Date of trips |
| total_trips | long | Count of trips on that date |
| total_revenue | double | Sum of total_amount |
| avg_fare | double | Average fare_amount |
| avg_trip_distance | double | Average trip distance in miles |
| avg_trip_duration | double | Average trip duration in minutes |
| avg_fare_per_mile | double | Average fare per mile |
| avg_tip_pct | double | Average tip percentage |

### `students_data.team3_taxi.gold_hourly_metrics` (Hourly Aggregation)

| Column | Type | Description |
|--------|------|-------------|
| pickup_hour | int | Hour of day (0-23) |
| pickup_day_of_week | int | Day of week (1=Sun, 7=Sat) |
| is_weekend | boolean | Whether it's a weekend day |
| total_trips | long | Count of trips |
| avg_fare | double | Average fare amount |
| avg_trip_distance | double | Average trip distance |
| avg_trip_duration | double | Average trip duration |
| avg_tip_pct | double | Average tip percentage |

## ML Features Table

Used by `04_ml_pipeline` for fare amount prediction:

| Feature | Type | Description |
|---------|------|-------------|
| trip_distance | double | Primary predictor — metered distance |
| pickup_hour | int | Captures time-of-day pricing effects |
| pickup_day_of_week | int | Captures weekday/weekend patterns |
| pickup_month | int | Captures seasonal trends |
| is_weekend | int | Binary weekend indicator |
| passenger_count | int | Number of passengers |
| PULocationID | int | Pickup zone — captures location-based pricing |
| DOLocationID | int | Dropoff zone — captures route-based pricing |
| RatecodeID | int | Rate code (flat rate vs metered) |
| payment_type | int | Payment method |
| trip_duration_minutes | double | Trip time — correlated with metered fare |
| **fare_amount** | **double** | **TARGET variable** |

## RAG Tables

### `students_data.team3_taxi.rag_embeddings`

| Column | Type | Description |
|--------|------|-------------|
| doc_id | string | Document identifier |
| title | string | Document title |
| chunk_id | string | Unique chunk identifier (doc_id + chunk index) |
| chunk_text | string | Text content of the chunk |
| embedding_str | string | Serialised embedding vector as string |

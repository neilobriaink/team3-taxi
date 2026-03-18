# Data Dictionary: NYC Taxi Data Warehouse

**Last Updated:** March 18, 2026  
**Catalog:** `students_data`  
**Schema:** `team3-taxi`

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Entity-Relationship Diagram](#entity-relationship-diagram)
3. [Table Catalog](#table-catalog)
4. [Column Definitions](#column-definitions)
5. [Data Quality Rules](#data-quality-rules)
6. [Lineage & Transformation Logic](#lineage--transformation-logic)
7. [Grain & Cardinality](#grain--cardinality)
8. [Data Dictionary Metadata Tables](#data-dictionary-metadata-tables)

---

## Architecture Overview

### Medallion Architecture (Bronze-Silver-Gold)

This data warehouse implements the **medallion architecture**, a layered approach that progressively refines data quality and adds business value.

```
SOURCE DATA
    ↓
[BRONZE LAYER] - Raw, unfiltered data copy
    ↓ (no transformations, schema preservation)
[SILVER LAYER] - Cleaned, validated, business-logic-enriched data
    ↓ (filtering, standardization, derived columns)
[GOLD LAYER] - Presentation-ready, conformed dimensional models
    ↓ (fact tables, dimensions, KPIs, optimized for analytics)
ANALYTICS & BI TOOLS
```

### Layer Responsibilities

| Layer | Purpose | Source | Key Characteristics |
|-------|---------|--------|---------------------|
| **Bronze** | Raw data ingestion | External (NYC Taxi Academy) | Zero transformations, full fidelity, auditable |
| **Silver** | Data cleaning & enrichment | Bronze layer | Filtered rows, validated ranges, derived fields, business logic |
| **Gold** | Analytics-ready presentation | Silver + Dimensions | Conformed schemas, surrogate keys, KPIs, denormalized for speed |

---

## Entity-Relationship Diagram

### Star Schema: Fact-to-Dimension Model

```
                    dim_vendor
                       ▲
                       │ 1
                       │
                       M
                       │
                    FACT_TRIP
                       │
             ┌─────────┼─────────────┬──────────────┬──────────────┐
             │         │             │              │              │
             M         M             M              M              M
             │         │             │              │              │
     dim_rate_code  dim_payment_type  dim_location   dim_datetime_day
     dim_location   (for pickup &    (pickup &      dim_datetime_hour
     (pickup &      dropoff)         dropoff)       dim_datetime_minute
     dropoff)                                      (pickup & dropoff)

Cardinality: All relationships are Many-to-One (M:1)
Join Type: All dimensions linked via LEFT JOIN (preserve trips with unknown dimensions)
Key Pattern: Surrogate keys (vendor_key, rate_code_key, etc.)
```

### Table Relationships Summary

| Fact Table | Dimension | FK Column | Cardinality | Description |
|------------|-----------|-----------|-------------|-------------|
| `fact_trip` | `dim_vendor` | `vendor_key` | M:1 | One vendor operates many trips |
| `fact_trip` | `dim_rate_code` | `rate_code_key` | M:1 | One rate code applies to many trips |
| `fact_trip` | `dim_payment_type` | `payment_type_key` | M:1 | One payment method used by many trips |
| `fact_trip` | `dim_location` | `pickup_location_key` | M:1 | One pickup location serves many trips |
| `fact_trip` | `dim_location` | `dropoff_location_key` | M:1 | One dropoff location serves many trips |
| `fact_trip` | `dim_datetime_day` | `pickup_datetime_day_key` | M:1 | One day has many trip pickups |
| `fact_trip` | `dim_datetime_day` | `dropoff_datetime_day_key` | M:1 | One day has many trip dropoffs |
| `fact_trip` | `dim_datetime_hour` | `pickup_datetime_hour_key` | M:1 | One hour has many trip pickups |
| `fact_trip` | `dim_datetime_hour` | `dropoff_datetime_hour_key` | M:1 | One hour has many trip dropoffs |
| `fact_trip` | `dim_datetime_minute` | `pickup_datetime_minute_key` | M:1 | One minute may have multiple pickups |
| `fact_trip` | `dim_datetime_minute` | `dropoff_datetime_minute_key` | M:1 | One minute may have multiple dropoffs |

---

## Table Catalog

### BRONZE LAYER

#### `bronze_yellow_trips`

**Definition:** Raw, unfiltered copy of NYC yellow taxi trip data from Databricks Academy resources. This is the source of truth for data lineage and auditing.

**Source:** `data_academy_resources.nyc_taxi.yellow_tripdata`  
**Grain:** One row per trip  
**Refresh Strategy:** Full refresh (CREATE OR REPLACE)  
**Row Count (est.):** ~2-3 million  
**Time Period:** Jan 2015, Jan-Mar 2016

**Purpose:**
- Preserve raw data exactly as received (immutable record)
- Enable debugging and root-cause analysis
- Support compliance and audit trails
- Serve as the single source of truth

**Key Characteristics:**
- Zero transformations or filtering
- All 19 columns from source preserved verbatim
- No derived fields
- No surrogate keys

---

### SILVER LAYER

#### `silver_trips`

**Definition:** Validated, filtered taxi trips with all data quality rules applied. This is the clean operational data layer supporting fact table creation.

**Source:** `bronze_yellow_trips`  
**Grain:** One row per trip  
**Refresh Strategy:** Full refresh (CREATE OR REPLACE)  
**Row Count (est.):** ~1.5-2.5 million (reduced from Bronze by quality filters)  
**Time Period:** Jan 2015, Jan-Mar 2016

**Purpose:**
- Filter out invalid/anomalous records
- Enforce business logic (geographic bounds, reasonable fares)
- Preserve natural keys for dimensional linking
- Provide stable input for fact table construction

**Key Characteristics:**
- All Bronze columns preserved
- Data quality constraints enforced (see [Data Quality Rules](#data-quality-rules) section)
- No surrogate keys (natural keys only)
- No derived fields

---

#### `fact_trip_silver`

**Definition:** Enriched silver fact table with surrogate trip_id, derived trip_duration_minutes, and standardized column naming (snake_case). Serves as the foundation for dimensional modeling in the Gold layer.

**Source:** `silver_trips`  
**Grain:** One row per trip  
**Refresh Strategy:** Full refresh (CREATE OR REPLACE)  
**Row Count:** Same as `silver_trips`

**Purpose:**
- Introduce surrogate key (`trip_id` = MD5 hash)
- Add derived fields (`trip_duration_minutes`)
- Standardize column naming for dimensional modeling
- Bridge Silver and Gold layers with conformed naming

**Key Characteristics:**
- 25 columns (including new surrogate key + derived field)
- `trip_id` is MD5 hash of: VendorID|pickup|dropoff|coordinates
- `trip_duration_minutes` = DATEDIFF(MINUTE, pickup, dropoff)
- Column names converted to snake_case

**Unique Constraints:**
- `trip_id` should be unique (MD5 hash of composite key)

---

### GOLD LAYER

#### `fact_trip`

**Definition:** Presentation-ready fact table optimized for analytics and BI reporting. Contains surrogate dimension keys, multi-grain time dimensions, base measures, and derived KPIs.

**Source:** `fact_trip_silver` + all dimension tables  
**Grain:** One row per trip  
**Refresh Strategy:** Full refresh (CREATE OR REPLACE)  
**Row Count:** Same as Silver  
**Time Period:** Jan 2015, Jan-Mar 2016

**Purpose:**
- Support analytics queries for taxi operations analysis
- Enable ad-hoc BI reporting and dashboards
- Provide conformed dimensions for consistency
- Offer multiple temporal granularities for flexible aggregation

**Key Characteristics:**
- **28 columns:** 6 dimension FKs + 5 time dimension keys + 11 base measures + 6 derived KPIs
- **Multi-grain Time Dimensions:** Analysts can choose day, hour, or minute granularity
  - `pickup_datetime_day_key` vs `pickup_datetime_hour_key` vs `pickup_datetime_minute_key`
  - `dropoff_datetime_day_key` vs `dropoff_datetime_hour_key` vs `dropoff_datetime_minute_key`
- **Measures:** All financial and distance metrics from Silver
- **KPIs:**
  - `revenue_per_mile` = total_amount / trip_distance
  - `tip_pct` = tip_amount / fare_amount
  - `fare_per_minute` = fare_amount / trip_duration_minutes
  - `fare_per_passenger` = total_amount / passenger_count
  - `effective_rate` = total_amount / trip_duration_minutes
  - `avg_speed_mph` = trip_distance / (trip_duration_minutes / 60)

**Join Logic (all LEFT JOINs to preserve rows):**
```sql
LEFT JOIN dim_vendor ON fact_trip_silver.vendor_id = dim_vendor.vendor_id
LEFT JOIN dim_rate_code ON fact_trip_silver.rate_code_id = dim_rate_code.rate_code_id
LEFT JOIN dim_payment_type ON fact_trip_silver.payment_type_id = dim_payment_type.payment_type_id
LEFT JOIN dim_location pl ON fact_trip_silver.pickup_lat = pl.latitude AND fact_trip_silver.pickup_lon = pl.longitude
LEFT JOIN dim_location dl ON fact_trip_silver.dropoff_lat = dl.latitude AND fact_trip_silver.dropoff_lon = dl.longitude
(+ time dimension joins for day, hour, minute granularities)
```

---

### DIMENSION TABLES

#### `dim_vendor`

**Definition:** Slowly Changing Dimension (Type 1). Lookup table mapping vendor codes to business names.

**Grain:** One row per vendor  
**Row Count:** 2 rows  
**Update Frequency:** Manual updates (Type 1: overwrite)

| Column | Description |
|--------|-------------|
| `vendor_key` | Surrogate key (PK) |
| `vendor_id` | Natural key (1 or 2) |
| `vendor_name` | Business name |

**Sample Data:**
| vendor_key | vendor_id | vendor_name |
|------------|-----------|----------------------|
| 1 | 1 | Creative Mobile Technologies |
| 2 | 2 | VeriFone Inc. |

---

#### `dim_rate_code`

**Definition:** Slowly Changing Dimension (Type 1). Lookup table describing how fares are calculated.

**Grain:** One row per rate code  
**Row Count:** 6 rows  
**Update Frequency:** Manual updates

| Column | Description |
|--------|-------------|
| `rate_code_key` | Surrogate key (PK) |
| `rate_code_id` | Natural key (1-6) |
| `rate_code_desc` | Description of rate category |

**Sample Data:**
| rate_code_key | rate_code_id | rate_code_desc |
|---------------|--------------|------------------|
| 1 | 1 | Standard rate |
| 2 | 2 | JFK |
| 3 | 3 | Newark |
| 4 | 4 | Nassau or Westchester |
| 5 | 5 | Negotiated fare |
| 6 | 6 | Group ride |

---

#### `dim_payment_type`

**Definition:** Slowly Changing Dimension (Type 1). Lookup table for payment methods.

**Grain:** One row per payment type  
**Row Count:** 6 rows  
**Update Frequency:** Manual updates

| Column | Description |
|--------|-------------|
| `payment_type_key` | Surrogate key (PK) |
| `payment_type_id` | Natural key (1-6) |
| `payment_type_desc` | Description of payment method |

**Sample Data:**
| payment_type_key | payment_type_id | payment_type_desc |
|------------------|-----------------|-------------------|
| 1 | 1 | Credit card |
| 2 | 2 | Cash |
| 3 | 3 | No charge |
| 4 | 4 | Dispute |
| 5 | 5 | Unknown |
| 6 | 6 | Voided trip |

---

#### `dim_location`

**Definition:** Junk Dimension combining pickup and dropoff coordinates. Represents all unique geographic locations (lat/lon pairs) serviced by taxis.

**Grain:** One row per unique (latitude, longitude) pair  
**Row Count:** ~20-50k locations (distinct pairs from fact table)  
**Update Frequency:** Full refresh each run

| Column | Description |
|--------|-------------|
| `location_key` | Surrogate key (PK) |
| `latitude` | Y-coordinate (40 to 41 for NYC) |
| `longitude` | X-coordinate (-75 to -73 for NYC) |

**Notes:**
- Derived from UNION of pickup + dropoff locations
- Generated via `ROW_NUMBER() OVER (ORDER BY latitude, longitude)`
- Enables both pickup_location_key and dropoff_location_key foreign keys
- Supports geographic analysis and mapping

---

#### `dim_datetime_day`

**Definition:** Conformed Time Dimension (Day grain). One row per calendar date. Provides daily calendar attributes for temporal aggregation.

**Grain:** One row per date  
**Row Count:** ~121 days (31 days in Jan 2015 + 91 days in Jan-Mar 2016)  
**Update Frequency:** Regenerated for each run

| Column | Description |
|--------|-------------|
| `datetime_day_key` | Surrogate key (PK) |
| `date_value` | Calendar date (YYYY-MM-DD) |
| `year` | Year (INT) |
| `month` | Month (INT, 1-12) |
| `day` | Day of month (INT, 1-31) |
| `day_of_week` | Day of week (INT, 1=Mon...7=Sun) |
| `is_weekend` | Weekend flag (Y/N) |

**Use Cases:**
- Daily trip volume trends
- Seasonal and day-of-week analysis
- Holiday/weekend impact analysis

---

#### `dim_datetime_hour`

**Definition:** Conformed Time Dimension (Hour grain). One row per hour. Enables traffic pattern analysis and peak-hour insights.

**Grain:** One row per hour  
**Row Count:** ~2,904 hours (121 days × 24 hours)  
**Update Frequency:** Regenerated for each run

| Column | Description |
|--------|-------------|
| `datetime_hour_key` | Surrogate key (PK) |
| `datetime_value` | Hour start time (YYYY-MM-DD HH:00:00) |
| `date_value` | Calendar date (derived) |
| `year` | Year |
| `month` | Month |
| `day` | Day of month |
| `hour` | Hour of day (INT, 0-23) |
| `day_of_week` | Day of week |
| `is_weekend` | Weekend flag |

**Use Cases:**
- Peak-hour identification (rush hour patterns)
- Hourly traffic volume trends
- Time-of-day pricing analysis

---

#### `dim_datetime_minute`

**Definition:** Conformed Time Dimension (Minute grain). One row per minute. Provides granular temporal attributes for machine learning features and congestion modeling.

**Grain:** One row per minute  
**Row Count:** ~174,240 minutes (121 days × 1,440 minutes/day)  
**Update Frequency:** Regenerated for each run

| Column | Description |
|--------|-------------|
| `datetime_minute_key` | Surrogate key (PK) |
| `datetime_value` | Exact minute (YYYY-MM-DD HH:mm:00) |
| `date_value` | Calendar date (derived) |
| `year` | Year |
| `month` | Month |
| `day` | Day of month |
| `hour` | Hour of day |
| `minute` | Minute of hour (INT, 0-59) |
| `day_of_week` | Day of week |
| `is_weekend` | Weekend flag |

**Use Cases:**
- ML feature engineering (time-based features)
- Sub-minute congestion modeling
- Real-time traffic pattern detection

---

## Column Definitions

### `bronze_yellow_trips` & `silver_trips` Columns

| Column | Type | Nullable | Description | Role | Validation Rule |
|--------|------|----------|-------------|------|-----------------|
| `VendorID` | BIGINT | N | Vendor identifier | Natural Key | IN (1, 2) |
| `tpep_pickup_datetime` | TIMESTAMP | N | Trip start time | Attribute | NOT NULL |
| `tpep_dropoff_datetime` | TIMESTAMP | N | Trip end time | Attribute | NOT NULL, > pickup |
| `passenger_count` | BIGINT | Y | Passengers in trip | Measure | BETWEEN 1 AND 6 |
| `trip_distance` | DOUBLE | Y | Distance in miles | Measure | BETWEEN 0 AND 100 |
| `pickup_longitude` | DOUBLE | Y | Pickup longitude | Dimension FK | BETWEEN -75 AND -73 |
| `pickup_latitude` | DOUBLE | Y | Pickup latitude | Dimension FK | BETWEEN 40 AND 41 |
| `RateCodeID` | BIGINT | Y | Rate code (1-6) | Natural Key | BETWEEN 1 AND 6 |
| `store_and_fwd_flag` | STRING | Y | Store forward flag | Attribute | IN ('Y', 'N') |
| `dropoff_longitude` | DOUBLE | Y | Dropoff longitude | Dimension FK | BETWEEN -75 AND -73 |
| `dropoff_latitude` | DOUBLE | Y | Dropoff latitude | Dimension FK | BETWEEN 40 AND 41 |
| `payment_type` | BIGINT | Y | Payment method code | Natural Key | BETWEEN 1 AND 6 |
| `fare_amount` | DOUBLE | Y | Base fare USD | Measure | > 0, IS NOT NULL |
| `extra` | DOUBLE | Y | Extra charges USD | Measure | >= 0 |
| `mta_tax` | DOUBLE | Y | MTA tax USD | Measure | >= 0, IS NOT NULL |
| `tip_amount` | DOUBLE | Y | Tip amount USD | Measure | BETWEEN 0 AND 1000 |
| `tolls_amount` | DOUBLE | Y | Tolls USD | Measure | >= 0 |
| `improvement_surcharge` | DOUBLE | Y | Surcharge USD | Measure | >= 0, IS NOT NULL |
| `total_amount` | DOUBLE | Y | Total charged USD | Measure | > 0, IS NOT NULL |

### `fact_trip_silver` Additional Columns

| Column | Type | Description | Role | Notes |
|--------|------|-------------|------|-------|
| `trip_id` | STRING | MD5 hash surrogate key | Surrogate Key | Composite of: VendorID \| pickup \| dropoff \| coordinates |
| `vendor_id` | BIGINT | Vendor identifier | Natural Key | Standardized from VendorID |
| `rate_code_id` | BIGINT | Rate code | Natural Key | Standardized from RateCodeID |
| `payment_type_id` | BIGINT | Payment type | Natural Key | Standardized from payment_type |
| `pickup_datetime` | TIMESTAMP | Trip start time | Attribute | Standardized from tpep_pickup_datetime |
| `dropoff_datetime` | TIMESTAMP | Trip end time | Attribute | Standardized from tpep_dropoff_datetime |
| `trip_duration_minutes` | BIGINT | Duration in minutes | Derived | DATEDIFF(MINUTE, pickup, dropoff) |

### `fact_trip` (Gold Layer) Measure & KPI Columns

| Column | Type | Description | Calculation |
|--------|------|-------------|-------------|
| `revenue_per_mile` | DOUBLE | Revenue efficiency | total_amount / trip_distance |
| `tip_pct` | DOUBLE | Tip percentage of fare | (tip_amount / fare_amount) × 100 |
| `fare_per_minute` | DOUBLE | Fare rate per minute | fare_amount / trip_duration_minutes |
| `fare_per_passenger` | DOUBLE | Average fare per person | total_amount / passenger_count |
| `effective_rate` | DOUBLE | Total rate per minute | total_amount / trip_duration_minutes |
| `avg_speed_mph` | DOUBLE | Average speed | trip_distance / (trip_duration_minutes / 60) |

---

## Data Quality Rules

All data quality filtering is applied at the **Silver layer** (`silver_trips`). Records failing any rule are excluded.

### Filtering Logic (Applied in Silver Layer)

```sql
WHERE VendorID IN (1, 2)
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND passenger_count BETWEEN 1 AND 6
  AND dropoff_longitude BETWEEN -75 AND -73
  AND dropoff_latitude BETWEEN 40 AND 41
  AND pickup_longitude BETWEEN -75 AND -73
  AND pickup_latitude BETWEEN 40 AND 41
  AND trip_distance BETWEEN 0 AND 100
  AND store_and_fwd_flag IN ('N', 'Y')
  AND passenger_count BETWEEN 1 AND 9
  AND payment_type BETWEEN 1 AND 6
  AND fare_amount IS NOT NULL
  AND mta_tax IS NOT NULL
  AND improvement_surcharge IS NOT NULL
  AND total_amount IS NOT NULL
  AND tip_amount BETWEEN 0 AND 1000
```

### Rule Rationale

| Rule | Rationale |
|------|-----------|
| **Vendor IN (1, 2)** | Only two active vendors; rejects any others (data entry errors) |
| **Dropoff > Pickup** | Logical constraint; trip must end after it starts |
| **Passenger BETWEEN 1-6** | Typical NYC taxi capacity (1 person min, 5-6 person cabs max) |
| **Coordinates BETWEEN -75/-73 lon, 40/41 lat** | NYC geographic boundaries; rejects trips outside service area |
| **Trip Distance 0-100 miles** | NYC yellow taxis typically serve <20 miles; 100 is reasonable upper bound |
| **Store/Fwd Flag IN (Y, N)** | Only valid values; rejects nulls or invalid codes |
| **Payment Type 1-6** | Valid payment codes; rejects unknowns |
| **Fares, Tax, Surcharge NOT NULL** | Critical business amounts must be present |
| **Tip Amount 0-1000** | Catches extreme outliers (>$1000 unrealistic) |

### Impact on Rowcount

- **Bronze:** ~2-3 million rows (raw)
- **Silver:** ~1.5-2.5 million rows (~20-30% reduction)
- **Reduction Reason:** Invalid passengers, out-of-bounds coordinates, logic violations (dropoff ≤ pickup)

---

## Lineage & Transformation Logic

### Data Flow Chain

```
SOURCE: data_academy_resources.nyc_taxi.yellow_tripdata
    │
    ├─→ [BRONZE] bronze_yellow_trips
    │   └─ 0 transformations
    │   └─ 19 columns preserved verbatim
    │   └─ Full copy (unfiltered)
    │
    ├─→ [SILVER] silver_trips
    │   └─ Source: bronze_yellow_trips
    │   └─ 19 columns (same as Bronze)
    │   └─ Data quality filters applied (see rules above)
    │   └─ ~20-30% rowcount reduction
    │
    ├─→ [SILVER] fact_trip_silver
    │   └─ Source: silver_trips
    │   └─ +surrogate key: trip_id (MD5 hash)
    │   └─ +derived field: trip_duration_minutes
    │   └─ Column renaming: VendorID → vendor_id, etc. (snake_case)
    │   └─ 25 columns total
    │
    ├─→ [DIMENSIONS] dim_vendor, dim_rate_code, dim_payment_type, dim_location, dim_datetime_* (6 dims)
    │   └─ Source: fact_trip_silver (for location & time coverage)
    │   └─ Values extracted via DISTINCT + ROW_NUMBER
    │   └─ Surrogate keys generated
    │
    └─→ [GOLD] fact_trip
        └─ Source: fact_trip_silver JOIN (all dimensions)
        └─ 28 columns: 6 dimension FKs + 5 time keys + 11 measures + 6 KPIs
        └─ Multi-grain temporal keys for flexibility
        └─ All joins are LEFT JOIN (preserve rows with unknown dims)
```

### Key Transformations

#### Bronze → Silver
- **Filter 1:** Vendor validation (VendorID IN (1, 2))
- **Filter 2:** Logical constraint (dropoff > pickup)
- **Filter 3:** Geographic bounds (lat/lon within NYC)
- **Filter 4:** Sanity checks (passenger count, trip distance, fares)
- **Impact:** ~20-30% row reduction

#### Silver → Silver Fact (fact_trip_silver)
- **Surrogate Key:** `trip_id = MD5(VendorID|pickup|dropoff|coordinates)`
  - Creates stable, unique identifier per trip
  - Resistant to schema changes
  - Reproducible (same input → same hash)
- **Renaming:** CamelCase → snake_case
  - VendorID → vendor_id
  - RateCodeID → rate_code_id
  - tpep_pickup_datetime → pickup_datetime
  - payment_type kept (already snake_case)
- **Derived Field:** `trip_duration_minutes = DATEDIFF(MINUTE, pickup, dropoff)`

#### Silver Fact → Gold Fact (fact_trip) via Joins
- **Dimension Joins:** Natural keys → surrogate keys
  - vendor_id → vendor_key (LEFT JOIN to dim_vendor)
  - rate_code_id → rate_code_key (LEFT JOIN to dim_rate_code)
  - payment_type_id → payment_type_key (LEFT JOIN to dim_payment_type)
  - (lat, lon) → location_key (LEFT JOIN to dim_location, twice for pickup/dropoff)
- **Time Joins:** Timestamps → multi-grain day/hour/minute keys
  - DATE(pickup) → pickup_datetime_day_key (LEFT JOIN to dim_datetime_day)
  - DATE_TRUNC('hour', pickup) → pickup_datetime_hour_key (LEFT JOIN to dim_datetime_hour)
  - DATE_TRUNC('minute', pickup) → pickup_datetime_minute_key (LEFT JOIN to dim_datetime_minute)
  - Same for dropoff_datetime
- **KPI Derivation:** Calculated fields
  - revenue_per_mile = total_amount / trip_distance
  - tip_pct = (tip_amount / fare_amount) * 100
  - (and others from [Column Definitions](#column-definitions))

---

## Grain & Cardinality

### Fact Table Grain

**`fact_trip` Grain:** One row per trip

- **Definition:** Each row represents a single taxi journey with a pickup and dropoff
- **Uniqueness Identifier:** `trip_id` (MD5 surrogate key)
- **Time Periods Covered:**
  - January 2015 (31 days)
  - January - March 2016 (91 days)
  - Total: 122 days of data
- **Expected Row Range:** 1.5 - 2.5 million trips
- **Aggregate Level:** Atomic (lowest level of detail)

### Dimension Table Cardinality

| Dimension | Row Count | Description |
|-----------|-----------|-------------|
| `dim_vendor` | 2 | Two active taxi vendors |
| `dim_rate_code` | 6 | Six rate code categories |
| `dim_payment_type` | 6 | Six payment method options |
| `dim_location` | ~20-50k | Distinct pickup/dropoff (lat, lon) pairs |
| `dim_datetime_day` | ~122 | One per calendar date in data |
| `dim_datetime_hour` | ~2,904 | 122 days × 24 hours |
| `dim_datetime_minute` | ~174,240 | 122 days × 24 hours × 60 minutes |

### Join Cardinality (Fact-to-Dimension)

All relationships are **Many-to-One (M:1)**:

- **Vendor:** 2M trips → 2 vendors (avg ~1.2M trips per vendor)
- **Rate Code:** 2M trips → 6 rate codes (avg ~300k trips per rate)
- **Payment Type:** 2M trips → 6 payment types (avg ~300k per type)
- **Pickup Location:** 2M trips → 20-50k locations (avg 40-100 trips per location)
- **Dropoff Location:** 2M trips → 20-50k locations (same as above)
- **Pickup Day:** 2M trips → ~122 days (avg ~16k trips per day)
- **Pickup Hour:** 2M trips → ~2,904 hours (avg ~700 trips per hour)
- **Pickup Minute:** 2M trips → ~174k minutes (avg ~11 trips per minute)

---

## Data Dictionary Metadata Tables

The data dictionary is documented in three queryable SQL tables:

### `metadata_tables`
Catalog of all warehouse tables with business definitions.

**Query Example:**
```sql
SELECT table_name, layer, description, grain, refresh_strategy
FROM metadata_tables
WHERE layer = 'Silver'
ORDER BY table_name;
```

### `metadata_columns`
Column-level schema with data types, roles, and validation rules.

**Query Example:**
```sql
SELECT table_name, column_name, data_type, column_role, description
FROM metadata_columns
WHERE table_name = 'fact_trip'
ORDER BY column_order;
```

### `metadata_relationships`
Fact-to-dimension relationships with cardinality and join logic.

**Query Example:**
```sql
SELECT source_table, source_column, target_table, target_column, cardinality
FROM metadata_relationships
ORDER BY source_table, target_table;
```

---

## How to Use This Data Warehouse

### For Analysts

1. **Query the fact table:** `SELECT * FROM fact_trip WHERE pickup_datetime_day_key IN (...)`
2. **Choose temporal granularity:** Use `pickup_datetime_day_key` for daily trends, `pickup_datetime_hour_key` for hourly patterns
3. **Join dimensions:** All necessary relations are pre-built; dimensions are already foreign-keyed
4. **Filter by business attributes:** Use dimension tables to filter by vendor name, payment type, rate code, etc.

### For Data Engineers

1. **Run the pipeline:** Execute `00_Runner.ipynb` to refresh all layers
2. **Monitor metadata:** Query `metadata_tables` and `metadata_columns` to track schema changes
3. **Audit lineage:** Check `metadata_relationships` to validate fact-dimension joins
4. **Debug issues:** Start in Bronze for raw data; inspect Silver for cleaned data

### For Governance/Compliance

1. **Audit trail:** Bronze layer preserves raw data unchanged (compliance requirement)
2. **Data quality:** Review filtering rules in Silver layer; monitor rowcount deltas
3. **Schema governance:** Consult `metadata_columns` before making schema changes
4. **Lineage tracking:** Use `metadata_relationships` to map data flows

---

## Refresh & Maintenance

### Refresh Strategy
- **Frequency:** On-demand (triggered by `00_Runner.ipynb`)
- **Method:** Full refresh (CREATE OR REPLACE)
- **Duration:** ~10-15 minutes per full run
- **No Incremental Logic:** Current design does full table recreation; no delta merging

### Tables Affected
1. `bronze_yellow_trips` — Copied from source
2. `silver_trips` — Recreated from Bronze
3. `fact_trip_silver` — Recreated from Silver
4. All 6 dimensions — Recreated from fact_trip_silver
5. `fact_trip` — Recreated from fact_trip_silver + dimensions
6. All 3 metadata tables — Refreshed with current schema

### Monitoring
- Row count should match Bronze (no loss at Silver)
- Silver rowcount should be ~60-80% of Bronze (accounting for filters)
- Fact table rowcount = Silver rowcount
- All dimensions should have expected cardinality (vendor=2, rate_code=6, payment_type=6)

---

### Bronze Layer (Raw Ingestion)
The Bronze table (stg_journey) contains lightly filtered NYC Yellow Taxi trip data.
We applied minimal validation to remove clearly invalid records while preserving the source structure.
This ensures downstream layers receive consistent, usable data without altering the original semantics.

### Silver Layer (Cleaned & Standardized)
The Silver table (fact_trip_silver) refines the Bronze data into a clean, analytics‑ready structure.
We standardized naming conventions, derived key fields (such as trip duration and a unique trip identifier), and enforced data quality rules around coordinates, timestamps, passenger counts, and financial values.
This layer serves as the foundation for dimensional modelling and KPI creation in the Gold layer.

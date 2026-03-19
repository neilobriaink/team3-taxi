# Team 3 Taxi Project

End-to-end Data and AI demonstrator built on Databricks and Spark using NYC Yellow Taxi data.

This repository includes a full analytics and AI flow:
- Data engineering pipeline from source to analytics-ready model
- Dimensional modelling for BI and reporting
- Spark ML fare prediction tracked in MLflow
- Retrieval-augmented generation (RAG) over taxi policy documents

## Table of Contents

1. Project Overview
2. Architecture
3. Repository Structure
4. Environment Setup
5. Databricks Setup
6. Run Guide
7. Expected Outputs
8. ML Workflow Details
9. RAG Workflow Details
10. Validation Checks
11. Troubleshooting
12. Data Source
13. Quality and Security

## 1) Project Overview

The project demonstrates how structured analytics and unstructured knowledge workflows can coexist in one Lakehouse implementation.

High-level capabilities:
- Raw trip ingestion and staging
- Silver refinement and quality filtering
- Gold fact and dimensions for analytical consumption
- Model training and evaluation for fare prediction
- Retrieval and question answering using embedded policy documents

## 2) Architecture

Data and AI layers:

```text
Raw Source Tables
		-> Staging
		-> Silver Fact
		-> Gold Dimensions + Gold Fact
		->
			 a) Analytics / BI queries
			 b) ML training data
			 c) RAG context enrichment
```

Notebook orchestration flow:

```text
00_config
	-> 00_Runner
			-> 01_Create_Staging
			-> 02_Create_Silver_Fact_Table
			-> 03_Create_Dimensions
			-> 04_Create_Gold_Fact_Table

Independent AI workflows:
	-> ml_pipeline/task4-refine
	-> notebooks/05_rag_workflow
```

## 3) Repository Structure

- `notebooks/`
	- `00_config.ipynb`: shared constants, schema setup, source detection
	- `00_Runner.ipynb`: orchestrates core data notebooks
	- `01_Create_Staging.ipynb`: builds staging table
	- `02_Create_Silver_Fact_Table.ipynb`: builds silver fact table
	- `03_Create_Dimensions.ipynb`: builds dimensions
	- `04_Create_Gold_Fact_Table.ipynb`: builds gold fact table
	- `05_rag_workflow.ipynb`: retrieval-augmented generation demo
	- `Manhattan Taxi Hotspots Map.ipynb`: visual exploration notebook
- `ml_pipeline/`
	- `task4-refine.ipynb`: training and evaluation notebook
	- `task4.ipynb`: earlier training variant
	- `Taxi Fare Predictor.ipynb`: model usage demo
	- `how-to-load-taxi-fare-model.md`: model loading guide
	- `model-assessment-report.md`: model quality report
- `data_dictionary/`
	- `DATA_DICTIONARY.md`: schema and lineage documentation
- `requirements.txt`: project dependencies
- `setup.cfg`: flake8 settings
- `tests/`: test folder (currently empty)

## 4) Environment Setup

### Prerequisites

- Python 3.10+
- Git
- Databricks workspace access
- Unity Catalog permissions for target schema

### Clone and Virtual Environment

```bash
git clone <your-repo-url>
cd team3-taxi
python3 -m venv venv
source venv/bin/activate
```

### Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Optional: Local Quality Hooks

```bash
pip install pre-commit
pre-commit install
```

The local hook entries reference `venv/bin/...`, so keep the virtual environment folder name as `venv`.

### Local Lint Commands

```bash
black --check .
flake8 . --exclude=venv,./notebooks/,./ml_pipeline,./data-dictionary
```

## 5) Databricks Setup

Use a cluster/runtime that supports Spark ML and MLflow logging.

Required access:
- Catalog: `students_data`
- Schema: `team3-taxi`

Shared config is centralised in `notebooks/00_config.ipynb`, including:
- `CATALOG`, `SCHEMA`, and fully-qualified table names
- source table selection with fallback
- schema creation if needed

## 6) Run Guide

### Step 1: Configuration

Run `notebooks/00_config.ipynb` first.

Expected behavior:
- selects the catalog and schema
- creates schema if missing
- verifies source table availability
- sets active source (`ACTIVE_SOURCE`)

### Step 2: Core Data Pipeline

Run `notebooks/00_Runner.ipynb`.

It executes in order:
1. `01_Create_Staging.ipynb`
2. `02_Create_Silver_Fact_Table.ipynb`
3. `03_Create_Dimensions.ipynb`
4. `04_Create_Gold_Fact_Table.ipynb`

### Step 3: ML Training and Evaluation

Run `ml_pipeline/task4-refine.ipynb`.

This notebook:
- prepares features
- trains a Spark ML model
- evaluates model metrics
- logs the pipeline/model artifacts to MLflow

Reference docs:
- `ml_pipeline/model-assessment-report.md`
- `ml_pipeline/how-to-load-taxi-fare-model.md`

### Step 4: RAG Demonstration

Run `notebooks/05_rag_workflow.ipynb`.

This notebook:
- creates document chunks
- generates embeddings
- stores embeddings in Delta
- retrieves relevant chunks for user queries
- optionally calls an LLM endpoint for answer generation

Embedding strategy fallback order:
1. Databricks FM embedding endpoint
2. Sentence transformers
3. TF-IDF fallback

## 7) Expected Outputs

Depending on run path, you should see these table families:

Core pipeline tables:
- staging table (`stg_journey`)
- silver fact table (`fact_trip_silver`)
- dimension tables:
	- `dim_vendor`
	- `dim_rate_code`
	- `dim_payment_type`
	- `dim_location`
	- `dim_datetime_day`
	- `dim_datetime_hour`
	- `dim_datetime_minute`
- gold fact table (`fact_trip`)

RAG tables:
- `rag_embeddings`

Config notebook constants also define additional target names used across variants, such as:
- `bronze_trips`, `silver_trips`, `gold_trips`
- `gold_daily_metrics`, `gold_hourly_metrics`
- `rag_documents`, `rag_embeddings`

## 8) ML Workflow Details

The model workflow in `ml_pipeline/task4-refine.ipynb` trains and tracks a Spark pipeline and includes:
- train/test split
- feature assembly/scaling
- model training (random forest and comparison options)
- metric logging and artifact logging to MLflow

For deployment/inference usage examples, see:
- `ml_pipeline/Taxi Fare Predictor.ipynb`
- `ml_pipeline/how-to-load-taxi-fare-model.md`

## 9) RAG Workflow Details

`notebooks/05_rag_workflow.ipynb` demonstrates:
- building a small policy document corpus
- chunking for retrieval granularity
- embedding generation with robust fallback logic
- similarity retrieval and top-k context assembly
- prompt construction and answer generation
- integration of gold-layer KPIs into RAG prompt context

This shows both unstructured retrieval and structured analytics context in one workflow.

## 10) Validation Checks

After running the data pipeline, validate that tables exist and are non-empty:

```sql
SHOW TABLES IN students_data.`team3-taxi`;

SELECT COUNT(*) FROM students_data.`team3-taxi`.fact_trip_silver;
SELECT COUNT(*) FROM students_data.`team3-taxi`.fact_trip;
```

Validate dimensions:

```sql
SELECT COUNT(*) FROM students_data.`team3-taxi`.dim_vendor;
SELECT COUNT(*) FROM students_data.`team3-taxi`.dim_location;
```

Validate RAG embeddings:

```sql
SELECT COUNT(*) FROM students_data.`team3-taxi`.rag_embeddings;
```

## 11) Troubleshooting

### Source table not found

- Run `notebooks/00_config.ipynb` and confirm `ACTIVE_SOURCE` resolves.
- Confirm workspace permissions to primary and fallback sources.

### Schema permission errors

- Verify create/use permissions on catalog `students_data` and schema `team3-taxi`.

### MLflow model logging issues

- Ensure the notebook sets `MLFLOW_DFS_TMP` to a valid volume path when required by cluster security mode.

### RAG endpoint unavailable

- The RAG notebook includes fallback paths.
- If LLM generation endpoint is unavailable, retrieval still works and returns context chunks.

### Pre-commit hook cannot find binaries

- Ensure virtual environment is named `venv` and dependencies are installed inside it.

## 12) Data Source

Primary reference dataset:
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data

Databricks notebooks are configured to use workspace-accessible source tables with fallback handling.

## 13) Quality and Security

- Formatting/lint configuration: `setup.cfg`
- Pre-commit hooks: `.pre-commit-config.yaml`
- CI lint and secret scanning: `.github/workflows/lint.yml`
- Dependency scanning: `.github/workflows/snyk.yml`

## Notes

- The repository is notebook-first and intended for Databricks execution.
- Local unit tests are not implemented yet (`tests/` is currently empty).
- Some ML/RAG capabilities depend on Databricks model serving and endpoint availability.

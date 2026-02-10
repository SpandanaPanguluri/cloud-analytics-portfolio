# Healthcare Analytics — Hospital Outcomes & Quality

## Business Problem
Hospital leadership needs visibility into hospital quality and performance
across facilities to identify improvement opportunities and benchmark outcomes.

## Objective
Analyze hospital-level quality indicators and ratings to:
- Compare performance across hospitals and states
- Identify trends and outliers
- Support data-driven healthcare quality decisions

## Data Source
- CMS Hospital General Information  
  https://data.cms.gov/provider-data/dataset/xubh-q36u

> Note: This repository includes a small sample dataset for demonstration.
> The full dataset can be accessed from the CMS portal.

## Folder Structure
- `data/sample/` – Sample dataset
- `sql/` – KPI queries
- `notebooks/` – Exploratory analysis
- `docs/` – Data dictionary and documentation

## Key Metrics (KPIs)
- Average hospital overall rating
- Distribution of ratings by state
- Hospital type breakdown
- Facilities with missing or low ratings

## Architecture (Cloud-Ready Design)
Raw CSV → Cloud Storage (S3 / Azure Blob) →  
Serverless SQL (Athena / Synapse) → Curated Views → Dashboard

## Tools Used
- SQL (Athena / Synapse compatible)
- Python (pandas)
- Tableau / Power BI (dashboard layer)

# Hospital Analytics Pipeline (Python + DuckDB)

## Overview
End-to-end analytics pipeline using public CMS hospital data.

## What this project demonstrates
- Python data validation & cleaning
- Pandas vs DuckDB performance comparison
- Parquet-based analytics workflow
- SQL-defined healthcare KPIs
- Tableau-ready outputs

## Pipeline Steps
1. Load raw hospital CSV
2. Validate schema and keys
3. Clean and standardize fields
4. Write curated Parquet dimension table
5. Load data into DuckDB warehouse
6. Compute KPIs using SQL
7. Export KPI outputs for Tableau

## How to run
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python pipeline_hospitals.py



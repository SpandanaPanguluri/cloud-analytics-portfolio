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

## Folder Structure
- `data/sample/` – Sample dataset
- `sql/` – KPI queries
- `notebooks/` – Exploratory analysis
- `docs/` – Data dictionary and documentation

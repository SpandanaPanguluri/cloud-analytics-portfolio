# Tableau Data Source — Hospital Quality KPIs

## Description
This data source contains curated hospital quality KPIs aggregated at the
state level for dashboarding and benchmarking.

## Source
Derived from CMS Hospital General Information dataset using Python (pandas).

## Fields
| Field | Description |
|------|------------|
| state | State abbreviation |
| hospital_count | Number of hospitals |
| avg_rating | Average hospital overall rating |
| missing_ratings | Count of missing ratings |

## Refresh Logic
Re-generated via notebook:
`notebooks/01_exploration.ipynb`

d>python pipeline_hospitals.py
[INFO] Loaded raw CSV: C:/Users/spand/Downloads/cloud-analytics-portfolio-main/cloud-analytics-portfolio-main/healthcare-hospital-outcomes/data/sample/Hospital_General_Information.csv rows=5,421 cols=38
[INFO] Cleaned dim_hospital: rows=5,421 cols=39
[INFO] Wrote curated Parquet: data/curated/dim_hospital.parquet

=== Benchmark: Pandas vs DuckDB (Hospitals per State) ===
Pandas time: 0.0122s
DuckDB time: 0.0540s

Top 5 (Pandas):
State  hospital_count
   TX             462
   CA             378
   FL             221
   IL             194
   OH             194

Top 5 (DuckDB):
state  hospital_count
   TX             462
   CA             378
   FL             221
   IL             194
   OH             194
[INFO] Loaded dim_hospital into DuckDB: duckdb/portfolio.duckdb
[INFO] Exported KPI CSVs for Tableau into: outputs/tableau
[INFO] Also exported Parquet KPI output.
[DONE] Pipeline complete.

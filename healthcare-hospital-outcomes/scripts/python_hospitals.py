from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from typing import List, Optional

import duckdb
import pandas as pd


# ---------------------------
# Config
# ---------------------------
@dataclass
class Config:
    raw_csv: str
    curated_parquet: str
    duckdb_path: str
    tableau_out_dir: str


# ---------------------------
# Utilities
# ---------------------------
def require_columns(df: pd.DataFrame, required: List[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}\nFound columns: {list(df.columns)}")


def normalize_text(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def clean_zip(zip_series: pd.Series) -> pd.Series:
    # Keep 5-digit ZIPs where possible; otherwise set NA
    z = zip_series.astype("string").str.extract(r"(\d{5})", expand=False)
    return z.astype("string")


def validate_schema(df: pd.DataFrame) -> None:
    """
    Senior-style validation:
    - Required columns exist
    - Key uniqueness
    - Basic domain checks
    """
    # Adjust if your file uses slightly different column names
    required = ["Facility ID", "Facility Name", "City/Town", "State"]
    require_columns(df, required)

    # Facility ID should exist and be mostly unique
    if df["Facility ID"].isna().any():
        raise ValueError("Facility ID has missing values (cannot be a reliable key).")

    dup = df["Facility ID"].duplicated().sum()
    if dup > 0:
        # Not always fatal, but typically indicates a data issue
        print(f"[WARN] Facility ID has {dup} duplicate rows. Will dedupe by keeping first occurrence.")

    # State should be 2-letter codes after cleaning (some datasets contain full names)
    # We won't fail hard here; we'll flag it.
    bad_state = (~df["State"].astype("string").str.match(r"^[A-Z]{2}$", na=False)).mean()
    if bad_state > 0.05:
        print(f"[WARN] {bad_state:.1%} of State values are not 2-letter codes. You may need a mapping cleanup.")


def clean_hospital_general_info(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize key fields
    df["Facility ID"] = normalize_text(df["Facility ID"])
    df["Facility Name"] = normalize_text(df["Facility Name"])
    df["City/Town"] = normalize_text(df["City/Town"])
    df["State"] = normalize_text(df["State"]).str.upper()

    # Optional columns (handle if present)
    if "ZIP Code" in df.columns:
        df["ZIP Code"] = clean_zip(df["ZIP Code"])

    if "Emergency Services" in df.columns:
        es = normalize_text(df["Emergency Services"]).str.lower()
        df["has_emergency_services"] = es.isin(["yes", "y", "true", "1"])

    # Dedupe by Facility ID
    df = df.drop_duplicates(subset=["Facility ID"], keep="first").reset_index(drop=True)

    return df


def ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


# ---------------------------
# KPIs (SQL)
# ---------------------------
KPI_SQL = """
-- KPI 1: Hospitals per state
CREATE OR REPLACE VIEW v_hospitals_per_state AS
SELECT
  State AS state,
  COUNT(*) AS hospital_count
FROM dim_hospital
GROUP BY 1
ORDER BY hospital_count DESC;

-- KPI 2: Emergency services coverage by state (if column exists)
-- If has_emergency_services is missing, this view will fail; we'll handle that in Python.
CREATE OR REPLACE VIEW v_emergency_coverage_by_state AS
SELECT
  State AS state,
  COUNT(*) AS hospitals,
  SUM(CASE WHEN has_emergency_services THEN 1 ELSE 0 END) AS er_hospitals,
  (SUM(CASE WHEN has_emergency_services THEN 1 ELSE 0 END) * 1.0) / NULLIF(COUNT(*), 0) AS er_pct
FROM dim_hospital
GROUP BY 1
ORDER BY er_pct ASC;
"""


def run_duckdb_kpis(con: duckdb.DuckDBPyConnection, tableau_dir: str) -> None:
    con.execute(KPI_SQL)

    # Export KPI 1
    con.execute(f"""
        COPY (SELECT * FROM v_hospitals_per_state)
        TO '{os.path.join(tableau_dir, "kpi_hospitals_per_state.csv")}'
        (HEADER, DELIMITER ',');
    """)

    # Export KPI 2 only if column exists
    cols = [r[0] for r in con.execute("DESCRIBE dim_hospital").fetchall()]
    if "has_emergency_services" in cols:
        con.execute(f"""
            COPY (SELECT * FROM v_emergency_coverage_by_state)
            TO '{os.path.join(tableau_dir, "kpi_emergency_coverage_by_state.csv")}'
            (HEADER, DELIMITER ',');
        """)
    else:
        print("[INFO] Skipping emergency coverage KPI export (has_emergency_services not found).")


# ---------------------------
# Pandas vs DuckDB benchmark
# ---------------------------
def benchmark_pandas_vs_duckdb(df: pd.DataFrame) -> None:
    print("\n=== Benchmark: Pandas vs DuckDB (Hospitals per State) ===")

    # Pandas
    t0 = time.perf_counter()
    pandas_res = (
        df.groupby("State", as_index=False)
          .size()
          .rename(columns={"size": "hospital_count"})
          .sort_values("hospital_count", ascending=False)
    )
    t1 = time.perf_counter()

    # DuckDB (in-process, same df)
    con = duckdb.connect()
    t2 = time.perf_counter()
    duck_res = con.execute("""
        SELECT State AS state, COUNT(*) AS hospital_count
        FROM df
        GROUP BY 1
        ORDER BY hospital_count DESC
    """).df()
    t3 = time.perf_counter()

    print(f"Pandas time: {t1 - t0:.4f}s")
    print(f"DuckDB time: {t3 - t2:.4f}s")
    print("\nTop 5 (Pandas):")
    print(pandas_res.head(5).to_string(index=False))
    print("\nTop 5 (DuckDB):")
    print(duck_res.head(5).to_string(index=False))


# ---------------------------
# Main
# ---------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Hospital General Information pipeline (CSV -> Parquet -> DuckDB KPIs).")
    ap.add_argument("--raw_csv", default="C:/Users/spand/Downloads/cloud-analytics-portfolio-main/cloud-analytics-portfolio-main/healthcare-hospital-outcomes/data/sample/Hospital_General_Information.csv")
    ap.add_argument("--curated_parquet", default="data/curated/dim_hospital.parquet")
    ap.add_argument("--duckdb_path", default="duckdb/portfolio.duckdb")
    ap.add_argument("--tableau_out_dir", default="outputs/tableau")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    cfg = Config(
        raw_csv=args.raw_csv,
        curated_parquet=args.curated_parquet,
        duckdb_path=args.duckdb_path,
        tableau_out_dir=args.tableau_out_dir,
    )

    ensure_dirs(os.path.dirname(cfg.curated_parquet), os.path.dirname(cfg.duckdb_path), cfg.tableau_out_dir)

    # 1) Load raw data
    df_raw = pd.read_csv(cfg.raw_csv, dtype=str)  # dtype=str avoids silent mixed-type issues
    print(f"[INFO] Loaded raw CSV: {cfg.raw_csv} rows={len(df_raw):,} cols={len(df_raw.columns)}")

    # 2) Validate schema (before/after cleaning)
    validate_schema(df_raw)

    # 3) Clean columns and standardize
    df_clean = clean_hospital_general_info(df_raw)
    print(f"[INFO] Cleaned dim_hospital: rows={len(df_clean):,} cols={len(df_clean.columns)}")

    # 4) Save Parquet (curated)
    df_clean.to_parquet(cfg.curated_parquet, index=False)
    print(f"[INFO] Wrote curated Parquet: {cfg.curated_parquet}")

    # 5) Pandas vs DuckDB benchmark
    benchmark_pandas_vs_duckdb(df_clean)

    # 6) Load into DuckDB warehouse + write KPIs
    con = duckdb.connect(cfg.duckdb_path)
    con.execute("CREATE OR REPLACE TABLE dim_hospital AS SELECT * FROM read_parquet(?)", [cfg.curated_parquet])
    print(f"[INFO] Loaded dim_hospital into DuckDB: {cfg.duckdb_path}")

    run_duckdb_kpis(con, cfg.tableau_out_dir)
    print(f"[INFO] Exported KPI CSVs for Tableau into: {cfg.tableau_out_dir}")

    Export Parquet for Tableau/Power BI (some tools prefer CSV; Parquet is faster)
    con.execute(f"""
        COPY (SELECT * FROM v_hospitals_per_state)
        TO '{os.path.join(cfg.tableau_out_dir, "kpi_hospitals_per_state.parquet")}'
        (FORMAT PARQUET);
    """)
    print("[INFO] Also exported Parquet KPI output.")

    con.close()
    print("[DONE] Pipeline complete.")


if __name__ == "__main__":
    main()



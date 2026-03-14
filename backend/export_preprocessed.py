"""
Export preprocessed DuckDB tables to CSV files in the data/ directory.

Produces:
  roster_enriched.csv          - roster_processing_details + 15 derived columns
  metrics_enriched.csv         - aggregated_operational_metrics + 8 derived columns
  state_summary.csv            - per-state aggregate (TOTAL_ROS, STUCK, FAILURE_RATE, ...)
  org_summary.csv              - per-(org, state) aggregate
  stage_health_summary.csv     - per pipeline stage health aggregate
"""

import os
import sys

# Add backend dir to path so data_loader imports work
sys.path.insert(0, os.path.dirname(__file__))

from data_loader import get_connection

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def export_table(conn, table_name: str, filename: str):
    out_path = os.path.join(DATA_DIR, filename)
    conn.execute(f"COPY {table_name} TO '{out_path}' (HEADER, DELIMITER ',')")
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols = conn.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"].tolist()
    print(f"[export] {filename}: {count} rows × {len(cols)} columns")


def main():
    print("[export] Initializing DuckDB + preprocessing pipeline...")
    conn = get_connection()

    exports = [
        ("roster",               "roster_enriched.csv"),
        ("metrics",              "metrics_enriched.csv"),
        ("state_summary",        "state_summary.csv"),
        ("org_summary",          "org_summary.csv"),
        ("stage_health_summary", "stage_health_summary.csv"),
    ]

    for table, filename in exports:
        try:
            export_table(conn, table, filename)
        except Exception as e:
            print(f"[export] ERROR exporting {table}: {e}")

    print(f"\n[export] All files written to: {DATA_DIR}")


if __name__ == "__main__":
    main()

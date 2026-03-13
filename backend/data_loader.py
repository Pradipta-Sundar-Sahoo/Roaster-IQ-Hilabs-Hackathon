"""Load CSVs into DuckDB in-memory database for fast querying."""

import os
import duckdb
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

# Global DuckDB connection
_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")
        _load_csvs(_conn)
    return _conn


def _load_csvs(conn: duckdb.DuckDBPyConnection):
    roster_path = os.path.join(DATA_DIR, "roster_processing_details.csv")
    metrics_path = os.path.join(DATA_DIR, "aggregated_operational_metrics.csv")

    conn.execute(f"""
        CREATE TABLE roster AS
        SELECT * FROM read_csv_auto('{roster_path}', header=true, ignore_errors=true)
    """)

    conn.execute(f"""
        CREATE TABLE metrics AS
        SELECT * FROM read_csv_auto('{metrics_path}', header=true, ignore_errors=true)
    """)

    # Log table info
    roster_count = conn.execute("SELECT COUNT(*) FROM roster").fetchone()[0]
    metrics_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    print(f"Loaded roster: {roster_count} rows, metrics: {metrics_count} rows")


def query(sql: str) -> pd.DataFrame:
    """Execute SQL and return DataFrame."""
    conn = get_connection()
    return conn.execute(sql).fetchdf()


def get_table_columns(table: str) -> list[str]:
    """Get column names for a table."""
    conn = get_connection()
    result = conn.execute(f"DESCRIBE {table}").fetchdf()
    return result["column_name"].tolist()


def get_table_stats(table: str) -> dict:
    """Get basic stats for a table."""
    conn = get_connection()
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    columns = get_table_columns(table)
    return {"table": table, "row_count": count, "columns": columns}

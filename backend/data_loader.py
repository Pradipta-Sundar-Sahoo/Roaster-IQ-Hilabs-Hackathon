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
    _load_raw_tables(conn)
    _preprocess_tables(conn)
    _print_diagnostics(conn)


def _load_raw_tables(conn: duckdb.DuckDBPyConnection):
    roster_path = os.path.join(DATA_DIR, "roster_processing_details.csv")
    metrics_path = os.path.join(DATA_DIR, "aggregated_operational_metrics.csv")

    conn.execute(f"""
        CREATE TABLE raw_roster AS
        SELECT * FROM read_csv_auto('{roster_path}', header=true, ignore_errors=true)
    """)
    conn.execute(f"""
        CREATE TABLE raw_metrics AS
        SELECT * FROM read_csv_auto('{metrics_path}', header=true, ignore_errors=true)
    """)
    r = conn.execute("SELECT COUNT(*) FROM raw_roster").fetchone()[0]
    m = conn.execute("SELECT COUNT(*) FROM raw_metrics").fetchone()[0]
    print(f"[loader] raw_roster: {r} rows, raw_metrics: {m} rows")


def _preprocess_tables(conn: duckdb.DuckDBPyConnection):
    # --- roster: enriched with precomputed columns ---
    try:
        conn.execute("""
            CREATE TABLE roster AS
            SELECT *,
                -- Priority (references DAYS_STUCK and RED_COUNT computed in inner query)
                CASE
                    WHEN DAYS_STUCK > 90 AND RED_COUNT >= 2 THEN 'critical'
                    WHEN DAYS_STUCK > 30 OR  RED_COUNT >= 2 THEN 'high'
                    WHEN DAYS_STUCK > 7                      THEN 'medium'
                    ELSE 'low'
                END AS PRIORITY,

                CAST(RUN_NO > 1 AS INTEGER) AS IS_RETRY,

                TRIM(SPLIT_PART(REPLACE(LOB, '/', ','), ',', 1)) AS LOB_PRIMARY,

                CASE
                    WHEN FAILURE_STATUS IS NULL OR TRIM(FAILURE_STATUS) = '' THEN 'none'
                    WHEN FAILURE_STATUS ILIKE '%valid%'   OR FAILURE_STATUS ILIKE '%reject%'  THEN 'validation'
                    WHEN FAILURE_STATUS ILIKE '%timeout%' OR FAILURE_STATUS ILIKE '%time%'    THEN 'timeout'
                    WHEN FAILURE_STATUS ILIKE '%process%' OR FAILURE_STATUS ILIKE '%error%'   THEN 'processing'
                    WHEN FAILURE_STATUS ILIKE '%compli%'  OR FAILURE_STATUS ILIKE '%cms%'     THEN 'compliance'
                    ELSE 'other'
                END AS FAILURE_CATEGORY,

                -- Latest Red stage in pipeline order (last stage wins)
                CASE
                    WHEN SPS_LOAD_HEALTH           = 'Red' THEN 'SPS_LOAD'
                    WHEN DART_UI_VALIDATION_HEALTH  = 'Red' THEN 'DART_UI_VALIDATION'
                    WHEN DART_REVIEW_HEALTH         = 'Red' THEN 'DART_REVIEW'
                    WHEN DART_GEN_HEALTH            = 'Red' THEN 'DART_GEN'
                    WHEN ISF_GEN_HEALTH             = 'Red' THEN 'ISF_GEN'
                    WHEN MAPPING_APROVAL_HEALTH     = 'Red' THEN 'MAPPING_APPROVAL'
                    WHEN PRE_PROCESSING_HEALTH      = 'Red' THEN 'PRE_PROCESSING'
                    ELSE NULL
                END AS WORST_HEALTH_STAGE,

                -- Numeric stage index for ordering/filtering
                CASE LATEST_STAGE_NM
                    WHEN 'PRE_PROCESSING'       THEN 0
                    WHEN 'MAPPING_APPROVAL'     THEN 1
                    WHEN 'ISF_GENERATION'       THEN 2
                    WHEN 'DART_GENERATION'      THEN 3
                    WHEN 'DART_REVIEW'          THEN 4
                    WHEN 'DART_UI_VALIDATION'   THEN 5
                    WHEN 'SPS_LOAD'             THEN 6
                    WHEN 'RESOLVED'             THEN 7
                    WHEN 'INGESTION'            THEN -2
                    WHEN 'STOPPED'              THEN -1
                    WHEN 'REJECTED'             THEN -1
                    ELSE NULL
                END AS PIPELINE_STAGE_ORDER

            FROM (
                SELECT *,
                    DATEDIFF('day', FILE_RECEIVED_DT, CURRENT_TIMESTAMP) AS DAYS_STUCK,

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'Red' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'Red' THEN 1 ELSE 0 END) AS RED_COUNT,

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'Yellow' THEN 1 ELSE 0 END) AS YELLOW_COUNT,

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'Green' THEN 2 WHEN PRE_PROCESSING_HEALTH    = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'Green' THEN 2 WHEN MAPPING_APROVAL_HEALTH   = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'Green' THEN 2 WHEN ISF_GEN_HEALTH           = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'Green' THEN 2 WHEN DART_GEN_HEALTH          = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'Green' THEN 2 WHEN DART_REVIEW_HEALTH       = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'Green' THEN 2 WHEN DART_UI_VALIDATION_HEALTH = 'Yellow' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'Green' THEN 2 WHEN SPS_LOAD_HEALTH          = 'Yellow' THEN 1 ELSE 0 END) AS HEALTH_SCORE

                FROM raw_roster
            ) base
        """)
        print("[preprocess] roster enriched successfully")
    except Exception as e:
        print(f"[preprocess] WARNING: roster enrichment failed ({e}) — using raw table")
        conn.execute("DROP TABLE IF EXISTS roster")
        conn.execute("CREATE TABLE roster AS SELECT * FROM raw_roster")

    # --- metrics: enriched with precomputed columns ---
    try:
        conn.execute("""
            CREATE TABLE metrics AS
            SELECT *,
                ROUND(
                    (NEXT_ITER_SCS_CNT - FIRST_ITER_SCS_CNT) * 100.0
                    / NULLIF(FIRST_ITER_SCS_CNT, 0), 4
                ) AS RETRY_LIFT_PCT,

                ROUND(
                    OVERALL_FAIL_CNT * 100.0
                    / NULLIF(OVERALL_SCS_CNT + OVERALL_FAIL_CNT, 0), 4
                ) AS OVERALL_FAIL_RATE,

                ROUND(
                    FIRST_ITER_FAIL_CNT * 100.0
                    / NULLIF(FIRST_ITER_SCS_CNT + FIRST_ITER_FAIL_CNT, 0), 4
                ) AS FIRST_ITER_FAIL_RATE,

                CASE WHEN SCS_PERCENT < 95 THEN 1 ELSE 0 END AS IS_BELOW_SLA,

                -- Parse "MM-YYYY" to a sortable timestamp (MONTH string cannot be sorted directly)
                STRPTIME(MONTH, '%m-%Y') AS MONTH_DATE

            FROM raw_metrics
        """)
        print("[preprocess] metrics enriched successfully")
    except Exception as e:
        print(f"[preprocess] WARNING: metrics enrichment failed ({e}) — using raw table")
        conn.execute("DROP TABLE IF EXISTS metrics")
        conn.execute("CREATE TABLE metrics AS SELECT * FROM raw_metrics")

    # --- state_summary: one row per state ---
    try:
        conn.execute("""
            CREATE TABLE state_summary AS
            SELECT
                CNT_STATE                                                           AS STATE,
                COUNT(*)                                                            AS TOTAL_ROS,
                SUM(IS_STUCK)                                                       AS STUCK_COUNT,
                SUM(IS_FAILED)                                                      AS FAILED_COUNT,
                ROUND(SUM(IS_FAILED) * 100.0 / COUNT(*), 2)                        AS FAILURE_RATE,
                ROUND(AVG(DAYS_STUCK), 1)                                           AS AVG_DAYS_STUCK,
                SUM(CASE WHEN PRIORITY = 'critical' THEN 1 ELSE 0 END)             AS CRITICAL_COUNT,
                SUM(CASE WHEN PRIORITY = 'high'     THEN 1 ELSE 0 END)             AS HIGH_COUNT,
                ROUND(AVG(RED_COUNT), 3)                                            AS AVG_RED_COUNT,
                ROUND(AVG(HEALTH_SCORE), 2)                                         AS AVG_HEALTH_SCORE,
                MODE(FAILURE_CATEGORY)                                              AS TOP_FAILURE_CATEGORY,
                (
                    SELECT ORG_NM FROM roster inner_r
                    WHERE inner_r.CNT_STATE = outer_r.CNT_STATE AND inner_r.IS_FAILED = 1
                    GROUP BY ORG_NM ORDER BY COUNT(*) DESC LIMIT 1
                )                                                                   AS TOP_FAILING_ORG
            FROM roster outer_r
            GROUP BY CNT_STATE
            ORDER BY TOTAL_ROS DESC
        """)
        print("[preprocess] state_summary created")
    except Exception as e:
        print(f"[preprocess] WARNING: state_summary failed ({e})")

    # --- org_summary: one row per (org, state) ---
    try:
        conn.execute("""
            CREATE TABLE org_summary AS
            SELECT
                ORG_NM,
                CNT_STATE,
                COUNT(*)                                                            AS TOTAL_ROS,
                SUM(IS_STUCK)                                                       AS STUCK_COUNT,
                SUM(IS_FAILED)                                                      AS FAILED_COUNT,
                ROUND(SUM(IS_FAILED) * 100.0 / COUNT(*), 2)                        AS FAILURE_RATE,
                ROUND(AVG(RED_COUNT), 3)                                            AS AVG_RED_COUNT,
                ROUND(AVG(HEALTH_SCORE), 2)                                         AS AVG_HEALTH_SCORE,
                SUM(CASE WHEN PRIORITY = 'critical' THEN 1 ELSE 0 END)             AS CRITICAL_COUNT
            FROM roster
            GROUP BY ORG_NM, CNT_STATE
            ORDER BY TOTAL_ROS DESC
        """)
        print("[preprocess] org_summary created")
    except Exception as e:
        print(f"[preprocess] WARNING: org_summary failed ({e})")

    # --- stage_health_summary: one row per pipeline stage ---
    try:
        conn.execute("""
            CREATE TABLE stage_health_summary AS
            SELECT
                LATEST_STAGE_NM                                                     AS STAGE_NM,
                COUNT(*)                                                            AS TOTAL_ROS,
                SUM(RED_COUNT)                                                      AS RED_COUNT_TOTAL,
                SUM(YELLOW_COUNT)                                                   AS YELLOW_COUNT_TOTAL,
                SUM(CASE WHEN RED_COUNT = 0 AND YELLOW_COUNT = 0 THEN 1 ELSE 0 END) AS GREEN_COUNT_TOTAL,
                ROUND(AVG(RED_COUNT), 3)                                            AS AVG_RED_FLAGS,
                SUM(IS_STUCK)                                                       AS STUCK_IN_STAGE
            FROM roster
            GROUP BY LATEST_STAGE_NM
            ORDER BY TOTAL_ROS DESC
        """)
        print("[preprocess] stage_health_summary created")
    except Exception as e:
        print(f"[preprocess] WARNING: stage_health_summary failed ({e})")

    # Drop temp tables
    try:
        conn.execute("DROP TABLE IF EXISTS raw_roster")
        conn.execute("DROP TABLE IF EXISTS raw_metrics")
    except Exception as e:
        print(f"[preprocess] WARNING: could not drop temp tables ({e})")


def _print_diagnostics(conn: duckdb.DuckDBPyConnection):
    tables = ["roster", "metrics", "state_summary", "org_summary", "stage_health_summary"]
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            cols = conn.execute(f"DESCRIBE {t}").fetchdf()["column_name"].tolist()
            print(f"[diagnostics] {t}: {count} rows, {len(cols)} columns")
        except Exception:
            pass


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

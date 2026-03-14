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

    from schema_provider import build_schema_cache
    build_schema_cache(conn)


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
    # ── Step 1: Standardize raw column names ──
    _rename_columns = {
        "AVG_DART_GENERATION_DURATION": "AVG_DART_GEN_DURATION",
        "AVG_ISF_GENERATION_DURATION": "AVG_ISF_GEN_DURATION",
        "AVG_DART_UI_VLDTN_DURATION": "AVG_DART_UI_VALIDATION_DURATION",
    }
    for old_name, new_name in _rename_columns.items():
        try:
            conn.execute(f"ALTER TABLE raw_roster RENAME COLUMN {old_name} TO {new_name}")
        except Exception:
            pass

    # ── Step 2: Normalize categorical values in raw_roster ──
    # Health flags: 'Green'/'Red'/'Yellow' → uppercase
    health_cols = [
        "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
        "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH",
        "SPS_LOAD_HEALTH",
    ]
    for col in health_cols:
        try:
            conn.execute(f"UPDATE raw_roster SET {col} = UPPER({col}) WHERE {col} IS NOT NULL")
        except Exception:
            pass

    # FAILURE_STATUS: uppercase
    conn.execute("UPDATE raw_roster SET FAILURE_STATUS = UPPER(TRIM(FAILURE_STATUS)) WHERE FAILURE_STATUS IS NOT NULL")

    # SRC_SYS: uppercase
    conn.execute("UPDATE raw_roster SET SRC_SYS = UPPER(TRIM(SRC_SYS)) WHERE SRC_SYS IS NOT NULL")

    # FILE_STATUS_CD: cast float → integer
    try:
        conn.execute("ALTER TABLE raw_roster ALTER COLUMN FILE_STATUS_CD TYPE INTEGER USING CAST(FILE_STATUS_CD AS INTEGER)")
    except Exception:
        pass

    # LATEST_STAGE_NM: standardize full names → abbreviated to match column naming
    _stage_renames = {
        "DART_GENERATION": "DART_GEN",
        "ISF_GENERATION": "ISF_GEN",
        "MAPPING_APPROVAL": "MAPPING_APROVAL",
    }
    for old_val, new_val in _stage_renames.items():
        conn.execute(f"UPDATE raw_roster SET LATEST_STAGE_NM = '{new_val}' WHERE LATEST_STAGE_NM = '{old_val}'")

    print("[preprocess] Normalized categorical values (UPPER, stage names, FILE_STATUS_CD)")

    # ── Step 3: Build enriched roster table ──
    try:
        conn.execute("""
            CREATE TABLE roster AS
            SELECT *,
                CASE
                    WHEN DAYS_STUCK > 90 AND RED_COUNT >= 2 THEN 'CRITICAL'
                    WHEN DAYS_STUCK > 30 OR  RED_COUNT >= 2 THEN 'HIGH'
                    WHEN DAYS_STUCK > 7                      THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS PRIORITY,

                CAST(RUN_NO > 1 AS INTEGER) AS IS_RETRY,

                UPPER(TRIM(SPLIT_PART(REPLACE(LOB, '/', ','), ',', 1))) AS LOB_PRIMARY,

                CAST(LOB ILIKE '%MEDICARE%' AS INTEGER) AS HAS_MEDICARE,
                CAST(LOB ILIKE '%MEDICAID%' AS INTEGER) AS HAS_MEDICAID,
                CAST(LOB ILIKE '%COMMERCIAL%' AS INTEGER) AS HAS_COMMERCIAL,
                ARRAY_TO_STRING(LIST_SORT(LIST_DISTINCT(LIST_TRANSFORM(
                    STRING_SPLIT(UPPER(REPLACE(LOB, '/', ',')), ','),
                    x -> CASE
                        WHEN TRIM(x) LIKE '%MEDICARE%'   THEN 'MEDICARE'
                        WHEN TRIM(x) LIKE '%MEDICAID%'   THEN 'MEDICAID'
                        WHEN TRIM(x) LIKE '%COMMERCIAL%' THEN 'COMMERCIAL'
                        WHEN TRIM(x) LIKE '%INDEMNITY%'  THEN 'INDEMNITY'
                        WHEN TRIM(x) LIKE '%UNICARE%'    THEN 'UNICARE'
                        ELSE TRIM(x)
                    END
                ))), ',') AS LOB_CATEGORIES,
                LENGTH(LOB) - LENGTH(REPLACE(LOB, ',', '')) + 1 AS LOB_COUNT,

                CASE
                    WHEN LOB ILIKE '%HMO%' AND LOB ILIKE '%PPO%' THEN 'MIXED'
                    WHEN LOB ILIKE '%HMO%'  THEN 'HMO'
                    WHEN LOB ILIKE '%PPO%'  THEN 'PPO'
                    WHEN LOB ILIKE '%EPO%'  THEN 'EPO'
                    WHEN LOB ILIKE '%FFS%'  THEN 'FFS'
                    WHEN LOB ILIKE '%INDEMNITY%' THEN 'INDEMNITY'
                    ELSE 'UNSPECIFIED'
                END AS LOB_PLAN_TYPE,

                CASE
                    WHEN LOB ILIKE '%MEDICARE%HMO%'  THEN 'HIGHEST'
                    WHEN LOB ILIKE '%MEDICARE%'       THEN 'HIGH'
                    WHEN LOB ILIKE '%MEDICAID%HMO%'  THEN 'MEDIUM_HIGH'
                    WHEN LOB ILIKE '%MEDICAID%'       THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS LOB_COMPLIANCE_RISK,

                CASE
                    WHEN FAILURE_STATUS IS NULL OR TRIM(FAILURE_STATUS) = '' THEN 'NONE'
                    WHEN FAILURE_STATUS ILIKE '%VALID%'   OR FAILURE_STATUS ILIKE '%REJECT%'  THEN 'VALIDATION'
                    WHEN FAILURE_STATUS ILIKE '%TIMEOUT%' OR FAILURE_STATUS ILIKE '%TIME%'    THEN 'TIMEOUT'
                    WHEN FAILURE_STATUS ILIKE '%PROCESS%' OR FAILURE_STATUS ILIKE '%ERROR%'   THEN 'PROCESSING'
                    WHEN FAILURE_STATUS ILIKE '%COMPLI%'  OR FAILURE_STATUS ILIKE '%CMS%'     THEN 'COMPLIANCE'
                    ELSE 'OTHER'
                END AS FAILURE_CATEGORY,

                CASE
                    WHEN SPS_LOAD_HEALTH           = 'RED' THEN 'SPS_LOAD'
                    WHEN DART_UI_VALIDATION_HEALTH = 'RED' THEN 'DART_UI_VALIDATION'
                    WHEN DART_REVIEW_HEALTH         = 'RED' THEN 'DART_REVIEW'
                    WHEN DART_GEN_HEALTH            = 'RED' THEN 'DART_GEN'
                    WHEN ISF_GEN_HEALTH             = 'RED' THEN 'ISF_GEN'
                    WHEN MAPPING_APROVAL_HEALTH     = 'RED' THEN 'MAPPING_APROVAL'
                    WHEN PRE_PROCESSING_HEALTH      = 'RED' THEN 'PRE_PROCESSING'
                    ELSE NULL
                END AS WORST_HEALTH_STAGE,

                CASE LATEST_STAGE_NM
                    WHEN 'PRE_PROCESSING'       THEN 0
                    WHEN 'MAPPING_APROVAL'      THEN 1
                    WHEN 'ISF_GEN'              THEN 2
                    WHEN 'DART_GEN'             THEN 3
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

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'RED' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'RED' THEN 1 ELSE 0 END) AS RED_COUNT,

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'YELLOW' THEN 1 ELSE 0 END) AS YELLOW_COUNT,

                    (CASE WHEN PRE_PROCESSING_HEALTH    = 'GREEN' THEN 2 WHEN PRE_PROCESSING_HEALTH    = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN MAPPING_APROVAL_HEALTH   = 'GREEN' THEN 2 WHEN MAPPING_APROVAL_HEALTH   = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN ISF_GEN_HEALTH           = 'GREEN' THEN 2 WHEN ISF_GEN_HEALTH           = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_GEN_HEALTH          = 'GREEN' THEN 2 WHEN DART_GEN_HEALTH          = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_REVIEW_HEALTH       = 'GREEN' THEN 2 WHEN DART_REVIEW_HEALTH       = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN DART_UI_VALIDATION_HEALTH = 'GREEN' THEN 2 WHEN DART_UI_VALIDATION_HEALTH = 'YELLOW' THEN 1 ELSE 0 END +
                     CASE WHEN SPS_LOAD_HEALTH          = 'GREEN' THEN 2 WHEN SPS_LOAD_HEALTH          = 'YELLOW' THEN 1 ELSE 0 END) AS HEALTH_SCORE

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

                -- Record-level counts
                (OVERALL_SCS_CNT + OVERALL_FAIL_CNT) AS TOT_REC_CNT,

                -- Record-level ratios (PS-aligned: SCS/FAIL/REJ per total records)
                ROUND(
                    OVERALL_SCS_CNT * 100.0
                    / NULLIF(OVERALL_SCS_CNT + OVERALL_FAIL_CNT, 0), 4
                ) AS SCS_REC_RATIO,

                ROUND(
                    OVERALL_FAIL_CNT * 100.0
                    / NULLIF(OVERALL_SCS_CNT + OVERALL_FAIL_CNT, 0), 4
                ) AS FAIL_REC_RATIO,

                -- First-pass rejection rate (records rejected before any retry)
                ROUND(
                    FIRST_ITER_FAIL_CNT * 100.0
                    / NULLIF(FIRST_ITER_SCS_CNT + FIRST_ITER_FAIL_CNT, 0), 4
                ) AS REJ_REC_RATIO,

                -- Retry resolution: how many initially-rejected records were resolved by retries
                ROUND(
                    (NEXT_ITER_SCS_CNT - FIRST_ITER_SCS_CNT) * 100.0
                    / NULLIF(FIRST_ITER_FAIL_CNT, 0), 4
                ) AS RETRY_RESOLUTION_RATE,

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
                SUM(CASE WHEN PRIORITY = 'CRITICAL' THEN 1 ELSE 0 END)             AS CRITICAL_COUNT,
                SUM(CASE WHEN PRIORITY = 'HIGH'     THEN 1 ELSE 0 END)             AS HIGH_COUNT,
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
                SUM(CASE WHEN PRIORITY = 'CRITICAL' THEN 1 ELSE 0 END)             AS CRITICAL_COUNT
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

"""Data query tool — DuckDB queries on both CSVs."""

import pandas as pd
from data_loader import query, get_connection


def query_roster(sql_where: str = None, columns: list = None, limit: int = 100) -> dict:
    """Query the roster table with optional filters."""
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols} FROM roster"
    if sql_where:
        sql += f" WHERE {sql_where}"
    sql += f" LIMIT {limit}"

    df = query(sql)
    return {
        "data": df.to_dict(orient="records"),
        "row_count": len(df),
        "columns": list(df.columns),
    }


def query_metrics(sql_where: str = None, columns: list = None, limit: int = 100) -> dict:
    """Query the metrics table with optional filters."""
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols} FROM metrics"
    if sql_where:
        sql += f" WHERE {sql_where}"
    sql += f" LIMIT {limit}"

    df = query(sql)
    return {
        "data": df.to_dict(orient="records"),
        "row_count": len(df),
        "columns": list(df.columns),
    }


def execute_sql(sql: str) -> dict:
    """Execute arbitrary SQL query (read-only)."""
    sql_lower = sql.strip().lower()
    if any(kw in sql_lower for kw in ["drop", "delete", "insert", "update", "alter", "create"]):
        return {"error": "Only SELECT queries are allowed"}

    df = query(sql)
    return {
        "data": df.to_dict(orient="records"),
        "row_count": len(df),
        "columns": list(df.columns),
    }


def get_stuck_ros() -> dict:
    """Get all stuck ROs with computed days stuck."""
    df = query("""
        SELECT RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, LATEST_STAGE_NM,
               FILE_RECEIVED_DT, FILE_STATUS_CD, IS_FAILED, FAILURE_STATUS,
               PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH, ISF_GEN_HEALTH,
               DART_GEN_HEALTH, DART_REVIEW_HEALTH, DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH,
               DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days_stuck
        FROM roster
        WHERE IS_STUCK = 1
        ORDER BY days_stuck DESC
    """)
    return {"data": df.to_dict(orient="records"), "row_count": len(df)}


def get_failed_ros(state: str = None, org: str = None, limit: int = 50) -> dict:
    """Get failed ROs with optional state/org filter."""
    conditions = ["IS_FAILED = 1"]
    if state:
        conditions.append(f"CNT_STATE = '{state}'")
    if org:
        conditions.append(f"ORG_NM LIKE '%{org}%'")
    where = " AND ".join(conditions)

    df = query(f"""
        SELECT RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, LATEST_STAGE_NM,
               FILE_RECEIVED_DT, FAILURE_STATUS,
               PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH, ISF_GEN_HEALTH,
               DART_GEN_HEALTH, DART_REVIEW_HEALTH, DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH
        FROM roster
        WHERE {where}
        ORDER BY FILE_RECEIVED_DT DESC
        LIMIT {limit}
    """)
    return {"data": df.to_dict(orient="records"), "row_count": len(df)}


def get_failure_stats_by_state() -> dict:
    """Get failure statistics grouped by state."""
    df = query("""
        SELECT CNT_STATE,
               COUNT(*) as total_files,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
        FROM roster
        GROUP BY CNT_STATE
        ORDER BY failure_rate DESC
    """)
    return {"data": df.to_dict(orient="records"), "row_count": len(df)}


def get_health_flag_distribution() -> dict:
    """Get Red/Yellow/Green distribution across all health columns."""
    health_cols = [
        "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
        "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"
    ]
    results = {}
    for col in health_cols:
        df = query(f"""
            SELECT "{col}" as health, COUNT(*) as cnt
            FROM roster
            WHERE "{col}" IS NOT NULL AND "{col}" != ''
            GROUP BY "{col}"
        """)
        results[col] = df.to_dict(orient="records")
    return results


def get_market_trends() -> dict:
    """Get SCS_PERCENT trends by market across months."""
    df = query("""
        SELECT MONTH, MARKET, SCS_PERCENT,
               FIRST_ITER_SCS_CNT, FIRST_ITER_FAIL_CNT,
               NEXT_ITER_SCS_CNT, NEXT_ITER_FAIL_CNT,
               OVERALL_SCS_CNT, OVERALL_FAIL_CNT
        FROM metrics
        ORDER BY MARKET, MONTH
    """)
    return {"data": df.to_dict(orient="records"), "row_count": len(df)}


def get_retry_analysis() -> dict:
    """Analyze retry effectiveness across ROs."""
    df = query("""
        SELECT
            r1.RO_ID,
            r1.ORG_NM,
            r1.CNT_STATE,
            r1.LATEST_STAGE_NM as first_run_stage,
            r1.IS_FAILED as first_run_failed,
            r1.FAILURE_STATUS as first_run_failure,
            r2.RUN_NO as latest_run,
            r2.LATEST_STAGE_NM as latest_run_stage,
            r2.IS_FAILED as latest_run_failed,
            r2.FAILURE_STATUS as latest_run_failure
        FROM roster r1
        JOIN (
            SELECT RO_ID, RUN_NO, LATEST_STAGE_NM, IS_FAILED, FAILURE_STATUS
            FROM roster
            WHERE (RO_ID, RUN_NO) IN (SELECT RO_ID, MAX(RUN_NO) FROM roster WHERE RUN_NO > 1 GROUP BY RO_ID)
        ) r2 ON r1.RO_ID = r2.RO_ID
        WHERE r1.RUN_NO = 1
        LIMIT 200
    """)
    return {"data": df.to_dict(orient="records"), "row_count": len(df)}


def cross_table_state_analysis(state: str) -> dict:
    """Cross-table: correlate CSV1 state failures with CSV2 market SCS%."""
    roster_stats = query(f"""
        SELECT
            COUNT(*) as total_files,
            SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
            ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate,
            SUM(CASE WHEN IS_STUCK = 1 THEN 1 ELSE 0 END) as stuck_files
        FROM roster
        WHERE CNT_STATE = '{state}'
    """)

    market_stats = query(f"""
        SELECT MONTH, SCS_PERCENT,
               FIRST_ITER_SCS_CNT, FIRST_ITER_FAIL_CNT,
               OVERALL_SCS_CNT, OVERALL_FAIL_CNT
        FROM metrics
        WHERE MARKET = '{state}'
        ORDER BY MONTH
    """)

    top_failing_orgs = query(f"""
        SELECT ORG_NM,
               COUNT(*) as total,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failures,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fail_rate
        FROM roster
        WHERE CNT_STATE = '{state}'
        GROUP BY ORG_NM
        ORDER BY failures DESC
        LIMIT 10
    """)

    return {
        "state": state,
        "roster_stats": roster_stats.to_dict(orient="records"),
        "market_trends": market_stats.to_dict(orient="records"),
        "top_failing_orgs": top_failing_orgs.to_dict(orient="records"),
    }

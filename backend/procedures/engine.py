"""Procedure execution engine — runs named diagnostic procedures."""

import pandas as pd
from data_loader import query
from tools.visualizations import (
    create_stuck_tracker,
    create_failure_breakdown,
    create_market_trend,
    create_retry_lift,
    create_health_heatmap,
    create_duration_anomaly,
)


HEALTH_COLUMNS = [
    "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
    "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"
]


def execute_procedure(procedure: dict, params: dict = None) -> dict:
    """Execute a procedure by name and return structured results."""
    name = procedure["name"]
    params = params or {}

    executors = {
        "triage_stuck_ros": _execute_triage,
        "record_quality_audit": _execute_quality_audit,
        "market_health_report": _execute_market_report,
        "retry_effectiveness_analysis": _execute_retry_analysis,
    }

    if name not in executors:
        return {"error": f"No executor for procedure '{name}'"}

    return executors[name](procedure, params)


def _execute_triage(procedure: dict, params: dict) -> dict:
    """Execute triage_stuck_ros procedure."""
    state_filter = params.get("state_filter", procedure.get("parameters", {}).get("state_filter", {}).get("default"))
    include_failed = params.get("include_failed", True)

    # Step 1: Get stuck ROs
    stuck_df = query("""
        SELECT RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, LATEST_STAGE_NM,
               FILE_RECEIVED_DT, FILE_STATUS_CD, IS_FAILED, FAILURE_STATUS,
               PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH, ISF_GEN_HEALTH,
               DART_GEN_HEALTH, DART_REVIEW_HEALTH, DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH,
               DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days_stuck
        FROM roster
        WHERE IS_STUCK = 1
        ORDER BY days_stuck DESC
    """)

    if state_filter:
        stuck_df = stuck_df[stuck_df["CNT_STATE"] == state_filter]

    # Step 2: Count Red health flags
    stuck_df["red_count"] = stuck_df[HEALTH_COLUMNS].apply(
        lambda row: sum(1 for v in row if str(v).upper() == "RED"), axis=1
    )

    def classify(row):
        if row["days_stuck"] > 90 and row["red_count"] >= 2:
            return "CRITICAL"
        elif row["days_stuck"] > 30 or row["red_count"] >= 2:
            return "HIGH"
        elif row["days_stuck"] > 7:
            return "MEDIUM"
        return "LOW"

    stuck_df["priority"] = stuck_df.apply(classify, axis=1)

    # Also get failed ROs if requested
    failed_summary = {}
    if include_failed:
        failed_df = query("""
            SELECT CNT_STATE, FAILURE_STATUS, COUNT(*) as cnt
            FROM roster WHERE IS_FAILED = 1
            GROUP BY CNT_STATE, FAILURE_STATUS
            ORDER BY cnt DESC
            LIMIT 20
        """)
        failed_summary = {
            "total_failed": int(query("SELECT COUNT(*) as c FROM roster WHERE IS_FAILED = 1").iloc[0]["c"]),
            "by_state_and_status": failed_df.to_dict(orient="records"),
        }

    # Step 4: Cross-table - get market context for stuck states
    states = stuck_df["CNT_STATE"].unique().tolist()
    market_context = {}
    if states:
        placeholders = ", ".join(f"'{s}'" for s in states)
        market_df = query(f"""
            SELECT MARKET, MONTH, SCS_PERCENT
            FROM metrics
            WHERE MARKET IN ({placeholders})
            ORDER BY MARKET, MONTH DESC
        """)
        for state in states:
            state_data = market_df[market_df["MARKET"] == state]
            if not state_data.empty:
                market_context[state] = state_data.to_dict(orient="records")

    # Step 5: Generate visualization
    chart = create_stuck_tracker(stuck_df) if not stuck_df.empty else None

    return {
        "procedure": "triage_stuck_ros",
        "stuck_ros": stuck_df.to_dict(orient="records"),
        "stuck_count": len(stuck_df),
        "failed_summary": failed_summary,
        "market_context": market_context,
        "chart": chart,
        "summary": f"Found {len(stuck_df)} stuck ROs. {sum(stuck_df['priority'] == 'CRITICAL')} critical, {sum(stuck_df['priority'] == 'HIGH')} high priority.",
    }


def _execute_quality_audit(procedure: dict, params: dict) -> dict:
    """Execute record_quality_audit procedure."""
    state = params.get("state")
    org = params.get("org")
    threshold = params.get("threshold", 5.0)

    # Build filter
    conditions = []
    if state:
        conditions.append(f"CNT_STATE = '{state}'")
    if org:
        conditions.append(f"ORG_NM LIKE '%{org}%'")
    where = " AND ".join(conditions) if conditions else "1=1"

    # Step 1: Failure stats
    stats_df = query(f"""
        SELECT CNT_STATE, ORG_NM,
               COUNT(*) as total_files,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
        FROM roster
        WHERE {where}
        GROUP BY CNT_STATE, ORG_NM
        ORDER BY failure_rate DESC
        LIMIT 50
    """)

    # Step 2: Red health flag counts
    red_df = query(f"""
        SELECT CNT_STATE, ORG_NM,
               SUM(CASE WHEN PRE_PROCESSING_HEALTH = 'RED' THEN 1 ELSE 0 END) as pre_proc_red,
               SUM(CASE WHEN MAPPING_APROVAL_HEALTH = 'RED' THEN 1 ELSE 0 END) as mapping_red,
               SUM(CASE WHEN ISF_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as isf_red,
               SUM(CASE WHEN DART_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_gen_red,
               SUM(CASE WHEN DART_REVIEW_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_review_red,
               SUM(CASE WHEN DART_UI_VALIDATION_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_ui_red,
               SUM(CASE WHEN SPS_LOAD_HEALTH = 'RED' THEN 1 ELSE 0 END) as sps_red
        FROM roster
        WHERE {where}
        GROUP BY CNT_STATE, ORG_NM
        ORDER BY (pre_proc_red + mapping_red + isf_red + dart_gen_red + dart_review_red + dart_ui_red + sps_red) DESC
        LIMIT 50
    """)

    # Step 3: Failure status breakdown
    failure_df = query(f"""
        SELECT FAILURE_STATUS, COUNT(*) as cnt
        FROM roster
        WHERE IS_FAILED = 1 AND {where}
        GROUP BY FAILURE_STATUS
        ORDER BY cnt DESC
    """)

    # Step 4: Threshold flagging
    flagged = stats_df[stats_df["failure_rate"] > threshold]

    # Step 5: Visualization
    chart = create_failure_breakdown(stats_df, failure_df) if not stats_df.empty else None

    filter_desc = f"state={state}" if state else f"org={org}" if org else "all"
    return {
        "procedure": "record_quality_audit",
        "filter": filter_desc,
        "quality_stats": stats_df.to_dict(orient="records"),
        "red_flag_counts": red_df.to_dict(orient="records"),
        "failure_breakdown": failure_df.to_dict(orient="records"),
        "flagged_above_threshold": flagged.to_dict(orient="records"),
        "threshold": threshold,
        "chart": chart,
        "summary": f"Audited {len(stats_df)} orgs ({filter_desc}). {len(flagged)} exceed {threshold}% failure threshold.",
    }


def _execute_market_report(procedure: dict, params: dict) -> dict:
    """Execute market_health_report procedure."""
    market = params.get("market")
    if not market:
        return {"error": "market parameter is required"}

    # Step 1: Market SCS% trend
    market_df = query(f"""
        SELECT MONTH, MARKET, SCS_PERCENT,
               FIRST_ITER_SCS_CNT, FIRST_ITER_FAIL_CNT,
               NEXT_ITER_SCS_CNT, NEXT_ITER_FAIL_CNT,
               OVERALL_SCS_CNT, OVERALL_FAIL_CNT
        FROM metrics
        WHERE MARKET = '{market}'
        ORDER BY MONTH
    """)

    # Step 2: File-level stats for same state
    file_stats_df = query(f"""
        SELECT
            COUNT(*) as total_files,
            SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
            ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as file_failure_rate,
            SUM(CASE WHEN IS_STUCK = 1 THEN 1 ELSE 0 END) as stuck_files
        FROM roster
        WHERE CNT_STATE = '{market}'
    """)

    # Step 3: Top failing orgs
    top_orgs_df = query(f"""
        SELECT ORG_NM,
               COUNT(*) as total,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failures,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fail_rate
        FROM roster
        WHERE CNT_STATE = '{market}'
        GROUP BY ORG_NM
        ORDER BY failures DESC
        LIMIT 10
    """)

    # Step 4: Failure status distribution
    failure_dist = query(f"""
        SELECT FAILURE_STATUS, COUNT(*) as cnt
        FROM roster
        WHERE CNT_STATE = '{market}' AND IS_FAILED = 1
        GROUP BY FAILURE_STATUS
        ORDER BY cnt DESC
    """)

    # Step 5: Visualization
    chart = create_market_trend(market_df, market) if not market_df.empty else None

    # Correlation analysis
    correlation = "insufficient_data"
    if not market_df.empty and not file_stats_df.empty:
        scs = market_df["SCS_PERCENT"].values
        file_fail = file_stats_df.iloc[0]["file_failure_rate"] if not file_stats_df.empty else 0
        if len(scs) > 1 and scs[-1] < scs[0]:
            correlation = "declining_scs_with_failures"
        elif file_fail > 5:
            correlation = "high_file_failures"
        else:
            correlation = "stable"

    return {
        "procedure": "market_health_report",
        "market": market,
        "market_trends": market_df.to_dict(orient="records"),
        "file_level_stats": file_stats_df.to_dict(orient="records"),
        "top_failing_orgs": top_orgs_df.to_dict(orient="records"),
        "failure_distribution": failure_dist.to_dict(orient="records"),
        "correlation": correlation,
        "chart": chart,
        "summary": f"Market {market}: Latest SCS% = {market_df['SCS_PERCENT'].iloc[-1] if not market_df.empty else 'N/A'}%. File failure rate = {file_stats_df.iloc[0]['file_failure_rate'] if not file_stats_df.empty else 'N/A'}%. Correlation: {correlation}.",
    }


def _execute_retry_analysis(procedure: dict, params: dict) -> dict:
    """Execute retry_effectiveness_analysis procedure."""
    state_filter = params.get("state_filter")

    # Step 1: ROs with retries
    where = f"AND r1.CNT_STATE = '{state_filter}'" if state_filter else ""
    retry_df = query(f"""
        WITH first_runs AS (
            SELECT RO_ID, ORG_NM, CNT_STATE, LATEST_STAGE_NM, IS_FAILED, FAILURE_STATUS
            FROM roster WHERE RUN_NO = 1
        ),
        latest_runs AS (
            SELECT r.RO_ID, r.RUN_NO, r.LATEST_STAGE_NM as retry_stage, r.IS_FAILED as retry_failed, r.FAILURE_STATUS as retry_failure
            FROM roster r
            JOIN (SELECT RO_ID, MAX(RUN_NO) as max_run FROM roster WHERE RUN_NO > 1 GROUP BY RO_ID) m
                ON r.RO_ID = m.RO_ID AND r.RUN_NO = m.max_run
        )
        SELECT r1.RO_ID, r1.ORG_NM, r1.CNT_STATE,
               r1.LATEST_STAGE_NM as first_stage, r1.IS_FAILED as first_failed,
               r2.RUN_NO as retry_count, r2.retry_stage, r2.retry_failed
        FROM first_runs r1
        JOIN latest_runs r2 ON r1.RO_ID = r2.RO_ID
        {where}
        LIMIT 500
    """)

    # Step 2: Compute retry effectiveness
    if not retry_df.empty:
        total_retried = len(retry_df)
        improved = len(retry_df[(retry_df["first_failed"] == 1) & (retry_df["retry_failed"] == 0)])
        still_failed = len(retry_df[(retry_df["first_failed"] == 1) & (retry_df["retry_failed"] == 1)])
        resolved_on_retry = len(retry_df[retry_df["retry_stage"] == "RESOLVED"])
    else:
        total_retried = improved = still_failed = resolved_on_retry = 0

    # Step 3: Market-level retry lift
    market_lift_df = query("""
        SELECT MARKET, MONTH,
               FIRST_ITER_SCS_CNT, NEXT_ITER_SCS_CNT, OVERALL_SCS_CNT,
               ROUND((NEXT_ITER_SCS_CNT - FIRST_ITER_SCS_CNT) * 100.0 / NULLIF(FIRST_ITER_SCS_CNT, 0), 2) as retry_lift_pct
        FROM metrics
        ORDER BY MARKET, MONTH
    """)

    # Step 4: Visualization
    chart = create_retry_lift(market_lift_df) if not market_lift_df.empty else None

    return {
        "procedure": "retry_effectiveness_analysis",
        "total_retried_ros": total_retried,
        "improved_after_retry": improved,
        "still_failed_after_retry": still_failed,
        "resolved_on_retry": resolved_on_retry,
        "effectiveness_rate": round(improved / max(total_retried, 1) * 100, 2),
        "market_retry_lift": market_lift_df.to_dict(orient="records"),
        "sample_retries": retry_df.head(20).to_dict(orient="records"),
        "chart": chart,
        "summary": f"Analyzed {total_retried} retried ROs. {improved} improved ({round(improved/max(total_retried,1)*100,1)}%), {still_failed} still failed. {resolved_on_retry} resolved on retry.",
    }

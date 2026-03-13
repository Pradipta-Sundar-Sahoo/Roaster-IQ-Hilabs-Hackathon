"""Procedure execution engine — runs named diagnostic procedures.

Executors read steps from the procedure JSON so that user modifications
(adding/editing steps) actually change what gets executed.
"""

import pandas as pd
import numpy as np
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
        "generate_pipeline_health_report": _execute_pipeline_health_report,
    }

    if name not in executors:
        return {"error": f"No executor for procedure '{name}'"}

    result = executors[name](procedure, params)
    result["procedure_version"] = procedure.get("version", 1)

    # Execute any custom query steps added by the user
    custom_results = _run_custom_steps(procedure, params)
    if custom_results:
        result["custom_step_results"] = custom_results

    return result


def _run_custom_steps(procedure: dict, params: dict) -> list:
    """Execute any steps added beyond the base procedure steps.

    Base steps are identified by having a known action (query, compute, classify,
    cross_reference, visualize). Custom steps added by users are extra query steps
    that get appended. We detect them by looking for steps beyond the base set
    or steps flagged as custom.
    """
    results = []
    steps = procedure.get("steps", [])

    for i, step in enumerate(steps):
        if step.get("action") != "query":
            continue
        if not step.get("custom"):
            continue

        sql = step.get("sql", "")
        if not sql.strip():
            continue

        # Apply parameter substitution
        for key, val in params.items():
            sql = sql.replace(f"{{{key}}}", str(val))

        try:
            df = query(sql)
            results.append({
                "step_index": i,
                "description": step.get("description", f"Custom step {i}"),
                "data": df.to_dict(orient="records"),
                "row_count": len(df),
            })
        except Exception as e:
            results.append({
                "step_index": i,
                "description": step.get("description", f"Custom step {i}"),
                "error": str(e),
            })

    return results


def _get_step_sql(procedure: dict, step_action: str, default_sql: str) -> str:
    """Get SQL from a procedure step by action name, falling back to default."""
    for step in procedure.get("steps", []):
        if step.get("action") == "query" and step_action in step.get("description", "").lower():
            return step.get("sql", default_sql)
    return default_sql


def _get_param(procedure: dict, params: dict, key: str, fallback=None):
    """Get a parameter from user params, falling back to procedure defaults."""
    if key in params:
        return params[key]
    proc_params = procedure.get("parameters", {})
    if key in proc_params:
        return proc_params[key].get("default", fallback)
    return fallback


def _execute_triage(procedure: dict, params: dict) -> dict:
    """Execute triage_stuck_ros procedure — reads steps from JSON."""
    state_filter = _get_param(procedure, params, "state_filter") or params.get("state") or params.get("market")
    include_failed = _get_param(procedure, params, "include_failed", True)

    base_sql = _get_step_sql(procedure, "stuck", """
        SELECT RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, LATEST_STAGE_NM,
               FILE_RECEIVED_DT, FILE_STATUS_CD, IS_FAILED, FAILURE_STATUS,
               PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH, ISF_GEN_HEALTH,
               DART_GEN_HEALTH, DART_REVIEW_HEALTH, DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH,
               DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days_stuck
        FROM roster
        WHERE IS_STUCK = 1
        ORDER BY days_stuck DESC
    """)

    stuck_df = query(base_sql)

    if state_filter:
        stuck_df = stuck_df[stuck_df["CNT_STATE"] == state_filter]

    # Read classify rules from procedure if available
    classify_rules = None
    for step in procedure.get("steps", []):
        if step.get("action") == "classify" and step.get("rules"):
            classify_rules = step["rules"]
            break

    stuck_df["red_count"] = stuck_df[HEALTH_COLUMNS].apply(
        lambda row: sum(1 for v in row if str(v).upper() == "RED"), axis=1
    )

    if classify_rules:
        def classify_dynamic(row):
            for level in ["critical", "high", "medium", "low"]:
                rule = classify_rules.get(level, "")
                if "days_stuck > 90" in rule and "red_count >= 2" in rule:
                    if row["days_stuck"] > 90 and row["red_count"] >= 2:
                        return level.upper()
                elif "days_stuck > 30" in rule:
                    if row["days_stuck"] > 30 or row["red_count"] >= 2:
                        return level.upper()
                elif "days_stuck > 7" in rule:
                    if row["days_stuck"] > 7:
                        return level.upper()
            return "LOW"
        stuck_df["priority"] = stuck_df.apply(classify_dynamic, axis=1)
    else:
        def classify(row):
            if row["days_stuck"] > 90 and row["red_count"] >= 2:
                return "CRITICAL"
            elif row["days_stuck"] > 30 or row["red_count"] >= 2:
                return "HIGH"
            elif row["days_stuck"] > 7:
                return "MEDIUM"
            return "LOW"
        stuck_df["priority"] = stuck_df.apply(classify, axis=1)

    failed_summary = {}
    if include_failed:
        failed_sql = _get_step_sql(procedure, "failed", """
            SELECT CNT_STATE, FAILURE_STATUS, COUNT(*) as cnt
            FROM roster WHERE IS_FAILED = 1
            GROUP BY CNT_STATE, FAILURE_STATUS
            ORDER BY cnt DESC LIMIT 20
        """)
        failed_df = query(failed_sql)
        failed_summary = {
            "total_failed": int(query("SELECT COUNT(*) as c FROM roster WHERE IS_FAILED = 1").iloc[0]["c"]),
            "by_state_and_status": failed_df.to_dict(orient="records"),
        }

    states = stuck_df["CNT_STATE"].unique().tolist()
    market_context = {}
    if states:
        placeholders = ", ".join(f"'{s}'" for s in states)
        market_df = query(f"""
            SELECT MARKET, MONTH, SCS_PERCENT
            FROM metrics WHERE MARKET IN ({placeholders})
            ORDER BY MARKET, MONTH DESC
        """)
        for state in states:
            state_data = market_df[market_df["MARKET"] == state]
            if not state_data.empty:
                market_context[state] = state_data.to_dict(orient="records")

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
    """Execute record_quality_audit procedure — reads steps from JSON."""
    state = _get_param(procedure, params, "state")
    org = _get_param(procedure, params, "org")
    threshold = _get_param(procedure, params, "threshold", 5.0)

    conditions = []
    if state:
        conditions.append(f"CNT_STATE = '{state}'")
    if org:
        conditions.append(f"ORG_NM LIKE '%{org}%'")
    where = " AND ".join(conditions) if conditions else "1=1"

    stats_sql = _get_step_sql(procedure, "failure", f"""
        SELECT CNT_STATE, ORG_NM,
               COUNT(*) as total_files,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
        FROM roster WHERE {where}
        GROUP BY CNT_STATE, ORG_NM
        ORDER BY failure_rate DESC LIMIT 50
    """)
    stats_df = query(stats_sql)

    red_sql = _get_step_sql(procedure, "red health", f"""
        SELECT CNT_STATE, ORG_NM,
               SUM(CASE WHEN PRE_PROCESSING_HEALTH = 'RED' THEN 1 ELSE 0 END) as pre_proc_red,
               SUM(CASE WHEN MAPPING_APROVAL_HEALTH = 'RED' THEN 1 ELSE 0 END) as mapping_red,
               SUM(CASE WHEN ISF_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as isf_red,
               SUM(CASE WHEN DART_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_gen_red,
               SUM(CASE WHEN DART_REVIEW_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_review_red,
               SUM(CASE WHEN DART_UI_VALIDATION_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_ui_red,
               SUM(CASE WHEN SPS_LOAD_HEALTH = 'RED' THEN 1 ELSE 0 END) as sps_red
        FROM roster WHERE {where}
        GROUP BY CNT_STATE, ORG_NM
        ORDER BY (pre_proc_red + mapping_red + isf_red + dart_gen_red + dart_review_red + dart_ui_red + sps_red) DESC
        LIMIT 50
    """)
    red_df = query(red_sql)

    failure_df = query(f"""
        SELECT FAILURE_STATUS, COUNT(*) as cnt
        FROM roster WHERE IS_FAILED = 1 AND {where}
        GROUP BY FAILURE_STATUS ORDER BY cnt DESC
    """)

    flagged = stats_df[stats_df["failure_rate"] > threshold]
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
    """Execute market_health_report procedure — reads steps from JSON."""
    market = _get_param(procedure, params, "market")
    if not market:
        return {"error": "market parameter is required"}

    trend_sql = _get_step_sql(procedure, "trend", f"""
        SELECT MONTH, MARKET, SCS_PERCENT,
               FIRST_ITER_SCS_CNT, FIRST_ITER_FAIL_CNT,
               NEXT_ITER_SCS_CNT, NEXT_ITER_FAIL_CNT,
               OVERALL_SCS_CNT, OVERALL_FAIL_CNT
        FROM metrics WHERE MARKET = '{market}' ORDER BY MONTH
    """)
    market_df = query(trend_sql)

    file_stats_df = query(f"""
        SELECT COUNT(*) as total_files,
            SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_files,
            ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as file_failure_rate,
            SUM(CASE WHEN IS_STUCK = 1 THEN 1 ELSE 0 END) as stuck_files
        FROM roster WHERE CNT_STATE = '{market}'
    """)

    top_orgs_df = query(f"""
        SELECT ORG_NM, COUNT(*) as total,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failures,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fail_rate
        FROM roster WHERE CNT_STATE = '{market}'
        GROUP BY ORG_NM ORDER BY failures DESC LIMIT 10
    """)

    failure_dist = query(f"""
        SELECT FAILURE_STATUS, COUNT(*) as cnt
        FROM roster WHERE CNT_STATE = '{market}' AND IS_FAILED = 1
        GROUP BY FAILURE_STATUS ORDER BY cnt DESC
    """)

    chart = create_market_trend(market_df, market) if not market_df.empty else None

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
    """Execute retry_effectiveness_analysis procedure — reads steps from JSON."""
    state_filter = _get_param(procedure, params, "state_filter") or params.get("state") or params.get("market")

    where = f"WHERE r1.CNT_STATE = '{state_filter}'" if state_filter else ""
    retry_sql = _get_step_sql(procedure, "retr", f"""
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
        {where} LIMIT 500
    """)
    retry_df = query(retry_sql)

    if not retry_df.empty:
        total_retried = len(retry_df)
        cols = retry_df.columns.tolist()
        first_fail_col = "first_failed" if "first_failed" in cols else "IS_FAILED"
        retry_fail_col = "retry_failed" if "retry_failed" in cols else "IS_FAILED"
        retry_stage_col = "retry_stage" if "retry_stage" in cols else "LATEST_STAGE_NM"
        improved = len(retry_df[(retry_df[first_fail_col] == 1) & (retry_df[retry_fail_col] == 0)])
        still_failed = len(retry_df[(retry_df[first_fail_col] == 1) & (retry_df[retry_fail_col] == 1)])
        resolved_on_retry = len(retry_df[retry_df[retry_stage_col] == "RESOLVED"])
    else:
        total_retried = improved = still_failed = resolved_on_retry = 0

    market_lift_df = query("""
        SELECT MARKET, MONTH,
               FIRST_ITER_SCS_CNT, NEXT_ITER_SCS_CNT, OVERALL_SCS_CNT,
               ROUND((NEXT_ITER_SCS_CNT - FIRST_ITER_SCS_CNT) * 100.0 / NULLIF(FIRST_ITER_SCS_CNT, 0), 2) as retry_lift_pct
        FROM metrics ORDER BY MARKET, MONTH
    """)

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


def _execute_pipeline_health_report(procedure: dict, params: dict) -> dict:
    """Generate a comprehensive pipeline health report."""
    state = params.get("state")
    org = params.get("org")
    lob = params.get("lob")
    source_system = params.get("source_system")

    conditions = []
    if state:
        conditions.append(f"CNT_STATE = '{state}'")
    if org:
        conditions.append(f"ORG_NM LIKE '%{org}%'")
    if lob:
        conditions.append(f"LOB = '{lob}'")
    if source_system:
        conditions.append(f"SRC_SYS = '{source_system}'")
    where = " AND ".join(conditions) if conditions else "1=1"

    filter_desc_parts = []
    if state:
        filter_desc_parts.append(f"state={state}")
    if org:
        filter_desc_parts.append(f"org={org}")
    if lob:
        filter_desc_parts.append(f"lob={lob}")
    if source_system:
        filter_desc_parts.append(f"src={source_system}")
    filter_desc = ", ".join(filter_desc_parts) if filter_desc_parts else "all data"

    # ── 1. Summary Statistics ──
    stats_df = query(f"""
        SELECT
            COUNT(*) as total_ros,
            SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END) as failed_ros,
            SUM(CASE WHEN IS_STUCK=1 THEN 1 ELSE 0 END) as stuck_ros,
            SUM(CASE WHEN IS_RETRY=1 THEN 1 ELSE 0 END) as retry_ros,
            ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as failure_rate,
            ROUND(SUM(CASE WHEN IS_STUCK=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as stuck_rate,
            SUM(CASE WHEN PRIORITY='CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN PRIORITY='HIGH' THEN 1 ELSE 0 END) as high_count,
            ROUND(AVG(HEALTH_SCORE), 2) as avg_health_score,
            ROUND(AVG(RED_COUNT), 2) as avg_red_count,
            ROUND(AVG(DAYS_STUCK), 1) as avg_days_stuck
        FROM roster WHERE {where}
    """)
    raw_stats = stats_df.iloc[0].to_dict() if not stats_df.empty else {}
    summary_stats = {k: (0 if pd.isna(v) else v) for k, v in raw_stats.items()}

    # ── 2. Flagged ROs (critical + high priority) ──
    flagged_df = query(f"""
        SELECT RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, LATEST_STAGE_NM,
               PRIORITY, DAYS_STUCK, RED_COUNT, WORST_HEALTH_STAGE,
               IS_FAILED, IS_STUCK, FAILURE_CATEGORY, FAILURE_STATUS
        FROM roster
        WHERE {where} AND (PRIORITY IN ('CRITICAL', 'HIGH') OR IS_STUCK=1 OR IS_FAILED=1)
        ORDER BY
            CASE PRIORITY WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
            DAYS_STUCK DESC
        LIMIT 50
    """)

    # ── 3. Stage Bottlenecks ──
    stage_df = query(f"""
        SELECT LATEST_STAGE_NM as stage,
               COUNT(*) as total,
               SUM(CASE WHEN IS_STUCK=1 THEN 1 ELSE 0 END) as stuck,
               SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END) as failed,
               ROUND(AVG(RED_COUNT), 2) as avg_red_flags,
               ROUND(AVG(DAYS_STUCK), 1) as avg_days_stuck
        FROM roster WHERE {where}
        GROUP BY LATEST_STAGE_NM
        ORDER BY stuck DESC, failed DESC
    """)

    stage_meanings = {
        "PRE_PROCESSING": "Intake/format parsing — issues here indicate source file structural problems",
        "MAPPING_APROVAL": "Provider mapping review — issues here indicate data quality requiring manual review",
        "ISF_GEN": "Initial source file generation — issues here indicate transformation pipeline failures",
        "DART_GEN": "Provider data transformation — issues here indicate record-level processing failures",
        "DART_REVIEW": "Data review — issues here indicate validation rules catching errors",
        "DART_UI_VALIDATION": "UI validation — issues here indicate human reviewers finding data issues",
        "SPS_LOAD": "Final system-of-record load — issues here indicate downstream delivery failure",
        "RESOLVED": "Successfully completed all stages",
        "INGESTION": "Initial file intake",
        "STOPPED": "Processing halted by operator or system",
        "REJECTED": "File rejected during intake validation",
    }
    bottlenecks = []
    for _, row in stage_df.iterrows():
        stage = str(row["stage"])
        clean_row = {k: (0 if pd.isna(v) else v) for k, v in row.to_dict().items()}
        bottlenecks.append({
            **clean_row,
            "interpretation": stage_meanings.get(stage, f"Pipeline stage: {stage}"),
        })

    # ── 4. Derived Health Metrics (per-stage RED/YELLOW/GREEN) ──
    health_dist = query(f"""
        SELECT
            SUM(CASE WHEN PRE_PROCESSING_HEALTH='RED' THEN 1 ELSE 0 END) as pre_proc_red,
            SUM(CASE WHEN MAPPING_APROVAL_HEALTH='RED' THEN 1 ELSE 0 END) as mapping_red,
            SUM(CASE WHEN ISF_GEN_HEALTH='RED' THEN 1 ELSE 0 END) as isf_red,
            SUM(CASE WHEN DART_GEN_HEALTH='RED' THEN 1 ELSE 0 END) as dart_gen_red,
            SUM(CASE WHEN DART_REVIEW_HEALTH='RED' THEN 1 ELSE 0 END) as dart_review_red,
            SUM(CASE WHEN DART_UI_VALIDATION_HEALTH='RED' THEN 1 ELSE 0 END) as dart_ui_red,
            SUM(CASE WHEN SPS_LOAD_HEALTH='RED' THEN 1 ELSE 0 END) as sps_red
        FROM roster WHERE {where}
    """)
    raw_health = health_dist.iloc[0].to_dict() if not health_dist.empty else {}
    health_metrics = {k: (0 if pd.isna(v) else v) for k, v in raw_health.items()}

    failure_cats = query(f"""
        SELECT FAILURE_CATEGORY, COUNT(*) as count
        FROM roster WHERE IS_FAILED=1 AND {where}
        GROUP BY FAILURE_CATEGORY ORDER BY count DESC
    """)
    failure_explanations = {
        "VALIDATION": "Records failed schema or format validation — check source system output format",
        "TIMEOUT": "Processing exceeded time limits — may indicate resource constraints or unusually large files",
        "PROCESSING": "Generic processing errors during transformation stages",
        "COMPLIANCE": "Regulatory compliance checks failed — review against CMS/state requirements",
        "OTHER": "Unclassified failures requiring manual investigation",
        "NONE": "No specific failure category assigned",
    }
    failure_breakdown = []
    for _, row in failure_cats.iterrows():
        cat = str(row["FAILURE_CATEGORY"])
        failure_breakdown.append({
            "category": cat,
            "count": int(row["count"]),
            "explanation": failure_explanations.get(cat, f"Failure category: {cat}"),
        })

    # ── 5. Market Context ──
    market_context = {}
    target_states = [state] if state else []
    if not target_states:
        top_states = query(f"""
            SELECT CNT_STATE, COUNT(*) as cnt
            FROM roster WHERE {where}
            GROUP BY CNT_STATE ORDER BY cnt DESC LIMIT 5
        """)
        target_states = top_states["CNT_STATE"].tolist() if not top_states.empty else []

    if target_states:
        placeholders = ", ".join(f"'{s}'" for s in target_states)
        market_df = query(f"""
            SELECT MARKET, MONTH, SCS_PERCENT, RETRY_LIFT_PCT,
                   FIRST_ITER_SCS_CNT, FIRST_ITER_FAIL_CNT,
                   NEXT_ITER_SCS_CNT, NEXT_ITER_FAIL_CNT,
                   OVERALL_SCS_CNT, OVERALL_FAIL_CNT
            FROM metrics WHERE MARKET IN ({placeholders})
            ORDER BY MARKET, MONTH_DATE DESC
        """)
        for s in target_states:
            s_data = market_df[market_df["MARKET"] == s]
            if not s_data.empty:
                latest = s_data.iloc[0]
                market_context[s] = {
                    "latest_scs": float(latest["SCS_PERCENT"]),
                    "latest_month": str(latest["MONTH"]),
                    "latest_retry_lift": float(latest["RETRY_LIFT_PCT"]) if pd.notna(latest.get("RETRY_LIFT_PCT")) else None,
                    "trend": s_data.head(6).to_dict(orient="records"),
                }

    # ── 6. Retry Effectiveness ──
    retry_eff = {}
    try:
        retry_stats = query(f"""
            SELECT
                SUM(CASE WHEN IS_RETRY=1 THEN 1 ELSE 0 END) as total_retries,
                SUM(CASE WHEN IS_RETRY=1 AND IS_FAILED=0 THEN 1 ELSE 0 END) as retry_successes,
                SUM(CASE WHEN IS_RETRY=1 AND IS_FAILED=1 THEN 1 ELSE 0 END) as retry_failures,
                SUM(CASE WHEN RUN_NO=1 AND IS_FAILED=1 THEN 1 ELSE 0 END) as first_run_failures
            FROM roster WHERE {where}
        """)
        rs = retry_stats.iloc[0]
        total_retries = int(rs["total_retries"])
        retry_eff = {
            "total_retries": total_retries,
            "retry_successes": int(rs["retry_successes"]),
            "retry_failures": int(rs["retry_failures"]),
            "first_run_failures": int(rs["first_run_failures"]),
            "retry_success_rate": round(float(rs["retry_successes"]) / max(total_retries, 1) * 100, 1),
        }
    except Exception:
        pass

    # ── 7. Recommended Actions ──
    recommended_actions = []
    total = float(summary_stats.get("total_ros", 0))
    crit = int(summary_stats.get("critical_count", 0))
    fail_rate = float(summary_stats.get("failure_rate", 0))

    if crit > 0:
        recommended_actions.append({
            "priority": 1,
            "action": f"Triage {crit} critical-priority ROs immediately",
            "procedure": "triage_stuck_ros",
            "params": {"state_filter": state} if state else {},
            "reason": f"{crit} ROs have CRITICAL priority (>90 days stuck with 2+ RED flags)",
        })

    if fail_rate > 5:
        recommended_actions.append({
            "priority": 2,
            "action": f"Investigate {fail_rate}% failure rate — exceeds 5% threshold",
            "procedure": "record_quality_audit",
            "params": {"state": state} if state else {},
            "reason": "Failure rate above acceptable threshold, detailed audit recommended",
        })

    for s, ctx in market_context.items():
        if ctx.get("latest_scs", 100) < 95:
            recommended_actions.append({
                "priority": 3,
                "action": f"Market {s} SCS at {ctx['latest_scs']}% — below 95% SLA",
                "procedure": "market_health_report",
                "params": {"market": s},
                "reason": "Market success rate below SLA threshold",
            })

    retry_rate = retry_eff.get("retry_success_rate", 100)
    if retry_rate < 50 and retry_eff.get("total_retries", 0) > 0:
        recommended_actions.append({
            "priority": 4,
            "action": f"Retry success rate only {retry_rate}% — investigate retry strategy",
            "procedure": "retry_effectiveness_analysis",
            "params": {"state_filter": state} if state else {},
            "reason": "Low retry success rate suggests retries may not be effective for current failure types",
        })

    if not recommended_actions:
        recommended_actions.append({
            "priority": 5,
            "action": "No immediate action required — pipeline is operating within thresholds",
            "procedure": None,
            "params": {},
            "reason": "All key metrics are within acceptable ranges",
        })

    # ── 8. Charts ──
    charts = []

    if not stage_df.empty:
        fig_data = stage_df.copy()
        fig_data.columns = [str(c) for c in fig_data.columns]
        try:
            import plotly.graph_objects as pgo
            import json as _json
            fig = pgo.Figure()
            fig.add_trace(pgo.Bar(name="Stuck", x=fig_data["stage"], y=fig_data["stuck"], marker_color="#ef4444"))
            fig.add_trace(pgo.Bar(name="Failed", x=fig_data["stage"], y=fig_data["failed"], marker_color="#f59e0b"))
            fig.add_trace(pgo.Bar(name="Total", x=fig_data["stage"], y=fig_data["total"], marker_color="#6366f1"))
            fig.update_layout(
                title=f"Pipeline Stage Distribution ({filter_desc})",
                barmode="group", template="plotly_white",
                xaxis_title="Stage", yaxis_title="RO Count",
            )
            charts.append(_json.loads(fig.to_json()))
        except Exception:
            pass

    try:
        heatmap_where = f"WHERE CNT_STATE = '{state}'" if state else ""
        heatmap_df = query(f"""
            SELECT ORG_NM, PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH,
                   ISF_GEN_HEALTH, DART_GEN_HEALTH, DART_REVIEW_HEALTH,
                   DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH
            FROM roster {heatmap_where} LIMIT 25
        """)
        heatmap_chart = create_health_heatmap(heatmap_df)
        if heatmap_chart:
            charts.append(heatmap_chart)
    except Exception:
        pass

    # ── Build narrative summary ──
    total_ros = int(summary_stats.get("total_ros", 0))
    failed_ros = int(summary_stats.get("failed_ros", 0))
    stuck_ros = int(summary_stats.get("stuck_ros", 0))

    health_rating = "HEALTHY"
    if fail_rate > 15 or float(summary_stats.get("stuck_rate", 0)) > 20:
        health_rating = "CRITICAL"
    elif fail_rate > 8 or float(summary_stats.get("stuck_rate", 0)) > 10:
        health_rating = "DEGRADED"
    elif fail_rate > 3:
        health_rating = "WARNING"

    narrative = (
        f"## Pipeline Health Report ({filter_desc})\n\n"
        f"**Overall Status: {health_rating}**\n\n"
        f"- **{total_ros:,}** total ROs processed\n"
        f"- **{failed_ros:,}** failed ({fail_rate}%)\n"
        f"- **{stuck_ros:,}** stuck ({summary_stats.get('stuck_rate', 0)}%)\n"
        f"- **{crit}** critical priority\n"
        f"- Average health score: **{summary_stats.get('avg_health_score', 'N/A')}**\n\n"
    )

    if bottlenecks:
        worst = bottlenecks[0]
        narrative += f"**Top Bottleneck:** {worst['stage']} with {worst['stuck']} stuck ROs. "
        narrative += f"{worst['interpretation']}\n\n"

    if failure_breakdown:
        top_failure = failure_breakdown[0]
        narrative += f"**Primary Failure Type:** {top_failure['category']} ({top_failure['count']} ROs). "
        narrative += f"{top_failure['explanation']}\n\n"

    if recommended_actions:
        narrative += f"**{len(recommended_actions)} Recommended Action(s):**\n"
        for ra in recommended_actions:
            narrative += f"  {ra['priority']}. {ra['action']}\n"

    result = {
        "procedure": "generate_pipeline_health_report",
        "filter": filter_desc,
        "narrative_summary": narrative,
        "health_rating": health_rating,
        "summary_statistics": summary_stats,
        "flagged_ros": flagged_df.where(flagged_df.notna(), None).to_dict(orient="records"),
        "flagged_count": len(flagged_df),
        "stage_bottlenecks": bottlenecks,
        "derived_health_metrics": health_metrics,
        "failure_breakdown": failure_breakdown,
        "market_context": market_context,
        "retry_effectiveness": retry_eff,
        "recommended_actions": recommended_actions,
        "charts": charts,
        "summary": f"Pipeline Health Report ({filter_desc}): {health_rating}. {total_ros:,} ROs, {failed_ros:,} failed ({fail_rate}%), {stuck_ros:,} stuck, {crit} critical. {len(recommended_actions)} recommended actions.",
    }
    return _sanitize_nan(result)


def _sanitize_nan(obj):
    """Recursively replace NaN/inf with None for JSON serialization."""
    import math
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(item) for item in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    return obj

"""Operational Report Generator — produces structured reports for states, orgs, or time windows."""

from data_loader import query


def generate_state_report(state: str) -> dict:
    """Generate a comprehensive Pipeline & Quality Health Report for a state."""

    # Summary stats
    summary = query(f"""
        SELECT
            COUNT(*) as total_ros,
            SUM(CASE WHEN IS_STUCK = 1 THEN 1 ELSE 0 END) as stuck_ros,
            SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_ros,
            ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate,
            COUNT(DISTINCT ORG_NM) as unique_orgs,
            COUNT(DISTINCT SRC_SYS) as source_systems
        FROM roster
        WHERE CNT_STATE = '{state}'
    """)

    # Stage bottlenecks (most Red flags)
    bottlenecks = query(f"""
        SELECT
            SUM(CASE WHEN PRE_PROCESSING_HEALTH = 'RED' THEN 1 ELSE 0 END) as pre_proc_red,
            SUM(CASE WHEN MAPPING_APROVAL_HEALTH = 'RED' THEN 1 ELSE 0 END) as mapping_red,
            SUM(CASE WHEN ISF_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as isf_red,
            SUM(CASE WHEN DART_GEN_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_gen_red,
            SUM(CASE WHEN DART_REVIEW_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_review_red,
            SUM(CASE WHEN DART_UI_VALIDATION_HEALTH = 'RED' THEN 1 ELSE 0 END) as dart_ui_red,
            SUM(CASE WHEN SPS_LOAD_HEALTH = 'RED' THEN 1 ELSE 0 END) as sps_red
        FROM roster
        WHERE CNT_STATE = '{state}'
    """)

    # Top failing orgs
    top_failing = query(f"""
        SELECT ORG_NM,
               COUNT(*) as total,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failures,
               ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fail_rate
        FROM roster
        WHERE CNT_STATE = '{state}'
        GROUP BY ORG_NM
        HAVING failures > 0
        ORDER BY failures DESC
        LIMIT 10
    """)

    # Failure status breakdown
    failure_types = query(f"""
        SELECT FAILURE_STATUS, COUNT(*) as cnt
        FROM roster
        WHERE CNT_STATE = '{state}' AND IS_FAILED = 1
        GROUP BY FAILURE_STATUS
        ORDER BY cnt DESC
    """)

    # Market SCS%
    market = query(f"""
        SELECT MONTH, SCS_PERCENT, OVERALL_SCS_CNT, OVERALL_FAIL_CNT
        FROM metrics
        WHERE MARKET = '{state}'
        ORDER BY MONTH DESC
    """)

    # LOB distribution
    lob_stats = query(f"""
        SELECT LOB, COUNT(*) as cnt,
               SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failures
        FROM roster
        WHERE CNT_STATE = '{state}'
        GROUP BY LOB
        ORDER BY cnt DESC
    """)

    return {
        "report_type": "state",
        "state": state,
        "summary": summary.to_dict(orient="records")[0] if not summary.empty else {},
        "stage_bottlenecks": bottlenecks.to_dict(orient="records")[0] if not bottlenecks.empty else {},
        "top_failing_orgs": top_failing.to_dict(orient="records"),
        "failure_types": failure_types.to_dict(orient="records"),
        "market_scs": market.to_dict(orient="records"),
        "lob_distribution": lob_stats.to_dict(orient="records"),
        "recommendations": _generate_recommendations(
            summary.to_dict(orient="records")[0] if not summary.empty else {},
            bottlenecks.to_dict(orient="records")[0] if not bottlenecks.empty else {},
        ),
    }


def generate_org_report(org_name: str) -> dict:
    """Generate report for a specific organization."""
    summary = query(f"""
        SELECT
            COUNT(*) as total_ros,
            SUM(CASE WHEN IS_STUCK = 1 THEN 1 ELSE 0 END) as stuck_ros,
            SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) as failed_ros,
            ROUND(SUM(CASE WHEN IS_FAILED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate,
            COUNT(DISTINCT CNT_STATE) as states,
            COUNT(DISTINCT SRC_SYS) as source_systems,
            MIN(FILE_RECEIVED_DT) as earliest_file,
            MAX(FILE_RECEIVED_DT) as latest_file
        FROM roster
        WHERE ORG_NM LIKE '%{org_name}%'
    """)

    return {
        "report_type": "org",
        "org_name": org_name,
        "summary": summary.to_dict(orient="records")[0] if not summary.empty else {},
    }


def _generate_recommendations(summary: dict, bottlenecks: dict) -> list[str]:
    """Generate actionable recommendations based on report data."""
    recs = []

    fail_rate = summary.get("failure_rate", 0)
    if fail_rate > 10:
        recs.append(f"CRITICAL: Failure rate is {fail_rate}% — investigate root cause immediately")
    elif fail_rate > 5:
        recs.append(f"WARNING: Failure rate is {fail_rate}% — monitor closely and review top failing orgs")

    stuck = summary.get("stuck_ros", 0)
    if stuck > 0:
        recs.append(f"ACTION: {stuck} RO(s) are stuck — run triage_stuck_ros for escalation priority")

    # Find worst stage
    if bottlenecks:
        worst_stage = max(
            [(k, v) for k, v in bottlenecks.items() if isinstance(v, (int, float))],
            key=lambda x: x[1],
            default=(None, 0),
        )
        if worst_stage[1] > 0:
            stage_name = worst_stage[0].replace("_red", "").replace("_", " ").title()
            recs.append(f"BOTTLENECK: {stage_name} has {worst_stage[1]} Red health flags — highest bottleneck")

    if not recs:
        recs.append("Pipeline health is within acceptable parameters")

    return recs

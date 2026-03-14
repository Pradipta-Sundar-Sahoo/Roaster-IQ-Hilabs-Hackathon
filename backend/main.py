"""RosterIQ — FastAPI Backend."""

import os
import uuid
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

from data_loader import get_connection, get_table_stats
from memory.episodic import EpisodicMemory
from memory.procedural import ProceduralMemory
from memory.semantic import SemanticMemory
from vector_store import VectorStore
from query_pipeline import QueryPipeline
from agents.llm_provider import LLMProvider
from agents.supervisor import SupervisorAgent

# --- Globals ---
episodic_memory: EpisodicMemory = None
procedural_memory: ProceduralMemory = None
semantic_memory: SemanticMemory = None
supervisor: SupervisorAgent = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global episodic_memory, procedural_memory, semantic_memory, supervisor

    # Initialize DuckDB
    get_connection()

    # Initialize memory systems
    memory_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "memory")
    episodic_memory = EpisodicMemory(os.path.join(memory_dir, "episodic.db"))
    procedural_memory = ProceduralMemory(os.path.join(memory_dir, "procedures.json"))
    semantic_memory = SemanticMemory(os.path.join(memory_dir, "semantic_knowledge.yaml"))

    # Initialize vector store + query pipeline
    vector_store = VectorStore(os.path.join(memory_dir, "chroma_db"))
    vector_store.initialize_domain_knowledge(semantic_memory)
    vector_store.initialize_roster_profiles(get_connection())

    pipeline = QueryPipeline(
        vector_store=vector_store,
        episodic_memory=episodic_memory,
        semantic_memory=semantic_memory,
        llm_provider=LLMProvider(),
    )

    # Initialize supervisor agent
    supervisor = SupervisorAgent(
        episodic_memory=episodic_memory,
        procedural_memory=procedural_memory,
        semantic_memory=semantic_memory,
        pipeline=pipeline,
        vector_store=vector_store,
    )

    print("RosterIQ backend initialized.")
    yield
    print("RosterIQ backend shutting down.")


app = FastAPI(title="RosterIQ", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Request/Response Models ---
class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    message: str
    charts: list[dict] = []
    memory_updates: dict = {}
    web_search_results: list[dict] = []
    tool_calls: list[dict] = []
    procedure_used: str | None = None
    procedure_updates: list[dict] = []
    agent_used: str | None = None
    session_id: str = ""


# --- Endpoints ---
@app.get("/health")
async def health():
    try:
        roster_stats = get_table_stats("roster")
        metrics_stats = get_table_stats("metrics")
        return {
            "status": "healthy",
            "data": {"roster": roster_stats, "metrics": metrics_stats},
            "memory": {
                "episodic": episodic_memory is not None,
                "procedural": procedural_memory is not None,
                "semantic": semantic_memory is not None,
            },
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    session_id = request.session_id or str(uuid.uuid4())

    try:
        result = await supervisor.handle(request.message, session_id)
        # Build compact tool_calls for frontend
        raw_tool_results = result.get("tool_results", [])
        tool_calls = []
        for tr in raw_tool_results:
            tc = {"tool": tr.get("tool", ""), "args": tr.get("args", {})}
            res = tr.get("result", {})
            if isinstance(res, dict):
                if "data" in res and isinstance(res["data"], list):
                    tc["result"] = {
                        "row_count": res.get("row_count", 0),
                        "columns": res.get("columns", []),
                        "data": res["data"][:50],  # cap at 50 rows for frontend
                    }
                elif "error" in res:
                    tc["result"] = {"error": res["error"]}
                elif "summary" in res:
                    tc["result"] = {"summary": res["summary"]}
                else:
                    tc["result"] = {k: v for k, v in res.items() if k != "chart"}
            else:
                tc["result"] = res
            tool_calls.append(tc)

        return ChatResponse(
            message=result.get("message", ""),
            charts=result.get("charts", []),
            memory_updates=result.get("memory_updates", {}),
            web_search_results=result.get("web_search_results", []),
            tool_calls=tool_calls,
            procedure_used=result.get("procedure_used"),
            procedure_updates=result.get("procedure_updates", []),
            agent_used=result.get("agent_used"),
            session_id=session_id,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/session/briefing")
async def get_session_briefing(session_id: str = ""):
    """Generate a session briefing comparing current data to the last session."""
    try:
        briefing = episodic_memory.generate_session_briefing(session_id)
        return {"briefing": briefing, "has_briefing": bool(briefing)}
    except Exception as e:
        return {"briefing": "", "has_briefing": False, "error": str(e)}


@app.get("/memory/episodic")
async def get_episodic_memory(limit: int = 50):
    episodes = episodic_memory.get_all_episodes(limit=limit)
    state_changes = episodic_memory.get_all_state_changes(limit=limit)
    return {"episodes": episodes, "state_changes": state_changes}


@app.get("/memory/procedural")
async def get_procedural_memory():
    return procedural_memory.get_all_procedures()


@app.get("/memory/semantic")
async def get_semantic_memory():
    return semantic_memory.get_all_knowledge()


@app.put("/memory/procedural/{name}")
async def update_procedure(name: str, update: dict):
    try:
        procedural_memory.update_procedure(name, update)
        return {"status": "updated", "procedure": name}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Procedure '{name}' not found")


class CreateProcedureRequest(BaseModel):
    name: str
    description: str
    steps: list[dict] = []
    parameters: dict = {}


@app.post("/memory/procedural")
async def create_procedure(request: CreateProcedureRequest):
    try:
        out = procedural_memory.create_procedure(
            name=request.name,
            description=request.description,
            steps=request.steps,
            parameters=request.parameters,
        )
        return {"status": "created", "procedure": out["procedure"]}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


def _roster_time_cond(time_filter: str, from_month: str | None, to_month: str | None) -> str | None:
    """Build roster time condition. Returns SQL fragment or None."""
    if from_month and to_month:
        return (
            f"CAST(FILE_RECEIVED_DT AS TIMESTAMP) >= STRPTIME('{from_month}', '%m-%Y') "
            f"AND CAST(FILE_RECEIVED_DT AS TIMESTAMP) < STRPTIME('{to_month}', '%m-%Y') + INTERVAL '1 month'"
        )
    if time_filter == "7d":
        return "CAST(FILE_RECEIVED_DT AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '7 days'"
    if time_filter == "1m":
        return "CAST(FILE_RECEIVED_DT AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '1 month'"
    return None


def _metrics_time_cond(months: int | None, from_month: str | None, to_month: str | None) -> str | None:
    """Build metrics time condition. Returns SQL fragment or None."""
    if from_month and to_month:
        return (
            f"MONTH_DATE >= STRPTIME('{from_month}', '%m-%Y') "
            f"AND MONTH_DATE < STRPTIME('{to_month}', '%m-%Y') + INTERVAL '1 month'"
        )
    if months:
        return f"MONTH_DATE >= CURRENT_DATE - INTERVAL '{months} months'"
    return None


@app.get("/dashboard/overview")
async def dashboard_overview(
    state: str | None = None,
    time_filter: str = "all",
    from_month: str | None = None,
    to_month: str | None = None,
):
    """Overview. time_filter: all | 7d | 1m. Or use from_month/to_month (MM-YYYY) for custom range."""
    from data_loader import query

    conds = []
    if state:
        conds.append(f"CNT_STATE = '{state}'")
    tc = _roster_time_cond(time_filter, from_month, to_month)
    if tc:
        conds.append(tc)
    where = " AND ".join(conds) if conds else "1=1"

    stuck_count = query(f"SELECT COUNT(*) as cnt FROM roster WHERE IS_STUCK = 1 AND {where}").iloc[0]["cnt"]
    failed_count = query(f"SELECT COUNT(*) as cnt FROM roster WHERE IS_FAILED = 1 AND {where}").iloc[0]["cnt"]
    total_ros = query(f"SELECT COUNT(*) as cnt FROM roster WHERE {where}").iloc[0]["cnt"]

    health_cols = [
        "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
        "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"
    ]
    red_counts = {}
    for col in health_cols:
        cnt = query(f'SELECT COUNT(*) as cnt FROM roster WHERE "{col}" = \'RED\' AND {where}').iloc[0]["cnt"]
        red_counts[col] = int(cnt)

    latest_month = query("SELECT MONTH FROM metrics ORDER BY MONTH DESC LIMIT 1").iloc[0]["MONTH"]
    market_summary = query(f"""
        SELECT MARKET, SCS_PERCENT FROM metrics WHERE MONTH = '{latest_month}'
        ORDER BY SCS_PERCENT ASC
    """).to_dict(orient="records")

    return {
        "total_ros": int(total_ros),
        "stuck_ros": int(stuck_count),
        "failed_ros": int(failed_count),
        "red_health_flags": red_counts,
        "latest_month": latest_month,
        "market_summary": market_summary,
        "filters": {"state": state, "time_filter": time_filter},
    }


@app.get("/dashboard/charts/heatmap")
async def dashboard_chart_heatmap(
    state: str | None = None,
    time_filter: str = "all",
    from_month: str | None = None,
    to_month: str | None = None,
):
    """Pipeline stage health heatmap. time_filter: all | 7d | 1m. Or from_month/to_month (MM-YYYY)."""
    from data_loader import query
    from tools.visualizations import create_health_heatmap

    conds = []
    if state:
        conds.append(f"CNT_STATE = '{state}'")
    tc = _roster_time_cond(time_filter, from_month, to_month)
    if tc:
        conds.append(tc)
    where = " AND ".join(conds) if conds else "1=1"
    df = query(f"""
        SELECT ORG_NM, PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH,
               ISF_GEN_HEALTH, DART_GEN_HEALTH, DART_REVIEW_HEALTH,
               DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH
        FROM roster WHERE {where} LIMIT 30
    """)
    chart = create_health_heatmap(df)
    return {"chart": chart, "filters": {"state": state, "time_filter": time_filter}}


@app.get("/dashboard/charts/market_trend")
async def dashboard_chart_market_trend(
    market: str | None = None,
    months: int | None = 6,
    from_month: str | None = None,
    to_month: str | None = None,
):
    """Market SCS% trend. months: 3|6|12|18|24|36. Or from_month/to_month (MM-YYYY)."""
    from data_loader import query
    from tools.visualizations import create_market_trend

    tc = _metrics_time_cond(months, from_month, to_month)
    where = tc or "1=1"
    if market:
        where += f" AND MARKET = '{market}'"
    df = query(f"SELECT * FROM metrics WHERE {where} ORDER BY MARKET, MONTH_DATE")
    chart = create_market_trend(df, market)
    return {"chart": chart, "filters": {"market": market, "months": months}}


@app.get("/dashboard/charts/retry_lift")
async def dashboard_chart_retry_lift(
    market: str | None = None,
    months: int | None = 6,
    from_month: str | None = None,
    to_month: str | None = None,
):
    """Retry lift chart. months: 3|6|12|18|24|36. Or from_month/to_month (MM-YYYY)."""
    from data_loader import query
    from tools.visualizations import create_retry_lift

    tc = _metrics_time_cond(months, from_month, to_month)
    where = tc or "1=1"
    if market:
        where += f" AND MARKET = '{market}'"
    df = query(f"""
        SELECT MARKET, MONTH, FIRST_ITER_SCS_CNT, NEXT_ITER_SCS_CNT, OVERALL_SCS_CNT
        FROM metrics WHERE {where} ORDER BY MARKET, MONTH_DATE
    """)
    chart = create_retry_lift(df)
    return {"chart": chart, "filters": {"market": market, "months": months}}


@app.get("/dashboard/charts/stuck_tracker")
async def dashboard_chart_stuck_tracker(
    state: str | None = None,
    time_filter: str = "all",
    from_month: str | None = None,
    to_month: str | None = None,
):
    """Stuck RO tracker. time_filter: all | 7d | 1m. Or from_month/to_month (MM-YYYY)."""
    from data_loader import query
    from tools.visualizations import create_stuck_tracker

    conds = ["IS_STUCK = 1"]
    if state:
        conds.append(f"CNT_STATE = '{state}'")
    tc = _roster_time_cond(time_filter, from_month, to_month)
    if tc:
        conds.append(tc)
    where = " AND ".join(conds)
    df = query(f"""
        SELECT RO_ID, ORG_NM, CNT_STATE, LATEST_STAGE_NM, FILE_RECEIVED_DT,
               PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH, ISF_GEN_HEALTH,
               DART_GEN_HEALTH, DART_REVIEW_HEALTH, DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH,
               DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) AS DAYS_STUCK
        FROM roster WHERE {where} ORDER BY DAYS_STUCK DESC
    """)
    if not df.empty:
        health_cols = ["PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
                       "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"]
        df["RED_COUNT"] = df[health_cols].apply(lambda r: sum(1 for v in r if str(v).upper() == "RED"), axis=1)
        df["PRIORITY"] = df.apply(
            lambda r: "CRITICAL" if r["DAYS_STUCK"] > 90 and r["RED_COUNT"] >= 2
            else "HIGH" if r["DAYS_STUCK"] > 30 or r["RED_COUNT"] >= 2
            else "MEDIUM" if r["DAYS_STUCK"] > 7 else "LOW", axis=1
        )
    chart = create_stuck_tracker(df)
    return {"chart": chart, "filters": {"state": state, "time_filter": time_filter}}


@app.get("/dashboard/options")
async def dashboard_options():
    """Return filter options: states, markets, months (MM-YYYY)."""
    from data_loader import query

    states = query("SELECT DISTINCT CNT_STATE FROM roster WHERE CNT_STATE IS NOT NULL ORDER BY CNT_STATE").iloc[:, 0].tolist()
    markets = query("SELECT DISTINCT MARKET FROM metrics WHERE MARKET IS NOT NULL ORDER BY MARKET").iloc[:, 0].tolist()
    months = query("SELECT DISTINCT MONTH FROM metrics WHERE MONTH IS NOT NULL ORDER BY MONTH DESC").iloc[:, 0].tolist()
    return {"states": states, "markets": markets, "months": months}


@app.get("/dashboard/alerts")
async def dashboard_alerts(scs_threshold: float = 95.0):
    """Proactive monitoring alerts. Optional scs_threshold (default 95) for markets below SLA."""
    alerts = await supervisor.generate_proactive_alerts(scs_threshold=scs_threshold)
    return {"alerts": alerts}


@app.get("/alerts")
async def alerts(scs_threshold: float = 95.0):
    """Proactive monitoring — same as /dashboard/alerts. For judges: GET /alerts?scs_threshold=90"""
    return await dashboard_alerts(scs_threshold)


@app.get("/dashboard/intelligence")
async def dashboard_intelligence():
    """Decision-support intelligence: summaries, root causes, recommendations."""
    from data_loader import query as db_query

    # ── Pipeline health summary ──
    total = db_query("SELECT COUNT(*) as c FROM roster").iloc[0]["c"]
    stuck = db_query("SELECT COUNT(*) as c FROM roster WHERE IS_STUCK=1").iloc[0]["c"]
    failed = db_query("SELECT COUNT(*) as c FROM roster WHERE IS_FAILED=1").iloc[0]["c"]
    critical = db_query("SELECT COUNT(*) as c FROM roster WHERE PRIORITY='CRITICAL'").iloc[0]["c"]

    fail_rate = round(float(failed) / max(float(total), 1) * 100, 2)
    stuck_rate = round(float(stuck) / max(float(total), 1) * 100, 2)

    health_status = "healthy"
    if fail_rate > 15 or stuck_rate > 20:
        health_status = "critical"
    elif fail_rate > 8 or stuck_rate > 10:
        health_status = "degraded"
    elif fail_rate > 3:
        health_status = "warning"

    pipeline_summary = (
        f"Pipeline is {health_status.upper()}. "
        f"{int(total):,} total ROs: {int(failed):,} failed ({fail_rate}%), "
        f"{int(stuck):,} stuck ({stuck_rate}%), {int(critical):,} critical priority."
    )

    # ── Root cause insights ──
    root_causes = []

    top_failures = db_query("""
        SELECT FAILURE_CATEGORY, FAILURE_STATUS, COUNT(*) as cnt
        FROM roster WHERE IS_FAILED=1 AND FAILURE_CATEGORY != 'NONE'
        GROUP BY FAILURE_CATEGORY, FAILURE_STATUS
        ORDER BY cnt DESC LIMIT 5
    """)
    failure_explanations = {
        "COMPLETE VALIDATION FAILURE": "Schema mismatch or corrupt source data — all records in the file failed validation checks. Likely a source system format change.",
        "INCOMPATIBLE": "Source system changed its output format without notification. The file structure doesn't match expected schema.",
        "FAILED": "Generic processing failure — requires investigation of the specific pipeline stage where failure occurred.",
        "STUCK": "RO has not progressed beyond current stage within expected SLA. May need manual intervention.",
    }
    for _, row in top_failures.iterrows():
        status = str(row.get("FAILURE_STATUS", ""))
        explanation = failure_explanations.get(status, f"Failure pattern '{status}' detected across multiple ROs.")
        root_causes.append({
            "issue": f"{int(row['cnt'])} ROs with {row['FAILURE_CATEGORY']} failure ({status})",
            "explanation": explanation,
            "severity": "high" if row["cnt"] > 100 else "medium",
            "count": int(row["cnt"]),
        })

    stage_bottleneck = db_query("""
        SELECT STAGE_NM, STUCK_IN_STAGE, RED_COUNT_TOTAL, TOTAL_ROS,
               ROUND(RED_COUNT_TOTAL * 100.0 / NULLIF(TOTAL_ROS, 0), 2) as red_pct
        FROM stage_health_summary
        WHERE STUCK_IN_STAGE > 0
        ORDER BY STUCK_IN_STAGE DESC LIMIT 3
    """)
    stage_meanings = {
        "PRE_PROCESSING": "intake/format parsing — RED here means source files have structural issues",
        "MAPPING_APROVAL": "provider mapping review — RED here means data quality issues requiring manual review",
        "ISF_GEN": "initial source file generation — RED here means transformation pipeline issues",
        "DART_GEN": "provider data transformation — RED here means record-level processing failures",
        "DART_REVIEW": "data review stage — RED here means validation rules are catching errors",
        "DART_UI_VALIDATION": "UI validation — RED here means human review is finding issues",
        "SPS_LOAD": "final system-of-record load — RED here means downstream delivery failure",
    }
    for _, row in stage_bottleneck.iterrows():
        stage = str(row["STAGE_NM"])
        meaning = stage_meanings.get(stage, f"Pipeline stage {stage}")
        root_causes.append({
            "issue": f"{int(row['STUCK_IN_STAGE'])} ROs stuck at {stage} ({row['red_pct']}% RED)",
            "explanation": f"{stage} handles {meaning}. High stuck count suggests systemic issues at this stage.",
            "severity": "high" if row["STUCK_IN_STAGE"] > 50 else "medium",
            "count": int(row["STUCK_IN_STAGE"]),
        })

    # ── Recommended actions ──
    recommended_actions = []

    if int(critical) > 0:
        recommended_actions.append({
            "priority": 1,
            "action": f"Triage {int(critical)} critical-priority stuck ROs immediately",
            "procedure": "triage_stuck_ros",
            "params": {},
        })

    worst_state = db_query("""
        SELECT STATE, FAILURE_RATE, FAILED_COUNT
        FROM state_summary ORDER BY FAILURE_RATE DESC LIMIT 1
    """)
    if not worst_state.empty:
        ws = worst_state.iloc[0]
        recommended_actions.append({
            "priority": 2,
            "action": f"Audit {ws['STATE']} — highest failure rate at {ws['FAILURE_RATE']}% ({int(ws['FAILED_COUNT'])} failures)",
            "procedure": "record_quality_audit",
            "params": {"state": str(ws["STATE"])},
        })

    try:
        worst_market = db_query("""
            SELECT MARKET, SCS_PERCENT FROM metrics
            WHERE MONTH_DATE = (SELECT MAX(MONTH_DATE) FROM metrics)
            ORDER BY SCS_PERCENT ASC LIMIT 1
        """)
        if not worst_market.empty:
            wm = worst_market.iloc[0]
            if wm["SCS_PERCENT"] < 95:
                recommended_actions.append({
                    "priority": 3,
                    "action": f"Investigate {wm['MARKET']} market — SCS at {wm['SCS_PERCENT']}% (below 95% SLA)",
                    "procedure": "market_health_report",
                    "params": {"market": str(wm["MARKET"])},
                })
    except Exception:
        pass

    recommended_actions.append({
        "priority": 4,
        "action": "Analyze retry effectiveness to identify where reprocessing helps vs wastes resources",
        "procedure": "retry_effectiveness_analysis",
        "params": {},
    })

    # ── Retry effectiveness quick stats ──
    try:
        retry_stats = db_query("""
            SELECT
                COUNT(*) as total_ros_with_retries,
                SUM(CASE WHEN IS_RETRY=1 AND IS_FAILED=0 THEN 1 ELSE 0 END) as retry_successes,
                SUM(CASE WHEN IS_RETRY=1 AND IS_FAILED=1 THEN 1 ELSE 0 END) as retry_failures
            FROM roster WHERE IS_RETRY=1
        """)
        rs = retry_stats.iloc[0]
        retry_total = int(rs["total_ros_with_retries"])
        retry_eff = {
            "total_retries": retry_total,
            "retry_successes": int(rs["retry_successes"]),
            "retry_failures": int(rs["retry_failures"]),
            "success_rate": round(float(rs["retry_successes"]) / max(retry_total, 1) * 100, 1),
        }
    except Exception:
        retry_eff = {"total_retries": 0, "success_rate": 0}

    # ── Procedure effectiveness ──
    proc_effectiveness = {}
    for name in procedural_memory.get_procedure_names():
        eff = procedural_memory.get_procedure_effectiveness(name)
        proc_effectiveness[name] = {
            "total_runs": eff.get("total_runs", 0),
            "resolved_rate": eff.get("resolved_rate"),
            "last_run": eff.get("last_run"),
        }

    return {
        "pipeline_health_summary": pipeline_summary,
        "health_status": health_status,
        "root_cause_insights": sorted(root_causes, key=lambda x: x["count"], reverse=True),
        "recommended_actions": sorted(recommended_actions, key=lambda x: x["priority"]),
        "retry_effectiveness": retry_eff,
        "procedure_effectiveness": proc_effectiveness,
    }


class ProcedureRequest(BaseModel):
    params: dict = {}


class ReportRequest(BaseModel):
    state: str | None = None
    org: str | None = None
    lob: str | None = None
    source_system: str | None = None


@app.post("/report/generate")
async def generate_report(request: ReportRequest):
    """Generate a comprehensive pipeline health report."""
    params = {k: v for k, v in request.model_dump().items() if v is not None}
    try:
        procedure = procedural_memory.get_procedure("generate_pipeline_health_report")
    except KeyError:
        procedure = {"name": "generate_pipeline_health_report", "version": 1, "steps": [], "parameters": {}}

    from procedures.engine import execute_procedure as run_proc
    result = run_proc(procedure, params)
    procedural_memory.log_execution("generate_pipeline_health_report", params, "informational")
    return result


@app.get("/report/latest")
async def get_latest_report():
    """Generate a default (unfiltered) pipeline health report for the dashboard."""
    try:
        procedure = procedural_memory.get_procedure("generate_pipeline_health_report")
    except KeyError:
        procedure = {"name": "generate_pipeline_health_report", "version": 1, "steps": [], "parameters": {}}

    from procedures.engine import execute_procedure as run_proc
    return run_proc(procedure, {})


@app.post("/procedure/{name}")
async def run_procedure(name: str, request: ProcedureRequest):
    try:
        result = await supervisor.run_procedure(name, request.params)
        return result
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Procedure '{name}' not found")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

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


@app.get("/dashboard/overview")
async def dashboard_overview():
    from data_loader import query

    # Key stats from both CSVs
    stuck_count = query("SELECT COUNT(*) as cnt FROM roster WHERE IS_STUCK = 1").iloc[0]["cnt"]
    failed_count = query("SELECT COUNT(*) as cnt FROM roster WHERE IS_FAILED = 1").iloc[0]["cnt"]
    total_ros = query("SELECT COUNT(*) as cnt FROM roster").iloc[0]["cnt"]

    # Health flag distribution
    health_cols = [
        "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
        "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"
    ]
    red_counts = {}
    for col in health_cols:
        cnt = query(f"SELECT COUNT(*) as cnt FROM roster WHERE \"{col}\" = 'RED'").iloc[0]["cnt"]
        red_counts[col] = int(cnt)

    # Market metrics
    latest_month = query("SELECT MONTH FROM metrics ORDER BY MONTH DESC LIMIT 1").iloc[0]["MONTH"]
    market_summary = query(f"""
        SELECT MARKET, SCS_PERCENT
        FROM metrics
        WHERE MONTH = '{latest_month}'
        ORDER BY SCS_PERCENT ASC
    """).to_dict(orient="records")

    return {
        "total_ros": int(total_ros),
        "stuck_ros": int(stuck_count),
        "failed_ros": int(failed_count),
        "red_health_flags": red_counts,
        "latest_month": latest_month,
        "market_summary": market_summary,
    }


@app.get("/dashboard/alerts")
async def dashboard_alerts():
    """Proactive monitoring alerts."""
    alerts = await supervisor.generate_proactive_alerts()
    return {"alerts": alerts}


class ProcedureRequest(BaseModel):
    params: dict = {}


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

"""Supervisor Agent — routes queries to sub-agents, manages memory, orchestrates tools."""

import json
import os
import re
import google.generativeai as genai

from memory.episodic import EpisodicMemory
from memory.procedural import ProceduralMemory
from memory.semantic import SemanticMemory
from tools.data_query import (
    execute_sql, get_stuck_ros, get_failed_ros, get_failure_stats_by_state,
    get_health_flag_distribution, get_market_trends, get_retry_analysis,
    cross_table_state_analysis,
)
from tools.web_search import search, search_regulatory_context, search_org_context, search_compliance_context
from tools.visualizations import (
    create_stuck_tracker, create_failure_breakdown, create_market_trend,
    create_retry_lift, create_health_heatmap, create_duration_anomaly,
)
from procedures.engine import execute_procedure
from prompts import SUPERVISOR_SYSTEM_PROMPT, ENTITY_EXTRACTION_PROMPT

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

# Tool definitions for Gemini function calling
TOOL_DECLARATIONS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="query_data",
                description="Execute a SQL query against the roster or metrics table. Use DuckDB SQL syntax. The roster table has ~60K rows of Roster Operations with columns like RO_ID, ORG_NM, CNT_STATE, LOB, SRC_SYS, IS_STUCK, IS_FAILED, FAILURE_STATUS, LATEST_STAGE_NM, FILE_RECEIVED_DT, health flag columns (*_HEALTH), duration columns (*_DURATION), etc. The metrics table has ~357 rows with MONTH, MARKET, SCS_PERCENT, FIRST_ITER_SCS_CNT, NEXT_ITER_SCS_CNT, etc.",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "sql": genai.protos.Schema(type=genai.protos.Type.STRING, description="SQL SELECT query to execute"),
                    },
                    required=["sql"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="web_search",
                description="Search the web for regulatory context, organization info, or compliance requirements. Use when: (1) a market's SCS% is declining — search for regulatory changes, (2) validation failures appear — search for CMS compliance, (3) specific org has issues — search for business context.",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "query": genai.protos.Schema(type=genai.protos.Type.STRING, description="Search query"),
                        "search_type": genai.protos.Schema(type=genai.protos.Type.STRING, description="Type: regulatory, org, compliance, lob, general"),
                    },
                    required=["query"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="run_procedure",
                description="Run a named diagnostic procedure. Available: triage_stuck_ros (find stuck/failed ROs), record_quality_audit (audit failure rates by state/org), market_health_report (correlate market SCS% with file failures), retry_effectiveness_analysis (compare first-pass vs retry success).",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "procedure_name": genai.protos.Schema(type=genai.protos.Type.STRING, description="Name of procedure to run"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON string of parameters (e.g., {\"state\": \"NY\", \"threshold\": 5.0})"),
                    },
                    required=["procedure_name"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="create_chart",
                description="Create a visualization chart. Types: health_heatmap (orgs × stages health), failure_breakdown (failure types by state), duration_anomaly (actual vs avg durations), market_trend (SCS% over time), retry_lift (first-iter vs overall success), stuck_tracker (stuck ROs by priority).",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "chart_type": genai.protos.Schema(type=genai.protos.Type.STRING, description="Type of chart"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON string of chart parameters"),
                    },
                    required=["chart_type"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="recall_memory",
                description="Search episodic memory for past investigations. Use this when the user asks about previous sessions, past queries, or 'have we looked at X before'.",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "search_text": genai.protos.Schema(type=genai.protos.Type.STRING, description="Text to search for in past episodes"),
                    },
                    required=["search_text"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="update_procedure",
                description="Update a diagnostic procedure based on user feedback. Use when user wants to modify how a procedure works.",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "procedure_name": genai.protos.Schema(type=genai.protos.Type.STRING, description="Name of procedure to update"),
                        "change_description": genai.protos.Schema(type=genai.protos.Type.STRING, description="Description of the change to make"),
                        "new_step": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON string of new step to add, if applicable"),
                    },
                    required=["procedure_name", "change_description"],
                ),
            ),
        ]
    )
]


class SupervisorAgent:
    def __init__(self, episodic_memory: EpisodicMemory, procedural_memory: ProceduralMemory, semantic_memory: SemanticMemory):
        self.episodic = episodic_memory
        self.procedural = procedural_memory
        self.semantic = semantic_memory
        self.model = genai.GenerativeModel("gemini-2.5-flash")

        from agents.llm_provider import LLMProvider
        self.llm = LLMProvider()

    async def handle(self, user_query: str, session_id: str) -> dict:
        """Handle a user query through the full agent loop."""
        charts = []
        web_results = []
        procedure_used = None

        # 1. Extract entities
        entities = self._extract_entities(user_query)

        # 2. Build compact system prompt
        past = self.episodic.search_by_entities(entities, limit=3)
        if not past:
            past = self.episodic.search_by_query_text(user_query, limit=2)
        if past:
            ep_lines = ["## Recent Investigations"]
            for ep in past[:3]:
                ep_lines.append(f"- [{str(ep.get('timestamp',''))[:19]}] \"{str(ep.get('query',''))[:80]}\" → {str(ep.get('findings_summary',''))[:150]}")
            episodic_context = "\n".join(ep_lines)
        else:
            episodic_context = ""
        system_prompt = SUPERVISOR_SYSTEM_PROMPT.format(episodic_context=episodic_context)

        # 4. Call LLM with tools (Gemini primary, OpenRouter fallback)
        def tool_executor(tool_name, tool_args):
            return self._execute_tool(tool_name, tool_args)

        llm_result = await self.llm.chat_with_tools(system_prompt, user_query, tool_executor)

        tools_used = llm_result.get("tools_used", [])
        final_text = llm_result.get("final_text", "")

        # Collect charts and web results from tool results
        for tr in llm_result.get("tool_results", []):
            result = tr.get("result", {})
            if isinstance(result, dict):
                if "chart" in result and result["chart"]:
                    charts.append(result["chart"])
                if "charts" in result and result["charts"]:
                    charts.extend(result["charts"])
                if tr["tool"] == "web_search":
                    web_results.append(result)
                if tr["tool"] == "run_procedure":
                    procedure_used = tr["args"].get("procedure_name")

        if not final_text:
            final_text = "I processed your query but couldn't generate a text response. Please try rephrasing."

        # 7. Log to episodic memory
        findings_summary = final_text[:500]
        data_snapshot = self._create_snapshot(entities)
        episode_id = self.episodic.log_episode(
            session_id=session_id,
            query=user_query,
            intent=entities.get("intent", "general"),
            entities=entities,
            findings_summary=findings_summary,
            tools_used=tools_used,
            procedure_used=procedure_used,
            data_snapshot=data_snapshot,
        )

        # 8. Check for state changes
        state_changes = self._detect_state_changes(entities, data_snapshot, episode_id)

        return {
            "message": final_text,
            "charts": charts,
            "memory_updates": {
                "episodic": {"logged": True, "episode_id": episode_id},
                "state_changes": state_changes,
            },
            "web_search_results": web_results,
            "procedure_used": procedure_used,
            "agent_used": self._classify_agent(entities),
        }

    def _extract_entities(self, query: str) -> dict:
        """Extract entities from query using regex (fast, reliable, no LLM needed)."""
        return self._regex_extract(query)

    def _extract_entities_llm(self, query: str) -> dict:
        """LLM-based entity extraction (unused, kept for reference)."""
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(
                ENTITY_EXTRACTION_PROMPT.format(query=query),
                generation_config=genai.GenerationConfig(temperature=0),
            )
            text = response.text.strip()
            # Clean markdown code fences
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception:
            # Fallback: basic regex extraction
            return self._regex_extract(query)

    def _regex_extract(self, query: str) -> dict:
        """Fallback entity extraction using regex."""
        state_codes = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]
        found_states = [s for s in state_codes if re.search(rf'\b{s}\b', query.upper())]

        ro_ids = re.findall(r'RO-\d+', query)

        procedures = []
        proc_names = ["triage_stuck_ros", "record_quality_audit", "market_health_report", "retry_effectiveness_analysis"]
        for p in proc_names:
            if p.replace("_", " ") in query.lower() or p in query.lower():
                procedures.append(p)

        # Infer intent
        intent = "general"
        q_lower = query.lower()
        if any(w in q_lower for w in ["stuck", "triage", "critical", "stalled"]):
            intent = "triage"
        elif any(w in q_lower for w in ["quality", "audit", "failure rate", "rejection"]):
            intent = "audit"
        elif any(w in q_lower for w in ["market", "scs", "success rate", "trend"]):
            intent = "report"
        elif any(w in q_lower for w in ["retry", "reprocess", "re-run"]):
            intent = "analysis"
        elif any(w in q_lower for w in ["remember", "before", "last time", "history", "past"]):
            intent = "memory_recall"
        elif any(w in q_lower for w in ["update", "modify", "change", "also include", "add to"]):
            intent = "procedure_update"

        return {
            "states": found_states,
            "orgs": [],
            "ro_ids": ro_ids,
            "lobs": [],
            "procedures": procedures,
            "intent": intent,
        }

    def _execute_tool(self, tool_name: str, args: dict) -> dict:
        """Execute a tool call and return results."""
        try:
            if tool_name == "query_data":
                return execute_sql(args.get("sql", ""))

            elif tool_name == "web_search":
                return search(args.get("query", ""), max_results=3)

            elif tool_name == "run_procedure":
                proc_name = args.get("procedure_name", "")
                params = {}
                if args.get("params"):
                    try:
                        params = json.loads(args["params"])
                    except json.JSONDecodeError:
                        params = {}
                procedure = self.procedural.get_procedure(proc_name)
                return execute_procedure(procedure, params)

            elif tool_name == "create_chart":
                return self._create_chart(args.get("chart_type", ""), args.get("params", "{}"))

            elif tool_name == "recall_memory":
                text = args.get("search_text", "")
                episodes = self.episodic.search_by_query_text(text, limit=5)
                if not episodes:
                    episodes = self.episodic.get_all_episodes(limit=5)
                clean = []
                for ep in episodes:
                    clean.append({
                        "id": ep.get("id", ""),
                        "timestamp": str(ep.get("timestamp", ""))[:19],
                        "query": str(ep.get("query", ""))[:120],
                        "intent": str(ep.get("intent", "")),
                        "findings": str(ep.get("findings_summary", ""))[:200],
                        "tools": str(ep.get("tools_used", "")),
                    })
                return {"episodes": clean, "count": len(clean)}

            elif tool_name == "update_procedure":
                proc_name = args.get("procedure_name", "")
                change_desc = args.get("change_description", "")
                new_step = None
                if args.get("new_step"):
                    try:
                        new_step = json.loads(args["new_step"])
                    except json.JSONDecodeError:
                        pass

                updates = {"change_summary": change_desc}
                if new_step:
                    updates["add_step"] = new_step
                return self.procedural.update_procedure(proc_name, updates)

            return {"error": f"Unknown tool: {tool_name}"}

        except Exception as e:
            return {"error": f"Tool '{tool_name}' failed: {str(e)}"}

    def _create_chart(self, chart_type: str, params_str: str) -> dict:
        """Create a chart based on type."""
        from data_loader import query as db_query

        try:
            params = json.loads(params_str) if params_str else {}
        except json.JSONDecodeError:
            params = {}

        try:
            if chart_type == "health_heatmap":
                state = params.get("state")
                where = f"WHERE CNT_STATE = '{state}'" if state else ""
                df = db_query(f"""
                    SELECT ORG_NM, PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH,
                           ISF_GEN_HEALTH, DART_GEN_HEALTH, DART_REVIEW_HEALTH,
                           DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH
                    FROM roster {where}
                    LIMIT 30
                """)
                return {"chart": create_health_heatmap(df)}

            elif chart_type == "failure_breakdown":
                state = params.get("state")
                where = f"AND CNT_STATE = '{state}'" if state else ""
                stats = db_query(f"""
                    SELECT CNT_STATE, COUNT(*) as total_files,
                           SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END) as failed_files,
                           ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) as failure_rate
                    FROM roster WHERE 1=1 {where}
                    GROUP BY CNT_STATE ORDER BY failure_rate DESC LIMIT 20
                """)
                failures = db_query(f"""
                    SELECT FAILURE_STATUS, COUNT(*) as cnt
                    FROM roster WHERE IS_FAILED=1 {where}
                    GROUP BY FAILURE_STATUS ORDER BY cnt DESC
                """)
                return {"chart": create_failure_breakdown(stats, failures)}

            elif chart_type == "duration_anomaly":
                df = db_query("""
                    SELECT ORG_NM, CNT_STATE,
                           DART_GEN_DURATION, AVG_DART_GENERATION_DURATION,
                           DART_UI_VALIDATION_DURATION, AVG_DART_UI_VLDTN_DURATION,
                           SPS_LOAD_DURATION, AVG_SPS_LOAD_DURATION,
                           ISF_GEN_DURATION, AVG_ISF_GENERATION_DURATION
                    FROM roster
                    WHERE DART_GEN_DURATION IS NOT NULL
                    LIMIT 200
                """)
                return {"chart": create_duration_anomaly(df)}

            elif chart_type == "market_trend":
                market = params.get("market")
                where = f"WHERE MARKET = '{market}'" if market else ""
                df = db_query(f"SELECT * FROM metrics {where} ORDER BY MARKET, MONTH")
                return {"chart": create_market_trend(df, market)}

            elif chart_type == "retry_lift":
                df = db_query("""
                    SELECT MARKET, MONTH, FIRST_ITER_SCS_CNT, NEXT_ITER_SCS_CNT, OVERALL_SCS_CNT,
                           ROUND((NEXT_ITER_SCS_CNT - FIRST_ITER_SCS_CNT)*100.0/NULLIF(FIRST_ITER_SCS_CNT,0),2) as retry_lift_pct
                    FROM metrics ORDER BY MARKET, MONTH
                """)
                return {"chart": create_retry_lift(df)}

            elif chart_type == "stuck_tracker":
                df = db_query("""
                    SELECT RO_ID, ORG_NM, CNT_STATE, LATEST_STAGE_NM,
                           FILE_RECEIVED_DT, PRE_PROCESSING_HEALTH, MAPPING_APROVAL_HEALTH,
                           ISF_GEN_HEALTH, DART_GEN_HEALTH, DART_REVIEW_HEALTH,
                           DART_UI_VALIDATION_HEALTH, SPS_LOAD_HEALTH,
                           DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days_stuck
                    FROM roster WHERE IS_STUCK = 1 ORDER BY days_stuck DESC
                """)
                import pandas as pd
                health_cols = ["PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
                               "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"]
                if not df.empty:
                    df["red_count"] = df[health_cols].apply(lambda r: sum(1 for v in r if v == "Red"), axis=1)
                    df["priority"] = df.apply(
                        lambda r: "critical" if r["days_stuck"] > 90 and r["red_count"] >= 2
                        else "high" if r["days_stuck"] > 30 or r["red_count"] >= 2
                        else "medium" if r["days_stuck"] > 7 else "low", axis=1
                    )
                return {"chart": create_stuck_tracker(df)}

            return {"error": f"Unknown chart type: {chart_type}"}
        except Exception as e:
            return {"error": f"Chart creation failed: {str(e)}"}

    def _classify_agent(self, entities: dict) -> str:
        """Determine which sub-agent would handle this query."""
        intent = entities.get("intent", "general")
        if intent in ("triage",):
            return "pipeline_agent"
        elif intent in ("audit", "report", "analysis"):
            return "quality_agent"
        return "supervisor"

    def _create_snapshot(self, entities: dict) -> dict:
        """Create a data snapshot for episodic memory."""
        from data_loader import query as db_query
        try:
            snapshot = {
                "stuck_count": int(db_query("SELECT COUNT(*) as c FROM roster WHERE IS_STUCK=1").iloc[0]["c"]),
                "failed_count": int(db_query("SELECT COUNT(*) as c FROM roster WHERE IS_FAILED=1").iloc[0]["c"]),
            }
            # Add state-specific data if states are mentioned
            for state in entities.get("states", []):
                market = db_query(f"SELECT SCS_PERCENT FROM metrics WHERE MARKET='{state}' ORDER BY MONTH DESC LIMIT 1")
                if not market.empty:
                    snapshot[f"{state}_scs_percent"] = float(market.iloc[0]["SCS_PERCENT"])
            return snapshot
        except Exception:
            return {}

    def _detect_state_changes(self, entities: dict, current_snapshot: dict, episode_id: int) -> list:
        """Detect and log state changes compared to previous snapshots."""
        changes = []
        for state in entities.get("states", []):
            prev_snapshot = self.episodic.get_latest_snapshot_for_entity("market", state)
            if prev_snapshot:
                # Compare SCS_PERCENT
                prev_scs = prev_snapshot.get(f"{state}_scs_percent")
                curr_scs = current_snapshot.get(f"{state}_scs_percent")
                if prev_scs is not None and curr_scs is not None and prev_scs != curr_scs:
                    self.episodic.log_state_change(
                        "market", state, "SCS_PERCENT",
                        str(prev_scs), str(curr_scs), episode_id
                    )
                    changes.append({
                        "entity": state,
                        "field": "SCS_PERCENT",
                        "old": prev_scs,
                        "new": curr_scs,
                    })

        # Check stuck ROs
        for ro_id in entities.get("ro_ids", []):
            prev = self.episodic.get_latest_snapshot_for_entity("ro", ro_id)
            if prev:
                changes.append({"entity": ro_id, "field": "checked", "note": "Previously investigated"})

        return changes

    async def generate_proactive_alerts(self) -> list:
        """Generate proactive monitoring alerts by scanning data."""
        from data_loader import query as db_query
        alerts = []

        # Alert 1: Stuck ROs
        stuck = db_query("SELECT COUNT(*) as c FROM roster WHERE IS_STUCK = 1").iloc[0]["c"]
        if stuck > 0:
            stuck_details = db_query("""
                SELECT RO_ID, ORG_NM, CNT_STATE,
                       DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days
                FROM roster WHERE IS_STUCK = 1
            """)
            alerts.append({
                "type": "stuck_ros",
                "severity": "high",
                "message": f"{int(stuck)} RO(s) are currently stuck in the pipeline",
                "details": stuck_details.to_dict(orient="records"),
            })

        # Alert 2: Markets below 95% SCS
        low_markets = db_query("""
            SELECT MARKET, SCS_PERCENT, MONTH
            FROM metrics
            WHERE SCS_PERCENT < 95
            ORDER BY SCS_PERCENT ASC
        """)
        if not low_markets.empty:
            alerts.append({
                "type": "low_scs",
                "severity": "medium",
                "message": f"{len(low_markets)} market-month entries below 95% success rate",
                "details": low_markets.to_dict(orient="records"),
            })

        # Alert 3: High failure rate states
        high_fail = db_query("""
            SELECT CNT_STATE,
                   ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as fail_rate
            FROM roster
            GROUP BY CNT_STATE
            HAVING fail_rate > 5
            ORDER BY fail_rate DESC
        """)
        if not high_fail.empty:
            alerts.append({
                "type": "high_failure_rate",
                "severity": "medium",
                "message": f"{len(high_fail)} states have failure rates above 5%",
                "details": high_fail.to_dict(orient="records"),
            })

        # Alert 4: Compare with last session
        sessions = self.episodic.get_unique_sessions()
        if sessions:
            last_session = sessions[0]
            alerts.append({
                "type": "session_history",
                "severity": "info",
                "message": f"Last session: {last_session['query_count']} queries at {last_session['last_query']}",
                "details": last_session,
            })

        return alerts

    async def run_procedure(self, name: str, params: dict) -> dict:
        """Run a named procedure."""
        procedure = self.procedural.get_procedure(name)
        return execute_procedure(procedure, params)

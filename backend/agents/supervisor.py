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
from prompts import SUPERVISOR_SYSTEM_PROMPT, ENTITY_EXTRACTION_PROMPT, build_supervisor_prompt
from agents.pipeline_agent import PipelineAgent
from agents.quality_agent import QualityAgent
from agents.formatter_agent import FormatterAgent

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

# Tool definitions for Gemini function calling
TOOL_DECLARATIONS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="query_data",
                description="Execute a SQL query (DuckDB). Use EXACT column names from the schema — do NOT expand abbreviations. CRITICAL: CNT_STATE/MARKET=2-letter codes, IS_FAILED/IS_STUCK/IS_RETRY are INTEGER (=1 not =TRUE), no 'status' column (use IS_FAILED=1), no 'attempt_number' (use RUN_NO), 'table' is reserved.",
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
                description="Run a named diagnostic procedure. Available: triage_stuck_ros (find stuck/failed ROs), record_quality_audit (audit failure rates by state/org), market_health_report (correlate market SCS% with file failures), retry_effectiveness_analysis (compare first-pass vs retry), generate_pipeline_health_report (comprehensive operational report with summary stats, flagged ROs, bottlenecks, health metrics, market context, retry effectiveness, recommended actions, and charts).",
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
            genai.protos.FunctionDeclaration(
                name="update_semantic_knowledge",
                description=(
                    "Update the domain knowledge base when web search reveals new regulatory information, "
                    "new LOB types, new failure patterns, or corrections to existing knowledge. "
                    "Call this after web_search returns regulatory updates so the information is permanently remembered."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "category": genai.protos.Schema(type=genai.protos.Type.STRING, description="Knowledge category: lob_meanings, failure_statuses, source_systems, pipeline_stages, data_notes"),
                        "key": genai.protos.Schema(type=genai.protos.Type.STRING, description="The specific key or name to add/update"),
                        "value": genai.protos.Schema(type=genai.protos.Type.STRING, description="The value or description to store"),
                        "reason": genai.protos.Schema(type=genai.protos.Type.STRING, description="Why this knowledge is being added (e.g., 'CMS ruling found via web search')"),
                    },
                    required=["category", "key", "value", "reason"],
                ),
            ),
        ]
    )
]


class SupervisorAgent:
    def __init__(
        self,
        episodic_memory: EpisodicMemory,
        procedural_memory: ProceduralMemory,
        semantic_memory: SemanticMemory,
        pipeline=None,
        vector_store=None,
    ):
        self.episodic = episodic_memory
        self.procedural = procedural_memory
        self.semantic = semantic_memory
        self.pipeline = pipeline
        self.vector_store = vector_store
        self.model = genai.GenerativeModel("gemini-2.5-flash")
        self.pipeline_agent = PipelineAgent()
        self.quality_agent = QualityAgent()

        from agents.llm_provider import LLMProvider
        self.llm = LLMProvider()

    async def handle(self, user_query: str, session_id: str) -> dict:
        """Handle a user query through the full agent loop."""
        charts = []
        web_results = []
        procedure_used = None

        # 1. Extract entities
        entities = self._extract_entities(user_query)

        # 2. Build enriched episodic context
        past = self.episodic.search_semantic(user_query, limit=5)
        if not past:
            past = self.episodic.search_by_entities(entities, limit=3)

        ep_lines = []
        if past:
            ep_lines.append("## Relevant Past Investigations")
            for ep in past[:5]:
                ts = str(ep.get("timestamp", ""))[:19]
                q = str(ep.get("query", ""))[:100]
                findings = str(ep.get("findings_summary", ""))[:300]
                proc = ep.get("procedure_used", "")
                source = ep.get("_source", "episode")
                prefix = "[DIGEST] " if source == "digest" else ""
                ep_lines.append(f"- [{ts}] {prefix}\"{q}\"")
                if findings:
                    ep_lines.append(f"  Findings: {findings}")
                if proc:
                    ep_lines.append(f"  Procedure used: {proc}")
                for state in entities.get("states", []):
                    if state in str(ep.get("entities_json", "")):
                        ep_lines.append(f"  (Previously investigated {state})")
                        break

        proc_eff_lines = []
        for proc_name in self.procedural.get_procedure_names():
            eff = self.procedural.get_procedure_effectiveness(proc_name)
            if eff.get("total_runs", 0) > 0:
                proc_eff_lines.append(
                    f"- {proc_name}: {eff['resolved_rate']}% resolved over {eff['total_runs']} runs"
                )
        if proc_eff_lines:
            ep_lines.append("\n## Procedure Effectiveness History")
            ep_lines.extend(proc_eff_lines)

        episodic_context = "\n".join(ep_lines) if ep_lines else ""

        # 3. Route: pipeline for general queries, sub-agents for specialized intents
        intent = entities.get("intent", "general")

        def tool_executor(tool_name, tool_args):
            return self._execute_tool(tool_name, tool_args, session_id=session_id)

        if intent == "triage":
            agent_name = "pipeline_agent"
            llm_result = await self.pipeline_agent.handle(user_query, tool_executor, episodic_context)
        elif intent in ("audit", "report", "analysis"):
            agent_name = "quality_agent"
            llm_result = await self.quality_agent.handle(user_query, tool_executor, episodic_context)
        elif self.pipeline:
            agent_name = "pipeline"
            try:
                llm_result = await self.pipeline.process(
                    user_query, session_id, tool_executor, episodic_context=episodic_context
                )
            except Exception as e:
                print(f"  [supervisor] Pipeline failed ({e}), falling back to direct LLM")
                system_prompt = build_supervisor_prompt(episodic_context)
                agent_name = "supervisor"
                llm_result = await self.llm.chat_with_tools(system_prompt, user_query, tool_executor)
        else:
            agent_name = "supervisor"
            system_prompt = build_supervisor_prompt(episodic_context)
            llm_result = await self.llm.chat_with_tools(system_prompt, user_query, tool_executor)

        tools_used = llm_result.get("tools_used", [])
        tool_results = llm_result.get("tool_results", [])
        final_text = llm_result.get("final_text", "")

        # Formatter Agent: after all context/tools, produce clean final output
        if tool_results and final_text:
            try:
                from agents.formatter_agent import FormatterAgent
                formatter = FormatterAgent()
                formatted = await formatter.format(user_query, final_text, tool_results)
                if formatted and len(formatted.strip()) > 50:
                    final_text = formatted
            except Exception as e:
                print(f"  [supervisor] Formatter failed ({e}), using raw response")

        for tr in tool_results:
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

        # 4. Log to episodic memory
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

        # 5. Index episode into ChromaDB for vector search
        if self.vector_store:
            try:
                self.vector_store.index_episode(episode_id, user_query, findings_summary)
            except Exception:
                pass

        # 6. Check for state changes
        state_changes = self._detect_state_changes(entities, data_snapshot, episode_id)

        return {
            "message": final_text,
            "charts": charts,
            "memory_updates": {
                "episodic": {"logged": True, "episode_id": episode_id},
                "state_changes": state_changes,
            },
            "web_search_results": web_results,
            "tool_results": llm_result.get("tool_results", []),
            "procedure_used": procedure_used,
            "agent_used": agent_name,
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

    def _execute_tool(self, tool_name: str, args: dict, session_id: str | None = None) -> dict:
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
                result = execute_procedure(procedure, params)
                # Log effectiveness
                outcome = "informational"
                if isinstance(result, dict):
                    stuck_count = result.get("stuck_count", -1)
                    summary = result.get("summary", "").lower()
                    if stuck_count == 0:
                        outcome = "resolved"
                    elif "critical" in summary or stuck_count > 0:
                        outcome = "unresolved"
                self.procedural.log_execution(proc_name, params, outcome, session_id)
                return result

            elif tool_name == "update_semantic_knowledge":
                return self.semantic.update_knowledge(
                    category=args.get("category", "data_notes"),
                    key=args.get("key", ""),
                    value=args.get("value", ""),
                    reason=args.get("reason", ""),
                )

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
                           DART_GEN_DURATION, AVG_DART_GEN_DURATION,
                           DART_UI_VALIDATION_DURATION, AVG_DART_UI_VALIDATION_DURATION,
                           SPS_LOAD_DURATION, AVG_SPS_LOAD_DURATION,
                           ISF_GEN_DURATION, AVG_ISF_GEN_DURATION
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

    def _create_snapshot(self, entities: dict) -> dict:
        """Create a rich data snapshot for episodic memory — global + per-state."""
        from data_loader import query as db_query
        try:
            snapshot: dict = {
                "stuck_count": int(db_query("SELECT COUNT(*) as c FROM roster WHERE IS_STUCK=1").iloc[0]["c"]),
                "failed_count": int(db_query("SELECT COUNT(*) as c FROM roster WHERE IS_FAILED=1").iloc[0]["c"]),
                "stuck_by_state": {},
                "failed_by_state": {},
                "red_flag_by_state": {},
                "scs_percent_by_state": {},
                "top_failing_org_by_state": {},
            }

            stuck_df = db_query("SELECT CNT_STATE, COUNT(*) as cnt FROM roster WHERE IS_STUCK=1 GROUP BY CNT_STATE")
            for _, r in stuck_df.iterrows():
                snapshot["stuck_by_state"][r["CNT_STATE"]] = int(r["cnt"])

            failed_df = db_query("SELECT CNT_STATE, COUNT(*) as cnt FROM roster WHERE IS_FAILED=1 GROUP BY CNT_STATE")
            for _, r in failed_df.iterrows():
                snapshot["failed_by_state"][r["CNT_STATE"]] = int(r["cnt"])

            red_df = db_query("""
                SELECT CNT_STATE,
                  SUM(CASE WHEN PRE_PROCESSING_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN MAPPING_APROVAL_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN ISF_GEN_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN DART_GEN_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN DART_REVIEW_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN DART_UI_VALIDATION_HEALTH='RED' THEN 1 ELSE 0 END +
                      CASE WHEN SPS_LOAD_HEALTH='RED' THEN 1 ELSE 0 END) as red_total
                FROM roster GROUP BY CNT_STATE
            """)
            for _, r in red_df.iterrows():
                snapshot["red_flag_by_state"][r["CNT_STATE"]] = int(r["red_total"] or 0)

            scs_df = db_query(
                "SELECT MARKET, SCS_PERCENT FROM metrics WHERE (MARKET, MONTH) IN "
                "(SELECT MARKET, MAX(MONTH) FROM metrics GROUP BY MARKET)"
            )
            for _, r in scs_df.iterrows():
                snapshot["scs_percent_by_state"][r["MARKET"]] = float(r["SCS_PERCENT"])

            top_org_df = db_query("""
                SELECT CNT_STATE, ORG_NM FROM (
                    SELECT CNT_STATE, ORG_NM, COUNT(*) as failures,
                           ROW_NUMBER() OVER (PARTITION BY CNT_STATE ORDER BY COUNT(*) DESC) as rn
                    FROM roster WHERE IS_FAILED=1 GROUP BY CNT_STATE, ORG_NM
                ) t WHERE rn=1
            """)
            for _, r in top_org_df.iterrows():
                snapshot["top_failing_org_by_state"][r["CNT_STATE"]] = r["ORG_NM"]

            return snapshot
        except Exception:
            return {}

    def _detect_state_changes(self, entities: dict, current_snapshot: dict, episode_id: int) -> list:
        """Detect and log state changes compared to the global previous snapshot."""
        changes = []

        prev_snapshot = self.episodic.get_latest_rich_snapshot()

        if not prev_snapshot or "stuck_by_state" not in prev_snapshot:
            # First session or pre-upgrade snapshot — no comparison possible
            for ro_id in entities.get("ro_ids", []):
                prev = self.episodic.get_latest_snapshot_for_entity("ro", ro_id)
                if prev:
                    changes.append({"entity": ro_id, "field": "checked", "note": "Previously investigated"})
            return changes

        fields_to_compare = [
            ("stuck_by_state", "stuck_RO_count"),
            ("failed_by_state", "failed_RO_count"),
            ("red_flag_by_state", "red_flag_count"),
            ("scs_percent_by_state", "SCS_PERCENT"),
            ("top_failing_org_by_state", "top_failing_org"),
        ]

        for field_key, field_label in fields_to_compare:
            prev_map = prev_snapshot.get(field_key, {})
            curr_map = current_snapshot.get(field_key, {})
            all_states = set(list(prev_map.keys()) + list(curr_map.keys()))
            for state in all_states:
                old_val = prev_map.get(state)
                new_val = curr_map.get(state)
                if old_val is None or new_val is None or old_val == new_val:
                    continue
                self.episodic.log_state_change(
                    "market", state, field_label,
                    str(old_val), str(new_val), episode_id
                )
                narrative = self._format_change_narrative(state, field_label, old_val, new_val)
                changes.append({
                    "entity": state,
                    "field": field_label,
                    "old": old_val,
                    "new": new_val,
                    "narrative": narrative,
                })

        # Check stuck ROs
        for ro_id in entities.get("ro_ids", []):
            prev = self.episodic.get_latest_snapshot_for_entity("ro", ro_id)
            if prev:
                changes.append({"entity": ro_id, "field": "checked", "note": "Previously investigated"})

        return changes

    def _format_change_narrative(self, state: str, field: str, old, new) -> str:
        """Return a human-readable description of a state change."""
        if field == "stuck_RO_count":
            direction = "resolved" if new < old else "increased"
            return f"{state}: stuck ROs {direction} ({old} → {new})"
        elif field == "SCS_PERCENT":
            direction = "fell" if new < old else "rose"
            return f"{state}: SCS_PERCENT {direction} from {old:.1f}% → {new:.1f}%"
        elif field == "failed_RO_count":
            direction = "decreased" if new < old else "increased"
            return f"{state}: failed ROs {direction} ({old} → {new})"
        elif field == "red_flag_count":
            direction = "decreased" if new < old else "increased"
            return f"{state}: Red health flags {direction} ({old} → {new})"
        elif field == "top_failing_org":
            return f"{state}: top failing org changed from '{old}' → '{new}'"
        return f"{state}: {field} changed ({old} → {new})"

    async def generate_proactive_alerts(self) -> list:
        """Generate proactive monitoring alerts with intelligent trend detection."""
        from data_loader import query as db_query
        alerts = []

        # ── 1. Stuck ROs ──
        stuck = db_query("SELECT COUNT(*) as c FROM roster WHERE IS_STUCK = 1").iloc[0]["c"]
        if stuck > 0:
            stuck_details = db_query("""
                SELECT RO_ID, ORG_NM, CNT_STATE,
                       DATEDIFF('day', CAST(FILE_RECEIVED_DT AS TIMESTAMP), CURRENT_TIMESTAMP) as days
                FROM roster WHERE IS_STUCK = 1
                ORDER BY days DESC LIMIT 20
            """)
            alerts.append({
                "type": "stuck_ros",
                "severity": "high",
                "message": f"{int(stuck)} RO(s) are currently stuck in the pipeline",
                "recommended_action": "triage_stuck_ros",
                "recommended_params": {},
                "details": stuck_details.to_dict(orient="records"),
            })

        # ── 2. Markets below 95% SCS ──
        low_markets = db_query("""
            SELECT MARKET, SCS_PERCENT, MONTH
            FROM metrics WHERE SCS_PERCENT < 95
            ORDER BY SCS_PERCENT ASC
        """)
        if not low_markets.empty:
            worst = low_markets.iloc[0]
            alerts.append({
                "type": "low_scs",
                "severity": "medium",
                "message": f"{len(low_markets)} market-month entries below 95% SCS (worst: {worst['MARKET']} at {worst['SCS_PERCENT']}%)",
                "recommended_action": "market_health_report",
                "recommended_params": {"market": str(worst["MARKET"])},
                "details": low_markets.head(10).to_dict(orient="records"),
            })

        # ── 3. High failure rate states ──
        high_fail = db_query("""
            SELECT CNT_STATE,
                   ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as fail_rate
            FROM roster GROUP BY CNT_STATE
            HAVING fail_rate > 5 ORDER BY fail_rate DESC
        """)
        if not high_fail.empty:
            worst_state = high_fail.iloc[0]
            alerts.append({
                "type": "high_failure_rate",
                "severity": "medium",
                "message": f"{len(high_fail)} states have failure rates above 5% (worst: {worst_state['CNT_STATE']} at {worst_state['fail_rate']}%)",
                "recommended_action": "record_quality_audit",
                "recommended_params": {"state": str(worst_state["CNT_STATE"])},
                "details": high_fail.to_dict(orient="records"),
            })

        # ── 4. MoM Market Deterioration ──
        try:
            mom_df = db_query("""
                WITH ranked AS (
                    SELECT MARKET, MONTH, SCS_PERCENT, MONTH_DATE,
                           LAG(SCS_PERCENT) OVER (PARTITION BY MARKET ORDER BY MONTH_DATE) as prev_scs
                    FROM metrics
                )
                SELECT MARKET, MONTH, SCS_PERCENT, prev_scs,
                       ROUND(SCS_PERCENT - prev_scs, 2) as scs_change
                FROM ranked
                WHERE prev_scs IS NOT NULL AND (SCS_PERCENT - prev_scs) < -2
                ORDER BY scs_change ASC
            """)
            if not mom_df.empty:
                details = mom_df.head(10).to_dict(orient="records")
                worst_drop = mom_df.iloc[0]
                alerts.append({
                    "type": "mom_scs_decline",
                    "severity": "high",
                    "message": f"{worst_drop['MARKET']} SCS dropped {abs(worst_drop['scs_change'])}% MoM ({worst_drop['prev_scs']}% → {worst_drop['SCS_PERCENT']}%)",
                    "recommended_action": "market_health_report",
                    "recommended_params": {"market": str(worst_drop["MARKET"])},
                    "details": details,
                })
        except Exception:
            pass

        # ── 5. RED Stage Cluster Emergence ──
        try:
            stage_red = db_query("""
                SELECT STAGE_NM, RED_COUNT_TOTAL, TOTAL_ROS,
                       ROUND(RED_COUNT_TOTAL * 100.0 / NULLIF(TOTAL_ROS, 0), 2) as red_pct
                FROM stage_health_summary
                WHERE TOTAL_ROS > 10
                ORDER BY red_pct DESC
            """)
            if not stage_red.empty:
                avg_red_pct = stage_red["red_pct"].mean()
                hotspots = stage_red[stage_red["red_pct"] > avg_red_pct * 2]
                if not hotspots.empty:
                    alerts.append({
                        "type": "red_stage_cluster",
                        "severity": "high",
                        "message": f"{len(hotspots)} pipeline stage(s) have RED density >2x average ({avg_red_pct:.1f}%): {', '.join(hotspots['STAGE_NM'].tolist())}",
                        "recommended_action": "triage_stuck_ros",
                        "recommended_params": {},
                        "details": hotspots.to_dict(orient="records"),
                    })
        except Exception:
            pass

        # ── 6. Rejection Spikes by Org/Source System ──
        try:
            overall_fail_rate = db_query(
                "SELECT ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as r FROM roster"
            ).iloc[0]["r"]
            spike_threshold = float(overall_fail_rate) * 2

            org_spikes = db_query(f"""
                SELECT SRC_SYS, COUNT(*) as total_ros,
                       ROUND(SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as fail_rate
                FROM roster GROUP BY SRC_SYS
                HAVING total_ros > 20 AND fail_rate > {spike_threshold}
                ORDER BY fail_rate DESC
            """)
            if not org_spikes.empty:
                alerts.append({
                    "type": "source_system_spike",
                    "severity": "medium",
                    "message": f"{len(org_spikes)} source system(s) have failure rates >2x average ({overall_fail_rate}%): {', '.join(org_spikes['SRC_SYS'].tolist())}",
                    "recommended_action": "record_quality_audit",
                    "recommended_params": {},
                    "details": org_spikes.to_dict(orient="records"),
                })
        except Exception:
            pass

        # ── 7. Retry Deterioration ──
        try:
            retry_df = db_query("""
                WITH ranked AS (
                    SELECT MARKET, MONTH, RETRY_LIFT_PCT, MONTH_DATE,
                           LAG(RETRY_LIFT_PCT) OVER (PARTITION BY MARKET ORDER BY MONTH_DATE) as prev_lift
                    FROM metrics
                    WHERE RETRY_LIFT_PCT IS NOT NULL
                )
                SELECT MARKET, MONTH, RETRY_LIFT_PCT, prev_lift,
                       ROUND(RETRY_LIFT_PCT - prev_lift, 2) as lift_change
                FROM ranked
                WHERE prev_lift IS NOT NULL AND RETRY_LIFT_PCT < 0
                ORDER BY RETRY_LIFT_PCT ASC
            """)
            if not retry_df.empty:
                alerts.append({
                    "type": "retry_deterioration",
                    "severity": "medium",
                    "message": f"{len(retry_df)} market-month(s) where retries are making things worse (negative lift)",
                    "recommended_action": "retry_effectiveness_analysis",
                    "recommended_params": {},
                    "details": retry_df.head(10).to_dict(orient="records"),
                })
        except Exception:
            pass

        # ── 8. Repeated Failure Patterns ──
        try:
            repeat_df = db_query("""
                SELECT ORG_NM, LATEST_STAGE_NM, FAILURE_CATEGORY, COUNT(*) as repeat_count
                FROM roster
                WHERE IS_FAILED = 1 AND FAILURE_CATEGORY != 'NONE'
                GROUP BY ORG_NM, LATEST_STAGE_NM, FAILURE_CATEGORY
                HAVING repeat_count >= 3
                ORDER BY repeat_count DESC
                LIMIT 15
            """)
            if not repeat_df.empty:
                top = repeat_df.iloc[0]
                alerts.append({
                    "type": "repeated_failure_pattern",
                    "severity": "medium",
                    "message": f"{len(repeat_df)} org-stage-category combos have 3+ repeated failures (worst: {top['ORG_NM'][:30]} at {top['LATEST_STAGE_NM']} with {int(top['repeat_count'])}x {top['FAILURE_CATEGORY']})",
                    "recommended_action": "record_quality_audit",
                    "recommended_params": {},
                    "details": repeat_df.to_dict(orient="records"),
                })
        except Exception:
            pass

        # ── 9. Session history ──
        sessions = self.episodic.get_unique_sessions()
        if sessions:
            last_session = sessions[0]
            alerts.append({
                "type": "session_history",
                "severity": "info",
                "message": f"Last session: {last_session['query_count']} queries at {last_session['last_query']}",
                "recommended_action": None,
                "recommended_params": None,
                "details": last_session,
            })

        return alerts

    async def run_procedure(self, name: str, params: dict) -> dict:
        """Run a named procedure."""
        procedure = self.procedural.get_procedure(name)
        return execute_procedure(procedure, params)

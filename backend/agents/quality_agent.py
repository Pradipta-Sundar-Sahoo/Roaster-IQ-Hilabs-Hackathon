"""Record Quality Agent — failure rates, market metrics, retry effectiveness."""

import os
import google.generativeai as genai
from prompts import build_quality_prompt

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

QUALITY_TOOL_DECLARATIONS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="query_data",
                description=(
                    "Execute a SQL query (DuckDB). Use EXACT column names from the schema — "
                    "do NOT expand abbreviations. CRITICAL: CNT_STATE/MARKET=2-letter codes, "
                    "IS_FAILED/IS_STUCK/IS_RETRY are INTEGER (=1 not =TRUE), "
                    "no 'status' column (use IS_FAILED=1), no 'attempt_number' (use RUN_NO), "
                    "'table' is reserved. Join roster↔metrics on CNT_STATE=MARKET."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "sql": genai.protos.Schema(type=genai.protos.Type.STRING, description="SQL SELECT query"),
                    },
                    required=["sql"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="run_procedure",
                description=(
                    "Run a quality diagnostic procedure. Available: "
                    "record_quality_audit (failure rates by state/org, flags above threshold), "
                    "market_health_report (correlate SCS% with file failures — requires market param), "
                    "retry_effectiveness_analysis (compare first-pass vs retry success rates), "
                    "trace_root_cause (deep root cause analysis — traces low SCS% back through stages, source systems, LOBs; use for 'why', 'root cause', 'diagnose' queries; accepts state/market param), "
                    "rejection_pattern_clustering (cluster failures by type × org × LOB × source system — use for 'cluster', 'pattern', 'systemic' queries)."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "procedure_name": genai.protos.Schema(type=genai.protos.Type.STRING, description="record_quality_audit | market_health_report | retry_effectiveness_analysis | trace_root_cause | rejection_pattern_clustering"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON params e.g. {\"state\": \"NY\"} or {\"market\": \"NY\"}"),
                    },
                    required=["procedure_name"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="create_chart",
                description=(
                    "Create a quality or market visualization. "
                    "Types: failure_breakdown (failure types by state), "
                    "market_trend (SCS% over time by market), "
                    "retry_lift (first-iter vs overall success comparison)."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "chart_type": genai.protos.Schema(type=genai.protos.Type.STRING, description="failure_breakdown | market_trend | retry_lift"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON params e.g. {\"market\": \"New York\"}"),
                    },
                    required=["chart_type"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="web_search",
                description=(
                    "Search the web for regulatory context, CMS compliance rules, or LOB-specific "
                    "requirements that explain failure patterns. Use when FAIL_REC_CNT or REJ_REC_CNT "
                    "is elevated and a regulatory cause is suspected."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "query": genai.protos.Schema(type=genai.protos.Type.STRING, description="Search query"),
                        "search_type": genai.protos.Schema(type=genai.protos.Type.STRING, description="regulatory | compliance | lob | org | general"),
                    },
                    required=["query"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="update_semantic_knowledge",
                description=(
                    "Permanently store regulatory/compliance insights found via web search. "
                    "Call after web_search returns regulatory updates so the knowledge persists."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "category": genai.protos.Schema(type=genai.protos.Type.STRING, description="lob_meanings | failure_statuses | source_systems | pipeline_stages | data_notes"),
                        "key": genai.protos.Schema(type=genai.protos.Type.STRING, description="The key or name to add/update"),
                        "value": genai.protos.Schema(type=genai.protos.Type.STRING, description="The value or description to store"),
                        "reason": genai.protos.Schema(type=genai.protos.Type.STRING, description="Why this is being added, e.g. 'CMS ruling found via web search'"),
                    },
                    required=["category", "key", "value", "reason"],
                ),
            ),
        ]
    )
]


class QualityAgent:
    """Sub-agent for record quality, market metrics, and retry effectiveness."""

    def __init__(self):
        self.role = "record_quality"
        self.procedures = ["record_quality_audit", "market_health_report", "retry_effectiveness_analysis"]

    async def handle(self, user_query: str, tool_executor, episodic_context: str = "") -> dict:
        """Run quality-focused Gemini chat. Returns {final_text, tools_used, tool_results}."""
        from agents.llm_provider import LLMProvider
        llm = LLMProvider()

        system_prompt = build_quality_prompt()
        if episodic_context:
            system_prompt += f"\n\n{episodic_context}"

        return await llm.chat_with_tools(
            system_prompt, user_query, tool_executor,
            tool_declarations=QUALITY_TOOL_DECLARATIONS,
        )

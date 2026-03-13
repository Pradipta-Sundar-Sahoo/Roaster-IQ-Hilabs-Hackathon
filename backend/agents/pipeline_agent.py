"""Pipeline Health Agent — stuck ROs, stage durations, health flags."""

import json
import os
import google.generativeai as genai
from prompts import build_pipeline_prompt

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

PIPELINE_TOOL_DECLARATIONS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="query_data",
                description=(
                    "Execute a SQL query (DuckDB). Use EXACT column names from the schema — "
                    "do NOT expand abbreviations. CRITICAL: CNT_STATE=2-letter codes, "
                    "IS_STUCK/IS_FAILED/IS_RETRY are INTEGER (=1 not =TRUE), "
                    "no 'status' column (use IS_FAILED=1), no 'attempt_number' (use RUN_NO), "
                    "'table' is reserved."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "sql": genai.protos.Schema(type=genai.protos.Type.STRING, description="SQL SELECT query to execute"),
                    },
                    required=["sql"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="run_procedure",
                description="Run triage_stuck_ros to find and rank stuck/critical ROs by days stuck and Red health flags.",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "procedure_name": genai.protos.Schema(type=genai.protos.Type.STRING, description="Use: triage_stuck_ros"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON params e.g. {\"state\": \"NY\"}"),
                    },
                    required=["procedure_name"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="create_chart",
                description=(
                    "Create a pipeline visualization. "
                    "Types: stuck_tracker (stuck ROs by priority), "
                    "health_heatmap (org × stage health flags), "
                    "duration_anomaly (actual vs avg stage durations)."
                ),
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "chart_type": genai.protos.Schema(type=genai.protos.Type.STRING, description="stuck_tracker | health_heatmap | duration_anomaly"),
                        "params": genai.protos.Schema(type=genai.protos.Type.STRING, description="JSON params e.g. {\"state\": \"TN\"}"),
                    },
                    required=["chart_type"],
                ),
            ),
        ]
    )
]


class PipelineAgent:
    """Sub-agent for pipeline health: stuck ROs, stage durations, health flag patterns."""

    def __init__(self):
        self.role = "pipeline_health"
        self.procedures = ["triage_stuck_ros"]

    async def handle(self, user_query: str, tool_executor, episodic_context: str = "") -> dict:
        """Run pipeline-focused Gemini chat. Returns {final_text, tools_used, tool_results}."""
        from agents.llm_provider import LLMProvider
        llm = LLMProvider()

        system_prompt = build_pipeline_prompt()
        if episodic_context:
            system_prompt += f"\n\n{episodic_context}"

        return await llm.chat_with_tools(
            system_prompt, user_query, tool_executor,
            tool_declarations=PIPELINE_TOOL_DECLARATIONS,
        )

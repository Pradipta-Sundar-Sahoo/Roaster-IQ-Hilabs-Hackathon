"""Pipeline Health Agent — stuck ROs, stage durations, health flags."""

import json
import os
import google.generativeai as genai
from prompts import PIPELINE_AGENT_PROMPT

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

PIPELINE_TOOL_DECLARATIONS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="query_data",
                description=(
                    "Execute a SQL query against the roster table. Use DuckDB SQL syntax. "
                    "CRITICAL: (1) CNT_STATE uses 2-letter state codes (TN, NY, CA), NEVER full names. "
                    "(2) IS_STUCK/IS_FAILED/IS_RETRY are INTEGER — use =1 not =TRUE. "
                    "(3) No 'status' column — use IS_FAILED=1 or IS_STUCK=1. (4) No 'attempt_number' — use RUN_NO. "
                    "Key columns: RO_ID, ORG_NM, CNT_STATE, RUN_NO, IS_STUCK, IS_FAILED, FAILURE_STATUS, LATEST_STAGE_NM, "
                    "*_HEALTH (Green/Yellow/Red), *_DURATION, AVG_*_DURATION; "
                    "precomputed: DAYS_STUCK, RED_COUNT, YELLOW_COUNT, HEALTH_SCORE, PRIORITY, WORST_HEALTH_STAGE, IS_RETRY, FAILURE_CATEGORY."
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

        system_prompt = PIPELINE_AGENT_PROMPT
        if episodic_context:
            system_prompt += f"\n\n{episodic_context}"

        return await llm.chat_with_tools(
            system_prompt, user_query, tool_executor,
            tool_declarations=PIPELINE_TOOL_DECLARATIONS,
        )

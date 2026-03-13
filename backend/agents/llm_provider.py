"""LLM Provider — Gemini. OpenRouter methods retained for reference but not active."""

import json
import os
import re
import httpx
import google.generativeai as genai
from google.generativeai.types import generation_types

# Tool schemas for OpenRouter (OpenAI-compatible format)
OPENROUTER_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_data",
            "description": "Execute a SQL query (DuckDB). Use EXACT column names from the schema — do NOT expand abbreviations. CRITICAL: CNT_STATE/MARKET=2-letter codes, IS_FAILED/IS_STUCK/IS_RETRY are INTEGER (=1 not =TRUE), no 'status' column (use IS_FAILED=1), no 'attempt_number' (use RUN_NO), 'table' is reserved.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL SELECT query to execute"}
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for regulatory context, organization info, or compliance requirements.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "search_type": {"type": "string", "description": "Type: regulatory, org, compliance, lob, general"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_procedure",
            "description": "Run a named diagnostic procedure: triage_stuck_ros, record_quality_audit, market_health_report, retry_effectiveness_analysis.",
            "parameters": {
                "type": "object",
                "properties": {
                    "procedure_name": {"type": "string", "description": "Name of procedure to run"},
                    "params": {"type": "string", "description": "JSON string of parameters"},
                },
                "required": ["procedure_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_chart",
            "description": "Create a visualization. Types: health_heatmap, failure_breakdown, duration_anomaly, market_trend, retry_lift, stuck_tracker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {"type": "string", "description": "Type of chart"},
                    "params": {"type": "string", "description": "JSON string of chart parameters"},
                },
                "required": ["chart_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recall_memory",
            "description": "Search episodic memory for past investigations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "search_text": {"type": "string", "description": "Text to search for"},
                },
                "required": ["search_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_procedure",
            "description": "Update a diagnostic procedure based on user feedback.",
            "parameters": {
                "type": "object",
                "properties": {
                    "procedure_name": {"type": "string", "description": "Procedure to update"},
                    "change_description": {"type": "string", "description": "What to change"},
                    "new_step": {"type": "string", "description": "JSON of new step, if applicable"},
                },
                "required": ["procedure_name", "change_description"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_semantic_knowledge",
            "description": "Update the domain knowledge base when web search reveals new regulatory information, new LOB types, or failure patterns. Call after web_search returns regulatory updates.",
            "parameters": {
                "type": "object",
                "properties": {
                    "category": {"type": "string", "description": "Knowledge category: lob_meanings, failure_statuses, source_systems, pipeline_stages, data_notes"},
                    "key": {"type": "string", "description": "The specific key or name to add/update"},
                    "value": {"type": "string", "description": "The value or description to store"},
                    "reason": {"type": "string", "description": "Why this knowledge is being added"},
                },
                "required": ["category", "key", "value", "reason"],
            },
        },
    },
]


class LLMProvider:
    """Manages LLM calls via Gemini."""

    def __init__(self):
        self.openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.openrouter_model = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.5-flash-exp:free")
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

    async def chat_with_tools(
        self, system_prompt: str, user_query: str, tool_executor, tool_declarations=None
    ) -> dict:
        """Run a chat with tool calls. Returns dict with final_text, tools_used, tool_results."""
        return await self._gemini_chat(system_prompt, user_query, tool_executor, tool_declarations)

    async def extract_entities(self, prompt: str) -> dict:
        """Extract entities using Gemini."""
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt, generation_config=genai.GenerationConfig(temperature=0))
            text = response.text.strip()

            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception:
            return {}

    def _safe_send(self, chat, content, **kwargs):
        """Send message to Gemini, catching StopCandidateException gracefully."""
        try:
            return chat.send_message(content, **kwargs)
        except generation_types.StopCandidateException as e:
            candidate = e.args[0] if e.args else None
            fr = getattr(candidate, "finish_reason", "unknown")
            # Log what was malformed for debugging
            malformed_info = ""
            if candidate and hasattr(candidate, "content") and candidate.content:
                for p in candidate.content.parts:
                    if p.function_call and p.function_call.name:
                        malformed_info = f" (tried to call: {p.function_call.name})"
                    if p.text:
                        malformed_info += f" (partial text: {p.text[:100]})"
            print(f"  [Gemini stopped early: finish_reason={fr}{malformed_info}]")
            if candidate is not None:
                class _FakeResponse:
                    def __init__(self, c):
                        self.candidates = [c]
                return _FakeResponse(candidate)
            return None
        except Exception as e:
            print(f"  [Gemini error: {e}]")
            return None

    async def _gemini_chat(self, system_prompt: str, user_query: str, tool_executor, tool_declarations=None) -> dict:
        """Gemini function-calling chat loop with SQL self-correction."""
        if tool_declarations is None:
            from agents.supervisor import TOOL_DECLARATIONS
            tool_declarations = TOOL_DECLARATIONS

        model = genai.GenerativeModel("gemini-2.5-flash")
        chat = model.start_chat()

        response = self._safe_send(
            chat,
            [system_prompt, f"\nUser query: {user_query}"],
            tools=tool_declarations,
            generation_config=genai.GenerationConfig(temperature=0.1),
        )

        tools_used = []
        all_results = []
        collected_text = []  # Capture text from ALL responses, not just the last
        sql_retry_count = 0
        MAX_SQL_RETRIES = 5

        malformed_retries = 0
        MAX_MALFORMED_RETRIES = 3

        for _ in range(15):
            if response is None or not response.candidates:
                break

            candidate = response.candidates[0]
            finish_reason = getattr(candidate, "finish_reason", None)

            if finish_reason == 12:
                malformed_retries += 1
                print(f"  [Gemini MALFORMED_FUNCTION_CALL — recovery attempt {malformed_retries}/{MAX_MALFORMED_RETRIES}]")

                # Extract what Gemini was trying to do
                tried_tool = None
                partial_text_parts = []
                if candidate.content:
                    for p in candidate.content.parts:
                        if p.function_call and p.function_call.name:
                            tried_tool = p.function_call.name
                        if p.text and p.text.strip():
                            partial_text_parts.append(p.text.strip())

                # If there's partial text, capture it
                if partial_text_parts:
                    collected_text.extend(partial_text_parts)

                if malformed_retries > MAX_MALFORMED_RETRIES:
                    if all_results and not collected_text:
                        recovery_response = self._safe_send(
                            chat,
                            "Stop calling tools. Provide a detailed text analysis of ALL "
                            "the data you have already retrieved. Do NOT call any more tools.",
                            generation_config=genai.GenerationConfig(temperature=0.1),
                        )
                        if recovery_response and recovery_response.candidates:
                            for p in (recovery_response.candidates[0].content.parts
                                      if recovery_response.candidates[0].content else []):
                                if p.text and p.text.strip():
                                    collected_text.append(p.text.strip())
                    break

                # Build a context-aware recovery message
                if tried_tool:
                    recovery_msg = (
                        f"Your last function call to '{tried_tool}' was malformed and could not be parsed. "
                        f"If you still need to call {tried_tool}, format the arguments as valid JSON. "
                        f"Otherwise, if you have enough data, provide a text analysis instead."
                    )
                else:
                    recovery_msg = (
                        "Your last response was malformed. Either call a tool with properly "
                        "formatted JSON arguments, or provide a text response."
                    )

                response = self._safe_send(
                    chat,
                    recovery_msg,
                    tools=tool_declarations,
                    generation_config=genai.GenerationConfig(temperature=0.1),
                )
                continue

            # Successful (non-malformed) response — reset malformed counter
            malformed_retries = 0

            parts = candidate.content.parts if candidate.content else []
            function_calls = [p for p in parts if p.function_call and p.function_call.name]

            for p in parts:
                if p.text and p.text.strip():
                    collected_text.append(p.text.strip())

            if not function_calls:
                break

            tool_responses = []
            for part in function_calls:
                fc = part.function_call
                tool_name = fc.name
                tool_args = dict(fc.args) if fc.args else {}

                print(f"  Tool: {tool_name}({json.dumps(tool_args, default=str)[:120]})")
                tools_used.append(tool_name)
                result = tool_executor(tool_name, tool_args)

                is_sql_error = (
                    tool_name == "query_data"
                    and isinstance(result, dict)
                    and "error" in result
                    and result["error"].startswith("SQL_ERROR:")
                )

                if is_sql_error and sql_retry_count < MAX_SQL_RETRIES:
                    sql_retry_count += 1
                    print(f"  [SQL self-correction attempt {sql_retry_count}/{MAX_SQL_RETRIES}]: {result.get('error', '')[:120]}")
                    correction_payload = {
                        "error": result.get("error", ""),
                        "failed_sql": result.get("failed_sql", ""),
                        "hints": result.get("hints", {}),
                        "action_required": (
                            "The SQL above failed. Study the error, hints, and CORRECTIONS_REQUIRED carefully. "
                            "Use EXACT column names from the hints schema — do NOT expand abbreviations. "
                            "Rewrite the SQL fixing ALL identified issues and call query_data again. "
                            "Key rules: IS_FAILED=1 (not TRUE), IS_RETRY=1 (not TRUE), RUN_NO (not attempt_number), "
                            "no 'status' column, state codes like TN/NY (not full names), "
                            "'table' is a reserved keyword."
                        ),
                    }
                    result_str = json.dumps(correction_payload, default=str)[:5000]
                else:
                    all_results.append({"tool": tool_name, "args": tool_args, "result": result})
                    if isinstance(result, dict) and "data" in result and isinstance(result["data"], list):
                        compact = {"row_count": result.get("row_count", 0), "columns": result.get("columns", []), "sample": result["data"][:10]}
                        result_str = json.dumps(compact, default=str)[:3000]
                    else:
                        result_str = json.dumps(result, default=str)[:3000]

                tool_responses.append(
                    genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=tool_name, response={"result": result_str}
                        )
                    )
                )

            response = self._safe_send(
                chat, tool_responses,
                tools=tool_declarations,
                generation_config=genai.GenerationConfig(temperature=0.1),
            )

        final_text = "\n\n".join(collected_text)

        # Fallback: build a readable summary from tool results
        if not final_text and all_results:
            final_text = self._build_fallback_summary(all_results)

        return {"final_text": final_text, "tools_used": tools_used, "tool_results": all_results}

    @staticmethod
    def _build_fallback_summary(all_results: list) -> str:
        """Build a markdown summary from tool results when LLM doesn't generate text."""
        sections = []
        for r in all_results:
            res = r["result"]
            if not isinstance(res, dict):
                continue

            # Procedure results with summary
            if "summary" in res:
                sections.append(res["summary"])

            # Data results — render as markdown table
            if "data" in res and isinstance(res["data"], list) and res["data"]:
                rows = res["data"]
                cols = res.get("columns", list(rows[0].keys()))
                row_count = res.get("row_count", len(rows))

                # Build markdown table
                header = "| " + " | ".join(str(c) for c in cols) + " |"
                separator = "| " + " | ".join("---" for _ in cols) + " |"
                table_rows = []
                for row in rows[:20]:  # cap at 20 for readability
                    cells = []
                    for c in cols:
                        val = row.get(c)
                        if val is None:
                            cells.append("—")
                        elif isinstance(val, float):
                            cells.append(f"{val:.2f}")
                        else:
                            cells.append(str(val))
                    table_rows.append("| " + " | ".join(cells) + " |")

                table = "\n".join([header, separator] + table_rows)
                if row_count > 20:
                    table += f"\n\n*Showing 20 of {row_count} rows.*"
                sections.append(table)

            # Error results
            if "error" in res:
                sections.append(f"**Error:** {res['error']}")

        return "\n\n".join(sections) if sections else "Analysis complete. See the tool call details above for raw data."

    async def _openrouter_chat(self, system_prompt: str, user_query: str, tool_executor) -> dict:
        """OpenRouter chat with function calling."""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ]
        tools_used = []
        all_results = []
        max_iterations = 8

        for _ in range(max_iterations):
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.openrouter_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.openrouter_model,
                        "messages": messages,
                        "tools": OPENROUTER_TOOLS,
                        "temperature": 0.1,
                    },
                )
                data = resp.json()

            if "error" in data:
                raise Exception(f"OpenRouter error: {data['error']}")

            choice = data.get("choices", [{}])[0]
            message = choice.get("message", {})

            # Check for tool calls
            tool_calls = message.get("tool_calls", [])
            if not tool_calls:
                return {
                    "final_text": message.get("content", ""),
                    "tools_used": tools_used,
                    "tool_results": all_results,
                }

            # Add assistant message with tool calls
            messages.append(message)

            # Execute tool calls
            for tc in tool_calls:
                fn = tc.get("function", {})
                tool_name = fn.get("name", "")
                try:
                    tool_args = json.loads(fn.get("arguments", "{}"))
                except json.JSONDecodeError:
                    tool_args = {}

                tools_used.append(tool_name)
                result = tool_executor(tool_name, tool_args)
                all_results.append({"tool": tool_name, "args": tool_args, "result": result})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": json.dumps(result, default=str)[:3000],
                })

        return {"final_text": "", "tools_used": tools_used, "tool_results": all_results}

    async def _openrouter_simple(self, prompt: str) -> str:
        """Simple text generation via OpenRouter."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.openrouter_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.openrouter_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                },
            )
            data = resp.json()
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")

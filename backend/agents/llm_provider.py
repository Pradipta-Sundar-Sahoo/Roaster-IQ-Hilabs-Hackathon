"""LLM Provider — supports Gemini (primary) and OpenRouter (fallback)."""

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
            "description": "Execute a SQL query against the roster or metrics table. Use DuckDB SQL syntax.",
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
]


class LLMProvider:
    """Manages LLM calls with Gemini primary and OpenRouter fallback."""

    def __init__(self):
        self.provider = os.environ.get("LLM_PROVIDER", "gemini")
        self.openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.openrouter_model = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.5-flash-exp:free")

        if self.provider == "gemini":
            genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

    async def chat_with_tools(self, system_prompt: str, user_query: str, tool_executor) -> dict:
        """Run a chat with tool calls. Returns dict with final_text, tools_used, tool_results."""
        try:
            if self.provider == "gemini":
                return await self._gemini_chat(system_prompt, user_query, tool_executor)
            else:
                return await self._openrouter_chat(system_prompt, user_query, tool_executor)
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "quota" in error_str.lower():
                print(f"Gemini quota exceeded, falling back to OpenRouter...")
                if self.openrouter_key:
                    self.provider = "openrouter"
                    return await self._openrouter_chat(system_prompt, user_query, tool_executor)
            raise

    async def extract_entities(self, prompt: str) -> dict:
        """Extract entities using LLM."""
        try:
            if self.provider == "gemini":
                model = genai.GenerativeModel("gemini-2.5-flash")
                response = model.generate_content(prompt, generation_config=genai.GenerationConfig(temperature=0))
                text = response.text.strip()
            else:
                text = await self._openrouter_simple(prompt)

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
            print(f"  [Gemini stopped early: finish_reason={e.args[0].finish_reason}]")
            return None
        except Exception as e:
            print(f"  [Gemini error: {e}]")
            return None

    async def _gemini_chat(self, system_prompt: str, user_query: str, tool_executor) -> dict:
        """Gemini function-calling chat loop with error handling."""
        from agents.supervisor import TOOL_DECLARATIONS

        model = genai.GenerativeModel("gemini-2.5-flash")
        chat = model.start_chat()

        response = self._safe_send(
            chat,
            [system_prompt, f"\nUser query: {user_query}"],
            tools=TOOL_DECLARATIONS,
            generation_config=genai.GenerationConfig(temperature=0.1),
        )

        tools_used = []
        all_results = []

        for _ in range(6):
            if response is None or not response.candidates:
                break

            candidate = response.candidates[0]
            parts = candidate.content.parts
            function_calls = [p for p in parts if p.function_call and p.function_call.name]

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

            response = self._safe_send(chat, tool_responses)

        final_text = ""
        if response and response.candidates:
            for part in response.candidates[0].content.parts:
                if part.text:
                    final_text += part.text

        if not final_text and all_results:
            summaries = []
            for r in all_results:
                res = r["result"]
                if isinstance(res, dict) and "summary" in res:
                    summaries.append(res["summary"])
                elif isinstance(res, dict) and "data" in res:
                    summaries.append(f"{r['tool']}: {res.get('row_count', '?')} rows returned")
            final_text = "Here are the results from my analysis:\n\n" + "\n".join(summaries) if summaries else "Analysis complete. See charts and data below."

        return {"final_text": final_text, "tools_used": tools_used, "tool_results": all_results}

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

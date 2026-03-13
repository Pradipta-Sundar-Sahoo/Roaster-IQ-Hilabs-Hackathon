"""Formatter Agent — produces the final user-facing response after all context and tool results are gathered."""

import asyncio
import json
import os
import re

import google.generativeai as genai

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))


FORMATTER_PROMPT = """You are the RosterIQ Formatter Agent. Your job is to turn raw agent output and tool results into a clean, professional response for the user.

## Input
- User query
- Draft response (may contain play-by-play narration, redundant tool summaries)
- Tool results (SQL, web search, procedure outputs — already summarized below)

## Output
Produce a single coherent response that:
1. Leads with the key finding or answer
2. Organizes content by topic, not by tool call order
3. Never narrates process ("I searched for...", "The query returned...", "I will now...")
4. Uses domain context to interpret numbers (e.g., "COMPLETE VALIDATION FAILURE suggests schema mismatch")
5. Ends with actionable recommendations when relevant

Be concise. Omit failed or irrelevant web searches. If SQL returned no rows, state that briefly. Do not repeat the same data in multiple ways.

## User Query
{query}

## Draft from Main Agent
{draft}

## Tool Results (for synthesis)
{tool_summary}

Return ONLY the final formatted response. No preamble, no "Here is...", no markdown headers unless listing items."""


class FormatterAgent:
    """Takes gathered context and tool results, produces a clean final response."""

    def __init__(self):
        self.model = genai.GenerativeModel("gemini-2.5-flash")

    def _summarize_tool_results(self, tool_results: list) -> str:
        """Build a concise summary of tool outputs for the formatter."""
        if not tool_results:
            return "(No tool results)"

        sections = []
        for tr in tool_results:
            tool_name = tr.get("tool", "")
            args = tr.get("args", {})
            result = tr.get("result", {})

            if tool_name == "query_data":
                sql = args.get("sql", "")[:80]
                if "error" in result:
                    sections.append(f"SQL failed: {result.get('error', '')[:150]}")
                else:
                    data = result.get("data", [])
                    cols = result.get("columns", [])
                    row_count = result.get("row_count", len(data))
                    if data:
                        header = " | ".join(str(c) for c in cols)
                        rows = []
                        for r in data[:15]:
                            cells = [str(r.get(c, ""))[:30] for c in cols]
                            rows.append(" | ".join(cells))
                        table = header + "\n" + "\n".join(rows)
                        if row_count > 15:
                            table += f"\n... ({row_count} total rows)"
                        sections.append(f"SQL ({row_count} rows):\n{table}")
                    else:
                        sections.append(f"SQL returned 0 rows. Query: {sql}")

            elif tool_name == "web_search":
                results_list = result.get("results", [])
                if results_list:
                    for r in results_list[:3]:
                        title = r.get("title", "")[:80]
                        content = r.get("content", "")[:200]
                        sections.append(f"Web: {title}\n{content}")
                else:
                    sections.append("Web search: no relevant results found")

            elif tool_name == "run_procedure":
                proc = args.get("procedure_name", "")
                summary = result.get("summary", "")
                stuck = result.get("stuck_count", result.get("stuck_ros", []))
                if isinstance(stuck, list):
                    stuck = len(stuck)
                sections.append(f"Procedure {proc}: {summary or str(result)[:300]}")

            elif tool_name == "create_chart":
                sections.append(f"Chart created: {args.get('chart_type', '')}")

            else:
                sections.append(f"{tool_name}: {str(result)[:200]}")

        return "\n\n---\n\n".join(sections)[:8000]

    async def format(
        self,
        query: str,
        draft_response: str,
        tool_results: list,
    ) -> str:
        """
        Produce the final formatted response from draft and tool results.
        Returns the clean, synthesized text for the user.
        """
        if not tool_results:
            return draft_response

        tool_summary = self._summarize_tool_results(tool_results)

        try:
            prompt = FORMATTER_PROMPT.format(
                query=query,
                draft=draft_response or "(No draft provided)",
                tool_summary=tool_summary,
            )

            def _generate():
                resp = self.model.generate_content(
                    prompt,
                    generation_config=genai.GenerationConfig(temperature=0.2),
                )
                return resp.text.strip()

            text = await asyncio.to_thread(_generate)
            return text
        except Exception as e:
            print(f"  [formatter] Failed ({e}), falling back to draft")
            return draft_response

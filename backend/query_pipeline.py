"""Multi-path query pipeline — classify, route, combine, judge, generate."""

import asyncio
import json
import re

import google.generativeai as genai

from prompts_pipeline import RESPONSE_SYSTEM_PROMPT, build_classifier_prompt, build_sufficiency_prompt
from prompts import build_supervisor_prompt

MAX_REFINEMENT_LOOPS = 3


class QueryPipeline:
    """Routes queries through multiple retrieval paths before generating a response."""

    def __init__(self, vector_store, episodic_memory, semantic_memory, llm_provider):
        self.vector_store = vector_store
        self.episodic = episodic_memory
        self.semantic = semantic_memory
        self.llm = llm_provider
        self.model = genai.GenerativeModel("gemini-2.5-flash")

    async def process(self, query: str, session_id: str, tool_executor, episodic_context: str = "") -> dict:
        """Main pipeline entry point. Returns dict with final_text, tools_used, tool_results."""

        # 1. Classify the query
        classification = await self._classify(query)
        print(f"  [pipeline] Classification: {json.dumps(classification, default=str)}")

        # Short-circuit for conversational queries
        if classification.get("is_conversational"):
            return await self._generate_simple_response(query, tool_executor, episodic_context)

        # 2. Route — execute activated paths in parallel
        paths = classification.get("paths", ["sql"])
        contexts = await self._route(paths, query, classification, tool_executor)

        # 3. Combine contexts
        combined = self._combine_contexts(contexts)

        # 4. Judge sufficiency (up to MAX_REFINEMENT_LOOPS refinement loops)
        for attempt in range(MAX_REFINEMENT_LOOPS):
            judgment = await self._judge_sufficiency(query, combined)
            sufficient = judgment.get("sufficient", True)
            print(f"  [pipeline] Sufficiency (attempt {attempt + 1}/{MAX_REFINEMENT_LOOPS}): {sufficient}")

            if sufficient:
                break

            refined = False

            # 4a. Refine via SQL if suggested
            refined_sql = judgment.get("refined_sql", "").strip()
            if refined_sql:
                extra = self._execute_sql_path(refined_sql, tool_executor)
                combined += "\n\n### Refined SQL Results (attempt " + str(attempt + 1) + ")\n" + extra.get("text", "")
                if "tool_result" in extra:
                    contexts.setdefault("_tool_results", []).append(extra["tool_result"])
                refined = True

            # 4b. Refine via vector search if suggested
            refined_vector = judgment.get("refined_vector_query", "").strip()
            if refined_vector:
                extra_vec = self._run_vector_path(refined_vector)
                combined += "\n\n### Refined Vector Results (attempt " + str(attempt + 1) + ")\n" + extra_vec.get("text", "")
                refined = True

            # 4c. Refine via history search if suggested
            refined_history = judgment.get("refined_history_query", "").strip()
            if refined_history:
                extra_hist = self._run_history_path(refined_history)
                combined += "\n\n### Refined History Results (attempt " + str(attempt + 1) + ")\n" + extra_hist.get("text", "")
                refined = True

            # 4d. If judge said insufficient but gave no refinement queries, auto-generate SQL
            if not refined:
                missing = judgment.get("missing", "")
                if missing:
                    auto_sql = await self._generate_refinement_sql(query, missing, combined)
                    if auto_sql:
                        extra = self._execute_sql_path(auto_sql, tool_executor)
                        combined += "\n\n### Auto-Refined SQL Results (attempt " + str(attempt + 1) + ")\n" + extra.get("text", "")
                        if "tool_result" in extra:
                            contexts.setdefault("_tool_results", []).append(extra["tool_result"])
                        refined = True

            if not refined:
                print(f"  [pipeline] No refinement possible, proceeding with available context")
                break

        # 5. Generate final response with enriched context
        return await self._generate_response(query, combined, tool_executor, episodic_context, contexts)

    # ------------------------------------------------------------------
    # Step 1: Classify
    # ------------------------------------------------------------------

    async def _classify(self, query: str) -> dict:
        """Use LLM to classify which retrieval paths the query needs."""
        try:
            prompt = build_classifier_prompt(query)
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0),
            )
            text = response.text.strip()
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"  [pipeline] Classification failed ({e}), defaulting to sql+vector")
            return {"paths": ["sql", "vector"], "sql_hint": "", "vector_query": query, "is_conversational": False}

    # ------------------------------------------------------------------
    # Step 2: Route
    # ------------------------------------------------------------------

    async def _route(self, paths: list, query: str, classification: dict, tool_executor) -> dict:
        """Execute activated retrieval paths in parallel. Returns dict of path_name -> context."""
        contexts = {"_tool_results": []}

        tasks = {}
        if "sql" in paths:
            tasks["sql"] = asyncio.to_thread(
                self._run_sql_path, query, classification, tool_executor
            )
        if "vector" in paths:
            vector_query = classification.get("vector_query") or query
            tasks["vector"] = asyncio.to_thread(
                self._run_vector_path, vector_query
            )
        if "history" in paths:
            tasks["history"] = asyncio.to_thread(
                self._run_history_path, query
            )

        if tasks:
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            for key, result in zip(tasks.keys(), results):
                if isinstance(result, Exception):
                    print(f"  [pipeline] {key} path failed: {result}")
                    contexts[key] = {"text": f"[{key} path error: {result}]"}
                else:
                    contexts[key] = result
                    if key == "sql" and "tool_result" in result:
                        contexts["_tool_results"].append(result["tool_result"])

        return contexts

    def _run_sql_path(self, query: str, classification: dict, tool_executor) -> dict:
        """Execute the SQL retrieval path."""
        sql_hint = classification.get("sql_hint", "")
        if sql_hint:
            return self._execute_sql_path(sql_hint, tool_executor)

        # No SQL hint — let the LLM decide later in the response generation step
        return {"text": "[SQL path: no pre-query hint provided — LLM will generate SQL via tools]"}

    def _execute_sql_path(self, sql: str, tool_executor, _retry_depth: int = 0) -> dict:
        """Run a SQL query with self-correction. On error, uses LLM to fix the SQL and retries."""
        from tools.data_query import execute_sql
        MAX_SQL_SELF_CORRECTIONS = 3

        result = execute_sql(sql)

        if "error" in result:
            if _retry_depth >= MAX_SQL_SELF_CORRECTIONS:
                print(f"  [pipeline] SQL self-correction exhausted ({MAX_SQL_SELF_CORRECTIONS} attempts)")
                return {
                    "text": f"SQL query failed after {MAX_SQL_SELF_CORRECTIONS} correction attempts.\n"
                            f"Last error: {result['error']}\nLast SQL: {sql}",
                    "tool_result": {"tool": "query_data", "args": {"sql": sql}, "result": result},
                }

            corrected = self._self_correct_sql(sql, result)
            if corrected and corrected != sql:
                print(f"  [pipeline] SQL self-correction attempt {_retry_depth + 1}/{MAX_SQL_SELF_CORRECTIONS}: retrying")
                return self._execute_sql_path(corrected, tool_executor, _retry_depth + 1)

            return {
                "text": f"SQL query failed: {result['error']}\nFailed SQL: {sql}",
                "tool_result": {"tool": "query_data", "args": {"sql": sql}, "result": result},
            }

        data = result.get("data", [])
        row_count = result.get("row_count", 0)
        columns = result.get("columns", [])

        if not data:
            return {"text": f"SQL query returned 0 rows.\nQuery: {sql}", "tool_result": {"tool": "query_data", "args": {"sql": sql}, "result": result}}

        header = "| " + " | ".join(str(c) for c in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        rows_text = []
        for row in data[:20]:
            cells = []
            for c in columns:
                val = row.get(c)
                if val is None:
                    cells.append("—")
                elif isinstance(val, float):
                    cells.append(f"{val:.2f}")
                else:
                    cells.append(str(val)[:50])
            rows_text.append("| " + " | ".join(cells) + " |")

        table = "\n".join([header, separator] + rows_text)
        if row_count > 20:
            table += f"\n*Showing 20 of {row_count} rows.*"

        text = f"### SQL Results ({row_count} rows)\nQuery: `{sql}`\n\n{table}"
        return {"text": text, "tool_result": {"tool": "query_data", "args": {"sql": sql}, "result": result}}

    def _self_correct_sql(self, failed_sql: str, error_result: dict) -> str | None:
        """Use LLM to fix a failed SQL query based on the error and schema hints."""
        from schema_provider import get_schema_text

        error_msg = error_result.get("error", "")
        hints = error_result.get("hints", {})
        corrections = hints.get("CORRECTIONS_REQUIRED", [])
        tip = hints.get("tip", "")

        prompt = (
            "You are a DuckDB SQL fixer. A SQL query failed. Fix it.\n\n"
            f"FAILED SQL:\n{failed_sql}\n\n"
            f"ERROR:\n{error_msg}\n\n"
            f"FULL DATABASE SCHEMA (use EXACT column names from here):\n{get_schema_text()}\n\n"
        )
        if corrections:
            prompt += "REQUIRED CORRECTIONS:\n" + "\n".join(f"- {c}" for c in corrections) + "\n\n"
        if tip:
            prompt += f"TIP: {tip}\n\n"

        prompt += "Return ONLY the corrected SQL SELECT query. No explanation, no markdown fences."

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0),
            )
            sql = response.text.strip()
            sql = re.sub(r"```sql\s*", "", sql)
            sql = re.sub(r"```\s*", "", sql)
            if sql.upper().startswith("SELECT"):
                print(f"  [pipeline] Self-corrected SQL: {sql[:120]}")
                return sql
        except Exception as e:
            print(f"  [pipeline] SQL self-correction LLM call failed: {e}")

        return None

    def _run_vector_path(self, query: str) -> dict:
        """Execute the vector search retrieval path."""
        results = self.vector_store.search_all(query, n_results=3)

        sections = []
        for source, matches in results.items():
            if matches:
                lines = [f"### Vector Search — {source}"]
                for m in matches:
                    dist = f" (distance: {m['distance']:.3f})" if m.get("distance") is not None else ""
                    lines.append(f"- {m['text']}{dist}")
                sections.append("\n".join(lines))

        text = "\n\n".join(sections) if sections else "[Vector search: no relevant matches found]"
        return {"text": text}

    def _run_history_path(self, query: str) -> dict:
        """Execute the history retrieval path."""
        # Semantic search over past episodes (uses Gemini embeddings)
        episodes = self.episodic.search_semantic(query, limit=3)

        # Also get recent findings
        recent = self.episodic.get_recent_findings(limit=5)

        sections = []

        if episodes:
            lines = ["### Past Investigations (semantic match)"]
            for ep in episodes:
                ts = str(ep.get("timestamp", ""))[:19]
                q = str(ep.get("query", ""))[:100]
                findings = str(ep.get("findings_summary", ""))[:300]
                source = ep.get("_source", "episode")
                prefix = "[DIGEST] " if source == "digest" else ""
                lines.append(f"- [{ts}] {prefix}\"{q}\"")
                if findings:
                    lines.append(f"  Findings: {findings}")
            sections.append("\n".join(lines))

        if recent:
            lines = ["### Recent Investigations"]
            for r in recent[:3]:
                ts = str(r.get("timestamp", ""))[:19]
                q = str(r.get("query", ""))[:80]
                findings = str(r.get("findings_summary", ""))[:200]
                lines.append(f"- [{ts}] \"{q}\" → {findings}")
            sections.append("\n".join(lines))

        text = "\n\n".join(sections) if sections else "[History: no relevant past investigations found]"
        return {"text": text}

    # ------------------------------------------------------------------
    # Step 3: Combine
    # ------------------------------------------------------------------

    def _combine_contexts(self, contexts: dict | list) -> str:
        """Merge all path results into a single context string."""
        if isinstance(contexts, list):
            return "\n\n---\n\n".join(str(c) for c in contexts)

        sections = []
        for key, ctx in contexts.items():
            if key.startswith("_"):
                continue
            if isinstance(ctx, dict):
                text = ctx.get("text", str(ctx))
            else:
                text = str(ctx)
            if not text:
                continue
            # Skip empty-result placeholders like "[Vector search: no relevant matches found]"
            if text.startswith("[") and "no relevant" in text:
                continue
            sections.append(text)

        return "\n\n---\n\n".join(sections) if sections else ""

    # ------------------------------------------------------------------
    # Step 4: Judge sufficiency
    # ------------------------------------------------------------------

    async def _judge_sufficiency(self, query: str, combined_context: str) -> dict:
        """Ask the LLM if we have enough context to answer the query."""
        if not combined_context or len(combined_context.strip()) < 20:
            return {"sufficient": False, "missing": "No meaningful context gathered", "refined_sql": ""}

        try:
            prompt = build_sufficiency_prompt(query, combined_context[:6000])
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0),
            )
            text = response.text.strip()
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"  [pipeline] Sufficiency judge failed ({e}), assuming sufficient")
            return {"sufficient": True, "reason": "Judge error — proceeding with available context"}

    async def _generate_refinement_sql(self, query: str, missing: str, existing_context: str) -> str:
        """Auto-generate a SQL query to fill gaps identified by the sufficiency judge."""
        from schema_provider import get_schema_text
        try:
            prompt = (
                "You are a SQL query generator for RosterIQ (DuckDB).\n\n"
                f"DATABASE SCHEMA:\n{get_schema_text()}\n\n"
                f"User question: {query}\n"
                f"Missing data: {missing}\n"
                f"Already gathered (summary): {existing_context[:1500]}\n\n"
                "Generate EXACTLY ONE SQL SELECT query to fill the gap. "
                "Return ONLY the SQL, no explanation, no markdown."
            )
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0),
            )
            sql = response.text.strip()
            sql = re.sub(r"```sql\s*", "", sql)
            sql = re.sub(r"```\s*", "", sql)
            if sql.upper().startswith("SELECT"):
                return sql
        except Exception as e:
            print(f"  [pipeline] Auto-refinement SQL generation failed: {e}")
        return ""

    # ------------------------------------------------------------------
    # Step 5: Generate response
    # ------------------------------------------------------------------

    async def _generate_response(self, query: str, combined_context: str, tool_executor, episodic_context: str = "", contexts: dict = None) -> dict:
        """Generate the final response using chat_with_tools with enriched context."""
        base_prompt = build_supervisor_prompt(episodic_context)

        # Track which SQL queries were already executed so the LLM doesn't re-run them
        already_executed = []
        if contexts and "_tool_results" in contexts:
            for tr in contexts["_tool_results"]:
                sql = tr.get("args", {}).get("sql", "")
                if sql:
                    already_executed.append(sql)

        dedup_note = ""
        if already_executed:
            dedup_note = (
                "\n\n## Already-Executed Queries (DO NOT re-run these)\n"
                + "\n".join(f"- `{s}`" for s in already_executed)
                + "\nThe results are in the Pre-Gathered Context above. Only run NEW queries for additional data."
            )

        system_prompt = RESPONSE_SYSTEM_PROMPT.format(
            base_system_prompt=base_prompt,
            combined_context=combined_context[:8000] + dedup_note,
        )

        llm_result = await self.llm.chat_with_tools(system_prompt, query, tool_executor)

        # Merge pre-fetched tool results with any new ones from the LLM
        if contexts and "_tool_results" in contexts:
            existing_results = llm_result.get("tool_results", [])
            pre_fetched = contexts["_tool_results"]
            llm_result["tool_results"] = pre_fetched + existing_results

        return llm_result

    async def _generate_simple_response(self, query: str, tool_executor, episodic_context: str = "") -> dict:
        """Handle conversational queries without retrieval."""
        system_prompt = build_supervisor_prompt(episodic_context)
        return await self.llm.chat_with_tools(system_prompt, query, tool_executor)

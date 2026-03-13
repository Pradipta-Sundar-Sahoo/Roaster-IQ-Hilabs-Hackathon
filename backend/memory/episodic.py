"""Episodic Memory — SQLite-backed persistent memory of past interactions."""

import json
import sqlite3
from datetime import datetime, timedelta


class EpisodicMemory:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        # isolation_level=None enables autocommit — DDL is written immediately
        conn = sqlite3.connect(self.db_path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                session_id TEXT NOT NULL,
                query TEXT NOT NULL,
                intent TEXT,
                entities_json TEXT,
                findings_summary TEXT,
                tools_used TEXT,
                procedure_used TEXT,
                data_snapshot_json TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                entity_type TEXT,
                entity_id TEXT,
                field TEXT,
                old_value TEXT,
                new_value TEXT,
                detected_in_episode_id INTEGER,
                FOREIGN KEY (detected_in_episode_id) REFERENCES episodes(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS episode_digests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                session_range TEXT,
                episode_ids_json TEXT,
                intent_group TEXT,
                digest_text TEXT NOT NULL,
                embedding_json TEXT,
                episode_count INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_episodes_session
            ON episodes(session_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_state_changes_entity
            ON state_changes(entity_type, entity_id)
        """)

        # Schema migrations — safe via try/except for existing DBs
        for ddl in [
            "ALTER TABLE episodes ADD COLUMN embedding_json TEXT",
            "ALTER TABLE episodes ADD COLUMN importance_score REAL DEFAULT 0.0",
        ]:
            try:
                conn.execute(ddl)
            except Exception:
                pass  # Column already exists

        conn.close()

        # Verify tables were actually created — if not, something went wrong
        self._verify_schema()

    def _verify_schema(self):
        """Confirm required tables exist; re-create if missing (handles corrupted/empty DB)."""
        try:
            conn = sqlite3.connect(self.db_path)
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            conn.close()
            required = {"episodes", "state_changes", "episode_digests"}
            if not required.issubset(tables):
                print(f"[episodic] WARNING: missing tables {required - tables}, re-running _init_db")
                import os
                os.remove(self.db_path)
                self._init_db()
        except Exception as e:
            print(f"[episodic] Schema verify failed: {e}")

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------

    def _generate_embedding(self, text: str) -> list | None:
        """Generate embedding using Gemini text-embedding-004. Returns None on failure."""
        try:
            import google.generativeai as genai
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=text[:2000],
            )
            return result["embedding"]
        except Exception:
            return None

    def _compute_importance(
        self,
        tools_used: list,
        procedure_used: str,
        findings_summary: str,
    ) -> float:
        """Score 0.0–1.0 based on how significant the episode was."""
        score = 0.0
        if tools_used:
            score += len([t for t in tools_used if t in ("web_search", "run_procedure")]) * 0.2
            score += min(tools_used.count("create_chart") * 0.15, 0.3)
        if procedure_used:
            score += 0.25
        if findings_summary:
            red_count = (
                findings_summary.lower().count("red")
                + findings_summary.lower().count("critical")
                + findings_summary.lower().count("stuck")
            )
            score += min(red_count * 0.1, 0.3)
        return min(round(score, 3), 1.0)

    # ------------------------------------------------------------------
    # Core logging
    # ------------------------------------------------------------------

    def log_episode(
        self,
        session_id: str,
        query: str,
        intent: str = None,
        entities: dict = None,
        findings_summary: str = None,
        tools_used: list = None,
        procedure_used: str = None,
        data_snapshot: dict = None,
    ) -> int:
        """Log a new episode and return its ID."""
        # Trigger consolidation if episode count is growing large
        self._maybe_consolidate()

        importance = self._compute_importance(tools_used or [], procedure_used, findings_summary or "")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            """
            INSERT INTO episodes
            (timestamp, session_id, query, intent, entities_json, findings_summary,
             tools_used, procedure_used, data_snapshot_json, importance_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                session_id,
                query,
                intent,
                json.dumps(entities) if entities else None,
                findings_summary,
                ",".join(tools_used) if tools_used else None,
                procedure_used,
                json.dumps(data_snapshot) if data_snapshot else None,
                importance,
            ),
        )
        episode_id = cursor.lastrowid
        conn.commit()

        # Generate and store embedding (best-effort)
        embedding_text = f"{query} {findings_summary or ''}"
        embedding = self._generate_embedding(embedding_text)
        if embedding:
            conn.execute(
                "UPDATE episodes SET embedding_json = ? WHERE id = ?",
                (json.dumps(embedding), episode_id),
            )
            conn.commit()

        conn.close()
        return episode_id

    def log_state_change(
        self,
        entity_type: str,
        entity_id: str,
        field: str,
        old_value: str,
        new_value: str,
        episode_id: int = None,
    ):
        """Log a detected state change."""
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """
            INSERT INTO state_changes
            (timestamp, entity_type, entity_id, field, old_value, new_value, detected_in_episode_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                entity_type,
                entity_id,
                field,
                str(old_value),
                str(new_value),
                episode_id,
            ),
        )
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Semantic search
    # ------------------------------------------------------------------

    def search_semantic(self, query_text: str, limit: int = 5) -> list[dict]:
        """Embed query and rank episodes by cosine similarity × importance_score.
        Falls back to LIKE search if embeddings are unavailable."""
        try:
            # Quick schema check before any work
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT 1 FROM episodes LIMIT 1")
            conn.close()
        except sqlite3.OperationalError:
            print("[episodic] Table missing in search_semantic — reinitializing DB")
            self._init_db()
            return []

        query_embedding = self._generate_embedding(query_text)
        if query_embedding is None:
            return self.search_by_query_text(query_text, limit=limit)

        try:
            import numpy as np
        except ImportError:
            return self.search_by_query_text(query_text, limit=limit)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM episodes WHERE embedding_json IS NOT NULL ORDER BY timestamp DESC LIMIT 200"
        ).fetchall()
        conn.close()

        q_vec = np.array(query_embedding)
        scored = []
        for row in rows:
            ep = dict(row)
            try:
                ep_vec = np.array(json.loads(ep["embedding_json"]))
                cosine = float(
                    np.dot(q_vec, ep_vec)
                    / (np.linalg.norm(q_vec) * np.linalg.norm(ep_vec) + 1e-9)
                )
                importance = float(ep.get("importance_score") or 0.0)
                ep["_score"] = cosine * 0.7 + importance * 0.3
                ep["_source"] = "episode"
                scored.append(ep)
            except Exception:
                continue

        # Also include digests in the search
        digest_results = self._search_digests_semantic(q_vec, np)
        scored.extend(digest_results)

        scored.sort(key=lambda x: x["_score"], reverse=True)
        return scored[:limit]

    def _search_digests_semantic(self, q_vec, np) -> list[dict]:
        """Search episode_digests with cosine similarity."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM episode_digests WHERE embedding_json IS NOT NULL ORDER BY timestamp DESC LIMIT 50"
        ).fetchall()
        conn.close()

        scored = []
        for row in rows:
            d = dict(row)
            try:
                d_vec = np.array(json.loads(d["embedding_json"]))
                cosine = float(
                    np.dot(q_vec, d_vec)
                    / (np.linalg.norm(q_vec) * np.linalg.norm(d_vec) + 1e-9)
                )
                d["_score"] = cosine * 0.7 + 0.3 * 0.3
                d["_source"] = "digest"
                # Normalize to episode-like structure for prompt formatting
                d["query"] = f"[DIGEST: {d.get('intent_group', 'general')}] {d.get('session_range', '')}"
                d["findings_summary"] = d.get("digest_text", "")
                d["timestamp"] = d.get("timestamp", "")
                d["intent"] = d.get("intent_group", "")
                scored.append(d)
            except Exception:
                continue
        return scored

    # ------------------------------------------------------------------
    # Legacy text search (fallback)
    # ------------------------------------------------------------------

    def get_latest_rich_snapshot(self) -> dict | None:
        """Get the most recent episode snapshot that contains full per-state data."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT data_snapshot_json FROM episodes WHERE data_snapshot_json LIKE '%stuck_by_state%' ORDER BY timestamp DESC LIMIT 1"
        ).fetchall()
        conn.close()
        if rows and rows[0]["data_snapshot_json"]:
            try:
                return json.loads(rows[0]["data_snapshot_json"])
            except Exception:
                return None
        return None

    def search_by_entities(self, entities: dict, limit: int = 5) -> list[dict]:
        """Search past episodes matching any of the given entities."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conditions = []
        params = []

        for key, values in entities.items():
            if isinstance(values, list):
                for v in values:
                    conditions.append("entities_json LIKE ?")
                    params.append(f"%{v}%")
            elif values:
                conditions.append("entities_json LIKE ?")
                params.append(f"%{values}%")

        if not conditions:
            conn.close()
            return []

        where_clause = " OR ".join(conditions)
        rows = conn.execute(
            f"""
            SELECT * FROM episodes
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def search_by_query_text(self, text: str, limit: int = 5) -> list[dict]:
        """Search past episodes by query text similarity."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT * FROM episodes
                WHERE query LIKE ? OR findings_summary LIKE ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (f"%{text}%", f"%{text}%", limit),
            ).fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print(f"[episodic] Table missing in search_by_query_text — reinitializing DB")
                self._init_db()
                return []
            raise

    # ------------------------------------------------------------------
    # Session briefing
    # ------------------------------------------------------------------

    def generate_session_briefing(self, current_session_id: str) -> str:
        """Compare current data against most recent prior session snapshot.
        Returns a human-readable briefing. Empty string if no prior session exists."""
        sessions = self.get_unique_sessions()
        prev_sessions = [s for s in sessions if s["session_id"] != current_session_id]
        if not prev_sessions:
            return ""

        prev_session = prev_sessions[0]
        prev_episodes = self.get_session_history(prev_session["session_id"])
        if not prev_episodes:
            return ""

        # Find the most recent rich snapshot from the previous session
        prev_snapshot = None
        for ep in reversed(prev_episodes):
            if ep.get("data_snapshot_json"):
                try:
                    snap = json.loads(ep["data_snapshot_json"])
                    if "stuck_by_state" in snap:
                        prev_snapshot = snap
                        break
                except Exception:
                    continue

        if not prev_snapshot:
            return ""

        # Build current snapshot
        try:
            from data_loader import query as db_query

            current: dict = {}

            stuck_df = db_query("SELECT CNT_STATE, COUNT(*) as cnt FROM roster WHERE IS_STUCK=1 GROUP BY CNT_STATE")
            current["stuck_by_state"] = {r["CNT_STATE"]: int(r["cnt"]) for _, r in stuck_df.iterrows()}

            scs_df = db_query(
                "SELECT MARKET, SCS_PERCENT FROM metrics WHERE (MARKET, MONTH) IN "
                "(SELECT MARKET, MAX(MONTH) FROM metrics GROUP BY MARKET)"
            )
            current["scs_percent_by_state"] = {r["MARKET"]: float(r["SCS_PERCENT"]) for _, r in scs_df.iterrows()}

        except Exception:
            return ""

        # Compute time elapsed
        try:
            last_ts = prev_session.get("last_query", "")
            if last_ts:
                last_dt = datetime.fromisoformat(last_ts[:19])
                hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                if hours_ago < 1:
                    time_label = f"{int(hours_ago * 60)} minutes ago"
                elif hours_ago < 24:
                    time_label = f"{int(hours_ago)} hours ago"
                else:
                    time_label = f"{int(hours_ago / 24)} days ago"
            else:
                time_label = "your last session"
        except Exception:
            time_label = "your last session"

        lines = [f"**Welcome back!** Since {time_label}:"]
        any_change = False

        # Compare stuck ROs per state
        prev_stuck = prev_snapshot.get("stuck_by_state", {})
        curr_stuck = current.get("stuck_by_state", {})
        all_states = sorted(set(list(prev_stuck.keys()) + list(curr_stuck.keys())))
        for state in all_states:
            old_val = prev_stuck.get(state, 0)
            new_val = curr_stuck.get(state, 0)
            if old_val != new_val:
                any_change = True
                if new_val < old_val:
                    lines.append(f"- **{state}**: stuck ROs resolved ({old_val} → {new_val}) ✅")
                else:
                    lines.append(f"- **{state}**: stuck ROs increased ({old_val} → {new_val}) ⚠️")

        # Compare SCS_PERCENT (flag > 1% change)
        prev_scs = prev_snapshot.get("scs_percent_by_state", {})
        curr_scs = current.get("scs_percent_by_state", {})
        for state in sorted(prev_scs.keys()):
            old_scs = prev_scs.get(state)
            new_scs = curr_scs.get(state)
            if old_scs is not None and new_scs is not None and abs(old_scs - new_scs) > 1.0:
                any_change = True
                direction = "fell" if new_scs < old_scs else "rose"
                emoji = "⚠️" if new_scs < old_scs else "✅"
                lines.append(
                    f"- **{state}** SCS%: {direction} from {old_scs:.1f}% → {new_scs:.1f}% {emoji}"
                )

        if not any_change:
            lines.append("- No significant changes detected since your last session.")

        lines.append(
            f"\n*Last session had {prev_session.get('query_count', '?')} "
            f"{'query' if prev_session.get('query_count') == 1 else 'queries'}. "
            f"Episodic memory is active.*"
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Memory consolidation
    # ------------------------------------------------------------------

    def _maybe_consolidate(self):
        """Trigger consolidation when episode count exceeds 100."""
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
        conn.close()
        if count > 100:
            self.consolidate_old_episodes()

    def consolidate_old_episodes(self, llm_summarizer_fn=None):
        """Archive episodes older than 30 days into summarized digests."""
        from collections import defaultdict

        cutoff = (datetime.now() - timedelta(days=30)).isoformat()

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        old_episodes = conn.execute(
            "SELECT * FROM episodes WHERE timestamp < ? AND (findings_summary NOT LIKE '[ARCHIVED]%' OR findings_summary IS NULL) ORDER BY intent, timestamp",
            (cutoff,),
        ).fetchall()
        conn.close()

        if len(old_episodes) < 10:
            return

        groups: dict = defaultdict(list)
        for ep in old_episodes:
            groups[ep["intent"] or "general"].append(dict(ep))

        for intent, episodes in groups.items():
            if len(episodes) < 3:
                continue

            episode_ids = [ep["id"] for ep in episodes]
            summary_text = "\n".join([
                f"[{ep['timestamp'][:10]}] {ep['query']}: {(ep['findings_summary'] or '')[:200]}"
                for ep in episodes[:20]
            ])

            if llm_summarizer_fn:
                try:
                    digest_text = llm_summarizer_fn(
                        f"Summarize these {len(episodes)} past {intent} investigations in 2-3 sentences "
                        f"focusing on patterns, common entities, and key findings:\n{summary_text}"
                    )
                except Exception:
                    digest_text = f"{len(episodes)} past {intent} investigations. " + summary_text[:300]
            else:
                digest_text = f"{len(episodes)} past {intent} investigations. " + summary_text[:300]

            embedding = self._generate_embedding(digest_text)

            conn = sqlite3.connect(self.db_path)
            conn.execute(
                """INSERT INTO episode_digests
                   (timestamp, session_range, episode_ids_json, intent_group, digest_text, embedding_json, episode_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().isoformat(),
                    f"{episodes[0]['timestamp'][:10]} to {episodes[-1]['timestamp'][:10]}",
                    json.dumps(episode_ids),
                    intent,
                    digest_text,
                    json.dumps(embedding) if embedding else None,
                    len(episodes),
                ),
            )
            placeholders = ",".join("?" * len(episode_ids))
            conn.execute(
                f"UPDATE episodes SET findings_summary = '[ARCHIVED] ' || COALESCE(findings_summary, '') WHERE id IN ({placeholders})",
                episode_ids,
            )
            conn.commit()
            conn.close()

    # ------------------------------------------------------------------
    # Snapshot and state change helpers
    # ------------------------------------------------------------------

    def get_latest_snapshot_for_entity(self, entity_type: str, entity_id: str) -> dict | None:
        """Get the most recent data snapshot involving a specific entity."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT data_snapshot_json FROM episodes
            WHERE entities_json LIKE ?
            AND data_snapshot_json IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (f"%{entity_id}%",),
        ).fetchone()
        conn.close()
        if row and row["data_snapshot_json"]:
            return json.loads(row["data_snapshot_json"])
        return None

    def get_state_changes_for_entity(self, entity_type: str, entity_id: str, limit: int = 10) -> list[dict]:
        """Get state changes for a specific entity."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM state_changes
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (entity_type, entity_id, limit),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_session_history(self, session_id: str) -> list[dict]:
        """Get all episodes from a session."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM episodes WHERE session_id = ? ORDER BY timestamp",
            (session_id,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_all_episodes(self, limit: int = 50) -> list[dict]:
        """Get all episodes ordered by recency."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM episodes ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_all_state_changes(self, limit: int = 50) -> list[dict]:
        """Get all state changes ordered by recency."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM state_changes ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_unique_sessions(self) -> list[dict]:
        """Get list of unique sessions with their first/last timestamps."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT session_id,
                   MIN(timestamp) as first_query,
                   MAX(timestamp) as last_query,
                   COUNT(*) as query_count
            FROM episodes
            GROUP BY session_id
            ORDER BY last_query DESC
        """).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_recent_findings(self, limit: int = 10) -> list[dict]:
        """Get recent episode findings for pipeline context."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT query, findings_summary, tools_used, procedure_used, timestamp "
            "FROM episodes WHERE findings_summary IS NOT NULL AND findings_summary != '' "
            "ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def format_for_prompt(self, episodes: list[dict]) -> str:
        """Format episodes into a string for LLM prompt injection."""
        if not episodes:
            return "No relevant past investigations found."

        lines = ["## Relevant Past Investigations"]
        for ep in episodes:
            source = ep.get("_source", "episode")
            prefix = "[DIGEST] " if source == "digest" else ""
            lines.append(f"\n**[{str(ep.get('timestamp',''))[:19]}]** {prefix}Query: \"{str(ep.get('query',''))[:100]}\"")
            if ep.get("intent"):
                lines.append(f"  Intent: {ep['intent']}")
            if ep.get("findings_summary"):
                lines.append(f"  Findings: {str(ep['findings_summary'])[:300]}")
            if ep.get("procedure_used"):
                lines.append(f"  Procedure used: {ep['procedure_used']}")
            if ep.get("tools_used"):
                lines.append(f"  Tools: {ep['tools_used']}")
            score = ep.get("_score")
            if score is not None:
                lines.append(f"  Relevance: {score:.2f}")
        return "\n".join(lines)

"""Episodic Memory — SQLite-backed persistent memory of past interactions."""

import json
import sqlite3
from datetime import datetime


class EpisodicMemory:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
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
            CREATE INDEX IF NOT EXISTS idx_episodes_session
            ON episodes(session_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_state_changes_entity
            ON state_changes(entity_type, entity_id)
        """)
        conn.commit()
        conn.close()

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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            """
            INSERT INTO episodes
            (timestamp, session_id, query, intent, entities_json, findings_summary, tools_used, procedure_used, data_snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        episode_id = cursor.lastrowid
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

    def format_for_prompt(self, episodes: list[dict]) -> str:
        """Format episodes into a string for LLM prompt injection."""
        if not episodes:
            return "No relevant past investigations found."

        lines = ["## Relevant Past Investigations"]
        for ep in episodes:
            lines.append(f"\n**[{ep['timestamp']}]** Query: \"{ep['query']}\"")
            if ep.get("intent"):
                lines.append(f"  Intent: {ep['intent']}")
            if ep.get("findings_summary"):
                lines.append(f"  Findings: {ep['findings_summary']}")
            if ep.get("procedure_used"):
                lines.append(f"  Procedure used: {ep['procedure_used']}")
            if ep.get("tools_used"):
                lines.append(f"  Tools: {ep['tools_used']}")
        return "\n".join(lines)

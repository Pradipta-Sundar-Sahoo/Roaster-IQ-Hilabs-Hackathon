"""Vector Store — ChromaDB wrapper for semantic search over domain knowledge, investigations, and roster profiles."""

import chromadb


class VectorStore:
    """Manages 3 ChromaDB collections for multi-path retrieval."""

    def __init__(self, persist_dir: str):
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.domain_kb = self.client.get_or_create_collection("domain_knowledge")
        self.investigations = self.client.get_or_create_collection("investigation_history")
        self.roster_profiles = self.client.get_or_create_collection("roster_profiles")

    # ------------------------------------------------------------------
    # Initialization (idempotent — safe to call on every startup)
    # ------------------------------------------------------------------

    def initialize_domain_knowledge(self, semantic_memory, force_refresh=False):
        """Load YAML semantic knowledge into ChromaDB. Skips if already populated."""
        if force_refresh and self.domain_kb.count() > 0:
            self.client.delete_collection("domain_knowledge")
            self.domain_kb = self.client.get_or_create_collection("domain_knowledge")
            print("[vector_store] domain_knowledge force-refreshed")
        elif self.domain_kb.count() > 0:
            print(f"[vector_store] domain_knowledge already populated ({self.domain_kb.count()} docs)")
            return

        knowledge = semantic_memory.get_all_knowledge()
        docs, ids, metadatas = [], [], []

        # Pipeline stages
        for i, stage in enumerate(knowledge.get("pipeline_stages", [])):
            name = stage.get("name", "")
            desc = stage.get("description", "")
            docs.append(f"Pipeline stage: {name}. {desc}")
            ids.append(f"stage_{i}")
            metadatas.append({"category": "pipeline_stage", "name": name})

        # Failure statuses
        for key, val in knowledge.get("failure_statuses", {}).items():
            if isinstance(val, dict):
                meaning = val.get("meaning", "")
                implication = val.get("implication", "")
                text = f"Failure status '{key}': {meaning}. Implication: {implication}"
            else:
                text = f"Failure status '{key}': {val}"
            docs.append(text)
            ids.append(f"failure_{key}")
            metadatas.append({"category": "failure_status", "name": key})

        # LOB meanings
        for key, val in knowledge.get("lob_meanings", {}).items():
            if isinstance(val, dict):
                desc = val.get("description", "")
                risk = val.get("roster_impact", "")
                strictness = val.get("strictness", "")
                text = f"Line of Business '{key}': {desc}. Strictness: {strictness}. Roster impact: {risk}"
            else:
                text = f"Line of Business '{key}': {val}"
            docs.append(text)
            ids.append(f"lob_{key}")
            metadatas.append({"category": "lob", "name": key})

        # LOB analysis guidance
        for key, val in knowledge.get("lob_analysis_guidance", {}).items():
            docs.append(f"LOB analysis guidance — {key}: {val}")
            ids.append(f"lob_guide_{key}")
            metadatas.append({"category": "lob_guidance", "name": key})

        # Source systems
        for key, val in knowledge.get("source_systems", {}).items():
            desc = val.get("description", str(val)) if isinstance(val, dict) else str(val)
            docs.append(f"Source system '{key}': {desc}")
            ids.append(f"src_{key}")
            metadatas.append({"category": "source_system", "name": key})

        # Health flags
        for color, desc in knowledge.get("health_flags", {}).items():
            docs.append(f"Health flag '{color}': {desc}")
            ids.append(f"health_{color}")
            metadatas.append({"category": "health_flag", "name": color})

        # Status codes
        for code, desc in knowledge.get("file_status_codes", {}).items():
            docs.append(f"File status code {code}: {desc}")
            ids.append(f"status_code_{code}")
            metadatas.append({"category": "status_code", "name": str(code)})

        # Data notes
        for key, val in knowledge.get("data_notes", {}).items():
            docs.append(f"Data note — {key}: {val}")
            ids.append(f"note_{key}")
            metadatas.append({"category": "data_note", "name": key})

        # Cross-table relationships
        for key, val in knowledge.get("cross_table_relationships", {}).items():
            docs.append(f"Cross-table relationship: {val}")
            ids.append(f"xref_{key}")
            metadatas.append({"category": "cross_table", "name": key})

        if docs:
            self.domain_kb.add(documents=docs, ids=ids, metadatas=metadatas)
            print(f"[vector_store] Indexed {len(docs)} domain knowledge entries")

    def initialize_roster_profiles(self, conn):
        """Create org-level profiles from org_summary. Skips if already populated."""
        if self.roster_profiles.count() > 0:
            print(f"[vector_store] roster_profiles already populated ({self.roster_profiles.count()} docs)")
            return

        try:
            rows = conn.execute("""
                SELECT ORG_NM, CNT_STATE, TOTAL_ROS, STUCK_COUNT, FAILED_COUNT,
                       FAILURE_RATE, AVG_RED_COUNT, AVG_HEALTH_SCORE, CRITICAL_COUNT
                FROM org_summary
                ORDER BY TOTAL_ROS DESC
                LIMIT 500
            """).fetchall()
            cols = [d[0] for d in conn.execute("DESCRIBE org_summary").fetchall()]
        except Exception as e:
            print(f"[vector_store] Could not load roster profiles: {e}")
            return

        docs, ids, metadatas = [], [], []
        for i, row in enumerate(rows):
            r = dict(zip(cols, row))
            org = r.get("ORG_NM", "Unknown")
            state = r.get("CNT_STATE", "??")
            total = r.get("TOTAL_ROS", 0)
            failed = r.get("FAILED_COUNT", 0)
            rate = r.get("FAILURE_RATE", 0)
            critical = r.get("CRITICAL_COUNT", 0)
            health = r.get("AVG_HEALTH_SCORE", 0)

            text = (
                f"Organization '{org}' in {state}: {total} total ROs, "
                f"{failed} failed ({rate}% failure rate), "
                f"{critical} critical, avg health score {health}"
            )
            docs.append(text)
            ids.append(f"org_{i}_{state}")
            metadatas.append({"org": org, "state": state, "failure_rate": float(rate or 0)})

        if docs:
            self.roster_profiles.add(documents=docs, ids=ids, metadatas=metadatas)
            print(f"[vector_store] Indexed {len(docs)} roster profiles")

    # ------------------------------------------------------------------
    # Indexing new data
    # ------------------------------------------------------------------

    def index_episode(self, episode_id: int, query: str, findings_summary: str):
        """Add a new investigation episode to ChromaDB."""
        if not findings_summary:
            return
        doc = f"Query: {query}. Findings: {findings_summary[:500]}"
        try:
            self.investigations.add(
                documents=[doc],
                ids=[f"ep_{episode_id}"],
                metadatas=[{"episode_id": episode_id, "query": query[:200]}],
            )
        except Exception:
            pass  # Duplicate ID or other error — safe to ignore

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_domain(self, query: str, n_results: int = 5) -> list[dict]:
        """Semantic search over domain knowledge."""
        if self.domain_kb.count() == 0:
            return []
        results = self.domain_kb.query(query_texts=[query], n_results=min(n_results, self.domain_kb.count()))
        return self._format_results(results)

    def search_investigations(self, query: str, n_results: int = 5) -> list[dict]:
        """Semantic search over past investigations."""
        if self.investigations.count() == 0:
            return []
        results = self.investigations.query(query_texts=[query], n_results=min(n_results, self.investigations.count()))
        return self._format_results(results)

    def search_roster_profiles(self, query: str, n_results: int = 5) -> list[dict]:
        """Find orgs/states matching a natural language description."""
        if self.roster_profiles.count() == 0:
            return []
        results = self.roster_profiles.query(query_texts=[query], n_results=min(n_results, self.roster_profiles.count()))
        return self._format_results(results)

    def search_all(self, query: str, n_results: int = 3) -> dict:
        """Search all collections and return combined results."""
        return {
            "domain": self.search_domain(query, n_results),
            "investigations": self.search_investigations(query, n_results),
            "roster_profiles": self.search_roster_profiles(query, n_results),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_results(results: dict) -> list[dict]:
        """Convert ChromaDB query results to list of dicts."""
        formatted = []
        if not results or not results.get("documents"):
            return formatted
        docs = results["documents"][0]
        metadatas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        for i, doc in enumerate(docs):
            entry = {
                "text": doc,
                "metadata": metadatas[i] if i < len(metadatas) else {},
                "distance": distances[i] if i < len(distances) else None,
            }
            formatted.append(entry)
        return formatted

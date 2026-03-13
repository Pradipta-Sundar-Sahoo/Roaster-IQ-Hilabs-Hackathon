"""Semantic Memory — YAML-loaded domain knowledge about the roster pipeline."""

import os
from datetime import datetime
import yaml


class SemanticMemory:
    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path
        self.knowledge = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.yaml_path):
            with open(self.yaml_path, "r") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _save(self) -> None:
        with open(self.yaml_path, "w") as f:
            yaml.dump(self.knowledge, f, default_flow_style=False, allow_unicode=True)

    def get_all_knowledge(self) -> dict:
        return self.knowledge

    def update_knowledge(
        self,
        category: str,
        key: str,
        value: str,
        reason: str,
        session_id: str | None = None,
    ) -> dict:
        """Add or update a knowledge entry at runtime. Persists to YAML.
        Handles both dict-type (lob_meanings, failure_statuses) and list-type
        (pipeline_stages) categories."""
        if category not in self.knowledge:
            self.knowledge[category] = {}

        old_value = None
        target = self.knowledge[category]

        if isinstance(target, dict):
            old_value = target.get(key)
            target[key] = value
        elif isinstance(target, list):
            # List of dicts (e.g. pipeline_stages) — update by name or append
            existing = next((item for item in target if isinstance(item, dict) and item.get("name") == key), None)
            if existing:
                old_value = existing.get("description", "")
                existing["description"] = value
            else:
                target.append({"name": key, "description": value})

        # Track modification history
        if "modification_history" not in self.knowledge:
            self.knowledge["modification_history"] = []
        record = {
            "timestamp": datetime.now().isoformat(),
            "category": category,
            "key": key,
            "old_value": str(old_value) if old_value is not None else None,
            "new_value": str(value),
            "reason": reason,
            "session_id": session_id,
        }
        self.knowledge["modification_history"].append(record)
        self._save()

        return {"updated": True, "category": category, "key": key, "change": record}

    def get_stage_info(self, stage_name: str) -> dict | None:
        for stage in self.knowledge.get("pipeline_stages", []):
            if stage["name"].upper() == stage_name.upper():
                return stage
        return None

    def get_status_code_meaning(self, code: int) -> str:
        return self.knowledge.get("file_status_codes", {}).get(code, f"Unknown status code: {code}")

    def get_health_flag_meaning(self, color: str) -> str:
        return self.knowledge.get("health_flags", {}).get(color, f"Unknown health flag: {color}")

    def get_failure_status_info(self, status: str) -> dict | None:
        return self.knowledge.get("failure_statuses", {}).get(status)

    def get_source_system_info(self, system: str) -> dict | None:
        return self.knowledge.get("source_systems", {}).get(system)

    def get_lob_meaning(self, lob: str) -> str:
        return self.knowledge.get("lob_meanings", {}).get(lob, f"Unknown LOB: {lob}")

    def lookup(self, term: str) -> str:
        """Look up any term across all knowledge categories."""
        term_upper = term.upper()

        # Check pipeline stages
        for stage in self.knowledge.get("pipeline_stages", []):
            if stage["name"].upper() == term_upper:
                return f"**{stage['name']}**: {stage['description']}"

        # Check status codes
        try:
            code = int(term)
            if code in self.knowledge.get("file_status_codes", {}):
                return f"**Status Code {code}**: {self.knowledge['file_status_codes'][code]}"
        except ValueError:
            pass

        # Check health flags
        if term.capitalize() in self.knowledge.get("health_flags", {}):
            return f"**{term.capitalize()} Health Flag**: {self.knowledge['health_flags'][term.capitalize()]}"

        # Check failure statuses
        for key, val in self.knowledge.get("failure_statuses", {}).items():
            if term.lower() in key.lower():
                return f"**{key}**: {val.get('meaning', '')} — Implication: {val.get('implication', '')}"

        # Check source systems
        for key, val in self.knowledge.get("source_systems", {}).items():
            if term.lower() in key.lower():
                return f"**{key}**: {val.get('description', '')}"

        # Check LOBs
        for key, val in self.knowledge.get("lob_meanings", {}).items():
            if term.lower() in key.lower():
                return f"**{key}**: {val}"

        return f"No knowledge found for term: '{term}'"

    def format_for_prompt(self) -> str:
        """Format all semantic knowledge into a system prompt section."""
        k = self.knowledge
        lines = ["## Domain Knowledge (Semantic Memory)\n"]

        # Pipeline stages
        lines.append("### Pipeline Stages (in order)")
        for stage in k.get("pipeline_stages", []):
            lines.append(f"- **{stage['name']}**: {stage['description']}")

        # Status codes
        lines.append("\n### File Status Codes")
        for code, desc in k.get("file_status_codes", {}).items():
            lines.append(f"- **{code}**: {desc}")

        # Health flags
        lines.append("\n### Health Flag Colors")
        for color, desc in k.get("health_flags", {}).items():
            lines.append(f"- **{color}**: {desc}")
        if k.get("health_flag_note"):
            lines.append(f"- Note: {k['health_flag_note']}")

        # Failure statuses
        lines.append("\n### Failure Status Types")
        for status, info in k.get("failure_statuses", {}).items():
            lines.append(f"- **{status}**: {info.get('meaning', '')} (Severity: {info.get('severity', 'unknown')})")

        # Source systems
        lines.append("\n### Source Systems")
        for sys, info in k.get("source_systems", {}).items():
            lines.append(f"- **{sys}**: {info.get('description', '')}")

        # LOBs
        lines.append("\n### Lines of Business (LOB)")
        for lob, desc in k.get("lob_meanings", {}).items():
            lines.append(f"- **{lob}**: {desc}")

        # Cross-table relationships
        lines.append("\n### Cross-Table Relationships")
        for key, desc in k.get("cross_table_relationships", {}).items():
            lines.append(f"- {desc}")

        # Data notes
        lines.append("\n### Data Notes")
        for key, desc in k.get("data_notes", {}).items():
            lines.append(f"- {desc}")

        return "\n".join(lines)

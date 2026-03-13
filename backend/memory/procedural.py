"""Procedural Memory — JSON-backed versioned diagnostic procedures."""

import json
import os
from datetime import datetime
from copy import deepcopy


class ProceduralMemory:
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.procedures = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.json_path):
            with open(self.json_path, "r") as f:
                data = json.load(f)
            # Migrate: ensure execution_log exists on all procedures
            changed = False
            for proc in data.values():
                if "execution_log" not in proc:
                    proc["execution_log"] = []
                    changed = True
            if changed:
                with open(self.json_path, "w") as f:
                    json.dump(data, f, indent=2)
            return data
        return {}

    def _save(self):
        with open(self.json_path, "w") as f:
            json.dump(self.procedures, f, indent=2)

    def get_procedure(self, name: str) -> dict:
        if name not in self.procedures:
            raise KeyError(f"Procedure '{name}' not found")
        return deepcopy(self.procedures[name])

    def get_all_procedures(self) -> dict:
        return deepcopy(self.procedures)

    def get_procedure_names(self) -> list[str]:
        return list(self.procedures.keys())

    def update_procedure(self, name: str, updates: dict) -> dict:
        """Update a procedure's steps or parameters. Tracks version history."""
        if name not in self.procedures:
            raise KeyError(f"Procedure '{name}' not found")

        proc = self.procedures[name]
        old_version = proc["version"]

        # Record what changed
        change_record = {
            "timestamp": datetime.now().isoformat(),
            "from_version": old_version,
            "to_version": old_version + 1,
            "changes": {},
        }

        if "steps" in updates:
            change_record["changes"]["steps"] = "Steps modified"
            proc["steps"] = updates["steps"]

        if "parameters" in updates:
            change_record["changes"]["parameters"] = "Parameters modified"
            proc["parameters"] = updates["parameters"]

        if "description" in updates:
            change_record["changes"]["description"] = f"Description changed from '{proc['description']}'"
            proc["description"] = updates["description"]

        if "add_step" in updates:
            proc["steps"].append(updates["add_step"])
            change_record["changes"]["add_step"] = f"Added step: {updates['add_step'].get('description', 'new step')}"

        if "modify_step" in updates:
            idx = updates["modify_step"].get("index", -1)
            if 0 <= idx < len(proc["steps"]):
                old_step = proc["steps"][idx]
                proc["steps"][idx] = {**old_step, **updates["modify_step"].get("updates", {})}
                change_record["changes"]["modify_step"] = f"Modified step {idx}: {old_step.get('description', '')}"

        if "change_summary" in updates:
            change_record["changes"]["summary"] = updates["change_summary"]

        proc["version"] = old_version + 1
        proc["last_modified"] = datetime.now().isoformat()
        proc["modification_history"].append(change_record)

        self._save()
        return {
            "procedure": name,
            "old_version": old_version,
            "new_version": proc["version"],
            "changes": change_record["changes"],
        }

    def log_execution(
        self,
        name: str,
        params: dict,
        outcome: str,
        session_id: str | None = None,
    ) -> None:
        """Log a procedure execution outcome. Outcomes: resolved | unresolved | escalated | informational."""
        if name not in self.procedures:
            return
        record = {
            "timestamp": datetime.now().isoformat(),
            "params": params,
            "outcome": outcome,
            "session_id": session_id,
        }
        log = self.procedures[name].setdefault("execution_log", [])
        log.append(record)
        # Rolling window — keep last 50
        if len(log) > 50:
            self.procedures[name]["execution_log"] = log[-50:]
        self._save()

    def get_procedure_effectiveness(self, name: str) -> dict:
        """Return execution stats and resolved rate for a procedure."""
        if name not in self.procedures:
            return {}
        log = self.procedures[name].get("execution_log", [])
        if not log:
            return {"total_runs": 0, "resolved_rate": None, "last_run": None}
        resolved = sum(1 for e in log if e.get("outcome") == "resolved")
        return {
            "total_runs": len(log),
            "resolved_count": resolved,
            "resolved_rate": round(resolved / len(log) * 100, 1),
            "unresolved_count": sum(1 for e in log if e.get("outcome") == "unresolved"),
            "escalated_count": sum(1 for e in log if e.get("outcome") == "escalated"),
            "last_run": log[-1]["timestamp"] if log else None,
        }

    def format_for_prompt(self, procedure_name: str = None) -> str:
        """Format procedures for LLM prompt injection."""
        if procedure_name:
            if procedure_name not in self.procedures:
                return f"Procedure '{procedure_name}' not found."
            proc = self.procedures[procedure_name]
            return self._format_single(proc)

        lines = ["## Available Diagnostic Procedures"]
        for name, proc in self.procedures.items():
            lines.append(f"\n### {name} (v{proc['version']})")
            lines.append(f"**Description:** {proc['description']}")
            lines.append(f"**Steps:** {len(proc['steps'])}")
            if proc.get("parameters"):
                params = ", ".join(
                    f"{k} ({v.get('type', 'any')})" for k, v in proc["parameters"].items()
                )
                lines.append(f"**Parameters:** {params}")
            if proc.get("modification_history"):
                last_mod = proc["modification_history"][-1]
                lines.append(f"**Last modified:** v{last_mod['from_version']}→v{last_mod['to_version']} at {last_mod['timestamp']}")
            eff = self.get_procedure_effectiveness(name)
            if eff.get("total_runs", 0) > 0:
                lines.append(f"**Effectiveness:** {eff['resolved_rate']}% resolved ({eff['total_runs']} runs)")
        return "\n".join(lines)

    def _format_single(self, proc: dict) -> str:
        lines = [
            f"## Procedure: {proc['name']} (v{proc['version']})",
            f"**Description:** {proc['description']}",
            f"\n**Steps:**",
        ]
        for i, step in enumerate(proc["steps"], 1):
            lines.append(f"  {i}. [{step.get('action', 'unknown')}] {step.get('description', '')}")
            if "sql" in step:
                lines.append(f"     SQL: {step['sql']}")

        if proc.get("modification_history"):
            lines.append("\n**Modification History:**")
            for mod in proc["modification_history"]:
                lines.append(f"  - v{mod['from_version']}→v{mod['to_version']} ({mod['timestamp']}): {mod.get('changes', {})}")

        return "\n".join(lines)

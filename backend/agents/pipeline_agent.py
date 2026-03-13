"""Pipeline Health Agent — specialized in stuck ROs, stage durations, health flags."""

from prompts import PIPELINE_AGENT_PROMPT


class PipelineAgent:
    """Sub-agent focused on pipeline performance analysis.

    In the multi-agent architecture, the Supervisor delegates pipeline-specific
    queries to this agent. It specializes in:
    - Stuck RO triage and escalation
    - Stage duration anomaly detection
    - Health flag pattern analysis
    - Pipeline bottleneck identification
    """

    def __init__(self):
        self.role = "pipeline_health"
        self.prompt = PIPELINE_AGENT_PROMPT
        self.procedures = ["triage_stuck_ros"]
        self.primary_table = "roster"

    def get_context(self) -> str:
        """Return agent-specific context for the supervisor prompt."""
        return self.prompt

    def get_relevant_queries(self, intent: str) -> list[str]:
        """Suggest relevant SQL queries based on intent."""
        queries = {
            "triage": [
                "SELECT * FROM roster WHERE IS_STUCK = 1",
                "SELECT LATEST_STAGE_NM, COUNT(*) FROM roster WHERE IS_FAILED = 1 GROUP BY LATEST_STAGE_NM",
            ],
            "health": [
                "SELECT CNT_STATE, SUM(CASE WHEN PRE_PROCESSING_HEALTH='Red' THEN 1 ELSE 0 END) as red_count FROM roster GROUP BY CNT_STATE",
            ],
            "duration": [
                "SELECT ORG_NM, DART_GEN_DURATION, AVG_DART_GENERATION_DURATION FROM roster WHERE DART_GEN_DURATION > 2 * AVG_DART_GENERATION_DURATION",
            ],
        }
        return queries.get(intent, [])

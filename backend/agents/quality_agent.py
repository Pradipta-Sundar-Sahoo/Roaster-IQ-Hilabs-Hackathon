"""Record Quality Agent — specialized in failure rates, market metrics, retry analysis."""

from prompts import QUALITY_AGENT_PROMPT


class QualityAgent:
    """Sub-agent focused on record quality and market health analysis.

    In the multi-agent architecture, the Supervisor delegates quality-specific
    queries to this agent. It specializes in:
    - Failure rate analysis by state/org/LOB
    - Market SCS_PERCENT correlation with file-level quality
    - Retry effectiveness analysis
    - Cross-table correlation between CSV1 and CSV2
    """

    def __init__(self):
        self.role = "record_quality"
        self.prompt = QUALITY_AGENT_PROMPT
        self.procedures = ["record_quality_audit", "market_health_report", "retry_effectiveness_analysis"]
        self.primary_tables = ["roster", "metrics"]

    def get_context(self) -> str:
        """Return agent-specific context for the supervisor prompt."""
        return self.prompt

    def get_relevant_queries(self, intent: str) -> list[str]:
        """Suggest relevant SQL queries based on intent."""
        queries = {
            "audit": [
                "SELECT CNT_STATE, COUNT(*) as total, SUM(CASE WHEN IS_FAILED=1 THEN 1 ELSE 0 END) as failed FROM roster GROUP BY CNT_STATE",
                "SELECT FAILURE_STATUS, COUNT(*) FROM roster WHERE IS_FAILED=1 GROUP BY FAILURE_STATUS",
            ],
            "report": [
                "SELECT MARKET, MONTH, SCS_PERCENT FROM metrics ORDER BY MARKET, MONTH",
            ],
            "analysis": [
                "SELECT RO_ID, RUN_NO, IS_FAILED FROM roster WHERE RO_ID IN (SELECT RO_ID FROM roster WHERE RUN_NO > 1) ORDER BY RO_ID, RUN_NO",
            ],
        }
        return queries.get(intent, [])

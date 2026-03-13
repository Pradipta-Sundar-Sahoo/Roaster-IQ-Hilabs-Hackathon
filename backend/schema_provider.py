"""Dynamic schema provider — single source of truth for all table/column metadata.

Extracts table schemas from DuckDB at runtime and enriches them with
semantic notes so prompts never need hardcoded column lists.
"""

import difflib
import re
from typing import Optional


_schema: dict[str, list[tuple[str, str]]] = {}
_all_columns: dict[str, list[str]] = {}
_column_set: set[str] = set()
_sample_values: dict[str, dict[str, list]] = {}
_schema_text_cache: str = ""
_tool_desc_cache: str = ""

# ── Column-level semantic notes ──
# Keys are column names (case-insensitive match). Notes are injected into the
# schema text so the LLM understands meaning, valid values, and query patterns.
COLUMN_NOTES: dict[str, str] = {
    # Identity / grouping
    "ID":                "Unique roster operation identifier",
    "RO_ID":             "Roster operation ID — may differ from ID",
    "ORG_NM":            "Organization / provider group name (VARCHAR)",
    "CNT_STATE":         "2-letter US state code (e.g. TN, NY, CA). NEVER use full names.",
    "MARKET":            "2-letter US state code, same encoding as CNT_STATE",
    "SRC_SYS":           "Source system, UPPERCASE (e.g. AVAILITYPDM, PROVIDERGROUP, DPE)",
    "LOB":               "Comma-separated lines of business (raw, prefer LOB_PRIMARY)",
    "LOB_PRIMARY":       "First LOB extracted from LOB, UPPERCASE",
    "FILE_STATUS_CD":    "Integer status code of the file (e.g. 99=RESOLVED, 65=SPS_LOAD, 9=STOPPED)",

    # Pipeline stage
    "LATEST_STAGE_NM":   "Current pipeline stage. Values: PRE_PROCESSING, MAPPING_APROVAL, ISF_GEN, DART_GEN, DART_REVIEW, DART_UI_VALIDATION, SPS_LOAD, RESOLVED, INGESTION, STOPPED, REJECTED",
    "PIPELINE_STAGE_ORDER": "Integer index for stage ordering (0=PRE_PROCESSING … 7=RESOLVED, -1=STOPPED/REJECTED)",
    "WORST_HEALTH_STAGE": "Last pipeline stage with RED health (NULL if none)",

    # Health flags — all UPPERCASE: RED, GREEN, YELLOW
    "PRE_PROCESSING_HEALTH":      "RED / GREEN / YELLOW (always UPPERCASE)",
    "MAPPING_APROVAL_HEALTH":     "RED / GREEN / YELLOW (always UPPERCASE)",
    "ISF_GEN_HEALTH":             "RED / GREEN / YELLOW (always UPPERCASE)",
    "DART_GEN_HEALTH":            "RED / GREEN / YELLOW (always UPPERCASE)",
    "DART_REVIEW_HEALTH":         "RED / GREEN / YELLOW (always UPPERCASE)",
    "DART_UI_VALIDATION_HEALTH":  "RED / GREEN / YELLOW (always UPPERCASE)",
    "SPS_LOAD_HEALTH":            "RED / GREEN / YELLOW (always UPPERCASE)",

    # Duration columns (minutes)
    "PRE_PROCESSING_DURATION":       "Duration in minutes",
    "MAPPING_APROVAL_DURATION":      "Duration in minutes",
    "ISF_GEN_DURATION":              "Duration in minutes",
    "DART_GEN_DURATION":             "Duration in minutes",
    "DART_REVIEW_DURATION":          "Duration in minutes",
    "DART_UI_VALIDATION_DURATION":   "Duration in minutes",
    "SPS_LOAD_DURATION":             "Duration in minutes",
    "AVG_DART_GEN_DURATION":         "Average DART generation duration across records",
    "AVG_ISF_GEN_DURATION":          "Average ISF generation duration across records",
    "AVG_DART_UI_VALIDATION_DURATION": "Average DART UI validation duration across records",

    # Boolean-like flags — INTEGER 0/1, NOT boolean. Use =1 or =0.
    "IS_FAILED":         "INTEGER 0/1 — use IS_FAILED=1 (never =TRUE or ='FAILED')",
    "IS_STUCK":          "INTEGER 0/1 — use IS_STUCK=1 (never =TRUE)",
    "IS_RETRY":          "INTEGER 0/1 — 1 if RUN_NO > 1",
    "IS_BELOW_SLA":      "INTEGER 0/1 — 1 if SCS_PERCENT < 95",

    # Computed / enriched
    "RUN_NO":            "Attempt number (1 = first attempt, >1 = retry)",
    "DAYS_STUCK":        "Days since FILE_RECEIVED_DT (precomputed)",
    "RED_COUNT":         "Count of RED health flags across all 7 stages (0-7)",
    "YELLOW_COUNT":      "Count of YELLOW health flags across all 7 stages (0-7)",
    "HEALTH_SCORE":      "Weighted score: GREEN=2, YELLOW=1, RED=0 per stage (0-14)",
    "PRIORITY":          "CRITICAL / HIGH / MEDIUM / LOW (UPPERCASE). Derived from DAYS_STUCK + RED_COUNT.",
    "FAILURE_STATUS":    "Raw failure description, UPPERCASE (e.g. COMPLETE VALIDATION FAILURE, INCOMPATIBLE, FAILED)",
    "FAILURE_CATEGORY":  "Classified failure: NONE / VALIDATION / TIMEOUT / PROCESSING / COMPLIANCE / OTHER (UPPERCASE)",

    # Dates
    "FILE_RECEIVED_DT":  "Timestamp when the RO file was received",

    # metrics table
    "MONTH":             "Month string in MM-YYYY format",
    "MONTH_DATE":        "Parsed TIMESTAMP from MONTH — use for sorting/filtering by date",
    "SCS_PERCENT":       "Overall success percentage for the market/month",
    "FIRST_ITER_SCS_CNT":  "First-iteration success count",
    "FIRST_ITER_FAIL_CNT": "First-iteration failure count",
    "NEXT_ITER_SCS_CNT":   "Retry-iteration success count",
    "NEXT_ITER_FAIL_CNT":  "Retry-iteration failure count",
    "OVERALL_SCS_CNT":     "Total success count across all iterations",
    "OVERALL_FAIL_CNT":    "Total failure count across all iterations",
    "RETRY_LIFT_PCT":      "Percentage improvement from retries vs first iteration",
    "OVERALL_FAIL_RATE":   "Overall failure rate as percentage",
    "FIRST_ITER_FAIL_RATE": "First-iteration failure rate as percentage",

    # Summary tables
    "STATE":             "2-letter state code (same as CNT_STATE)",
    "TOTAL_ROS":         "Total roster operations count",
    "STUCK_COUNT":       "Number of stuck ROs in this group",
    "FAILED_COUNT":      "Number of failed ROs in this group",
    "FAILURE_RATE":      "Percentage of failed ROs",
    "AVG_DAYS_STUCK":    "Average days stuck for ROs in this group",
    "CRITICAL_COUNT":    "Count of CRITICAL priority ROs",
    "HIGH_COUNT":        "Count of HIGH priority ROs",
    "AVG_RED_COUNT":     "Average RED flags per RO in this group",
    "AVG_HEALTH_SCORE":  "Average health score in this group",
    "TOP_FAILURE_CATEGORY": "Most common failure category in this group",
    "TOP_FAILING_ORG":   "Organization with most failures in this group",
    "STAGE_NM":          "Pipeline stage name (same values as LATEST_STAGE_NM)",
    "AVG_RED_FLAGS":     "Average red flag count for ROs at this stage",
    "STUCK_IN_STAGE":    "Number of stuck ROs at this stage",
}


def build_schema_cache(conn) -> None:
    """Call once after all tables are created. Introspects DuckDB to populate caches."""
    global _schema, _all_columns, _column_set, _sample_values, _schema_text_cache, _tool_desc_cache

    tables = ["roster", "metrics", "state_summary", "org_summary", "stage_health_summary"]

    _schema.clear()
    _all_columns.clear()
    _column_set.clear()
    _sample_values.clear()

    for table in tables:
        try:
            cols_df = conn.execute(f"DESCRIBE {table}").fetchdf()
            col_list = [(row["column_name"], row["column_type"]) for _, row in cols_df.iterrows()]
            _schema[table] = col_list

            for col_name, _ in col_list:
                _column_set.add(col_name)
                _all_columns.setdefault(col_name.lower(), []).append(table)
        except Exception:
            pass

    _categorical_samples = {
        "roster": ["CNT_STATE", "FAILURE_CATEGORY", "PRIORITY", "LATEST_STAGE_NM",
                    "WORST_HEALTH_STAGE", "LOB_PRIMARY", "SRC_SYS", "FAILURE_STATUS"],
        "metrics": ["MARKET"],
    }
    for table, cols in _categorical_samples.items():
        _sample_values[table] = {}
        for col in cols:
            if col.lower() not in _all_columns:
                continue
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 30"
                ).fetchall()
                _sample_values[table][col] = [r[0] for r in rows]
            except Exception:
                pass

    _schema_text_cache = _build_schema_text()
    _tool_desc_cache = _build_tool_description()
    print(f"[schema_provider] Cached schema for {len(_schema)} tables, {len(_column_set)} unique columns")


def _build_schema_text() -> str:
    """Build full schema text with column types, notes, and sample values."""
    sections = []

    for table, cols in _schema.items():
        col_lines = []
        for name, dtype in cols:
            note = COLUMN_NOTES.get(name, "")
            note_str = f"  -- {note}" if note else ""
            col_lines.append(f"  {name} ({dtype}){note_str}")

        sample_info = ""
        if table in _sample_values:
            sample_parts = []
            for col, vals in _sample_values[table].items():
                display_vals = [str(v) for v in vals[:20]]
                sample_parts.append(f"  {col}: [{', '.join(display_vals)}]")
            if sample_parts:
                sample_info = "\n  Allowed values:\n" + "\n".join(sample_parts)

        sections.append(f"### {table}\n" + "\n".join(col_lines) + sample_info)

    global_rules = """
## IMPORTANT QUERY RULES
- ALL categorical string values are UPPERCASE. Always use UPPER case in WHERE clauses.
  Health: 'RED', 'GREEN', 'YELLOW'  |  Priority: 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
  Failure category: 'NONE', 'VALIDATION', 'OTHER'  |  States: 2-letter codes like 'TN', 'NY'
- IS_FAILED, IS_STUCK, IS_RETRY, IS_BELOW_SLA are INTEGER (0/1). Use =1 or =0, NEVER =TRUE/FALSE.
- There is NO column called 'status' or 'attempt_number'. Use IS_FAILED, IS_STUCK, RUN_NO.
- Use exact column names from the schema. Column names are UPPER_SNAKE_CASE.
"""
    return global_rules + "\n" + "\n\n".join(sections)


def _build_tool_description() -> str:
    """Build a compact column summary for tool descriptions."""
    parts = []
    for table, cols in _schema.items():
        col_names = [c[0] for c in cols]
        parts.append(f"{table}: {', '.join(col_names)}")
    return "; ".join(parts)


def get_schema_text() -> str:
    return _schema_text_cache


def get_tool_description() -> str:
    return _tool_desc_cache


def get_table_schema(table: str) -> list[tuple[str, str]]:
    return _schema.get(table, [])


def get_column_names(table: str) -> list[str]:
    return [c[0] for c in _schema.get(table, [])]


def get_all_column_names() -> set[str]:
    return _column_set.copy()


def get_sample_values(table: str, column: str) -> list:
    return _sample_values.get(table, {}).get(column, [])


def suggest_column_fix(wrong_col: str, table: str | None = None) -> Optional[str]:
    candidates = get_column_names(table) if table else list(_column_set)
    if not candidates:
        return None
    matches = difflib.get_close_matches(wrong_col.upper(), [c.upper() for c in candidates], n=1, cutoff=0.6)
    if matches:
        upper_match = matches[0]
        for c in candidates:
            if c.upper() == upper_match:
                return c
    return None


def find_column_corrections(failed_sql: str) -> list[str]:
    """Scan a failed SQL for common mistakes and return correction messages."""
    corrections = []

    # Strip string literals before extracting column-like tokens to avoid false positives
    sql_no_strings = re.sub(r"'[^']*'", "", failed_sql)
    tokens = set(re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', sql_no_strings))
    sql_keywords = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "AS", "ON", "JOIN",
        "LEFT", "RIGHT", "INNER", "OUTER", "GROUP", "BY", "ORDER", "HAVING",
        "LIMIT", "OFFSET", "CASE", "WHEN", "THEN", "ELSE", "END", "COUNT",
        "SUM", "AVG", "MIN", "MAX", "ROUND", "CAST", "NULLIF", "COALESCE",
        "DISTINCT", "DESC", "ASC", "IS", "NULL", "TRUE", "FALSE", "LIKE",
        "ILIKE", "BETWEEN", "EXISTS", "UNION", "ALL", "CREATE", "INSERT",
        "UPDATE", "DELETE", "DROP", "ALTER", "TABLE", "WITH", "RECURSIVE",
        "OVER", "PARTITION", "ROW_NUMBER", "RANK", "DENSE_RANK", "DATEDIFF",
        "STRPTIME", "STRFTIME", "CURRENT_TIMESTAMP", "INTEGER", "VARCHAR",
        "DOUBLE", "FLOAT", "BOOLEAN", "TIMESTAMP", "DATE", "TRY_CAST",
        "USING", "REPLACE", "TRIM", "UPPER", "LOWER", "SPLIT_PART",
        "PERCENTILE_CONT", "WITHIN",
    }
    table_names = set(_schema.keys())

    for token in tokens:
        upper = token.upper()
        if upper in sql_keywords or upper in table_names:
            continue
        if len(token) < 3:
            continue
        if token.lower() in _all_columns:
            continue
        suggestion = suggest_column_fix(token)
        if suggestion and suggestion.upper() != upper:
            corrections.append(f"Column '{token}' does NOT exist. Use '{suggestion}'.")

    # Boolean misuse on INTEGER columns
    sql_lower = failed_sql.lower()
    for col_name, col_type in _schema.get("roster", []) + _schema.get("metrics", []):
        if col_name.startswith("IS_") and "INT" in col_type.upper():
            if re.search(rf"{col_name.lower()}\s*=\s*true", sql_lower) or f"{col_name.lower()} is true" in sql_lower:
                corrections.append(f"{col_name} is INTEGER (0/1). Use {col_name}=1, not TRUE.")

    # 'status' column hallucination
    if re.search(r'\bstatus\b', sql_lower) and not any(
        k in sql_lower for k in ["failure_status", "file_status_cd"]
    ):
        corrections.append("No 'status' column exists. Use IS_FAILED=1 for failed, IS_STUCK=1 for stuck.")

    # Non-uppercase categorical value misuse (catches 'Red', 'red', 'Critical', etc.)
    _case_checks = {
        r"'(red|green|yellow|Red|Green|Yellow)'": "Health values must be UPPERCASE: 'RED', 'GREEN', 'YELLOW'",
        r"'(critical|high|medium|low|Critical|High|Medium|Low)'": "PRIORITY values must be UPPERCASE: 'CRITICAL', 'HIGH', etc.",
        r"'(none|validation|timeout|processing|compliance|other|None|Validation|Timeout|Processing|Compliance|Other)'": "FAILURE_CATEGORY must be UPPERCASE",
    }
    for pattern, msg in _case_checks.items():
        if re.search(pattern, failed_sql):
            corrections.append(msg)

    # Full state name usage
    _state_name_map = {
        "tennessee": "TN", "new york": "NY", "california": "CA", "texas": "TX",
        "florida": "FL", "ohio": "OH", "south carolina": "SC", "colorado": "CO",
        "georgia": "GA", "indiana": "IN", "kentucky": "KY", "virginia": "VA",
        "washington": "WA", "arizona": "AZ", "arkansas": "AR", "connecticut": "CT",
        "iowa": "IA", "kansas": "KS", "louisiana": "LA", "maryland": "MD",
        "maine": "ME", "missouri": "MO", "nebraska": "NE", "nevada": "NV",
        "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
        "west virginia": "WV", "wisconsin": "WI",
    }
    for name, code in _state_name_map.items():
        if f"'{name}'" in sql_lower or f'"{name}"' in sql_lower:
            corrections.append(f"Use 2-letter code '{code}' instead of '{name.title()}'.")

    return corrections


def get_schema_for_error_hints(referenced_tables: list[str]) -> dict:
    """Build schema hints dict for a failed SQL query."""
    hints = {}
    for table in referenced_tables:
        cols = get_table_schema(table)
        if cols:
            hints[f"{table}_columns"] = [f"{name} ({dtype})" for name, dtype in cols]
    hints["available_tables"] = list(_schema.keys())
    return hints

"""Web search tool — Tavily API integration for regulatory and business context."""

import os
from tavily import TavilyClient

_client: TavilyClient | None = None


def _get_client() -> TavilyClient:
    global _client
    if _client is None:
        api_key = os.environ.get("TAVILY_API_KEY")
        if not api_key:
            raise ValueError("TAVILY_API_KEY not set")
        _client = TavilyClient(api_key=api_key)
    return _client


def search(query: str, max_results: int = 3) -> dict:
    """Search the web and return clean results."""
    try:
        client = _get_client()
        response = client.search(query=query, max_results=max_results)
        results = []
        for r in response.get("results", []):
            results.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "content": r.get("content", "")[:500],
            })
        return {
            "query": query,
            "results": results,
            "result_count": len(results),
        }
    except Exception as e:
        return {"query": query, "error": str(e), "results": []}


def search_regulatory_context(state: str, topic: str = "provider enrollment") -> dict:
    """Search for state-specific regulatory context."""
    query = f"{state} Medicaid Medicare {topic} rule changes 2025 2026"
    return search(query)


def search_org_context(org_name: str) -> dict:
    """Search for business context about a healthcare organization."""
    query = f'"{org_name}" healthcare provider organization'
    return search(query)


def search_compliance_context(failure_type: str) -> dict:
    """Search for compliance requirements related to a failure type."""
    query = f"CMS provider roster {failure_type} data validation compliance requirements"
    return search(query)


def search_lob_requirements(lob: str, state: str) -> dict:
    """Search for LOB-specific regulatory requirements."""
    query = f"{lob} {state} provider data submission requirements regulations"
    return search(query)

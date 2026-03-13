const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function fetchWithTimeout(url: string, timeoutMs = 5000): Promise<unknown> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal });
    return res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timeoutId);
  }
}

export interface ToolCall {
  tool: string;
  args: Record<string, unknown>;
  result: {
    row_count?: number;
    columns?: string[];
    data?: Record<string, unknown>[];
    error?: string;
    summary?: string;
    [key: string]: unknown;
  };
}

export interface ChatResponse {
  message: string;
  charts: Record<string, unknown>[];
  memory_updates: {
    episodic?: { logged: boolean; episode_id: number };
    state_changes?: { entity: string; field: string; old: string; new: string; note?: string }[];
  };
  web_search_results: { query: string; results: { title: string; url: string; content: string }[] }[];
  tool_calls: ToolCall[];
  procedure_used: string | null;
  agent_used: string | null;
  session_id: string;
}

export interface Episode {
  id: number;
  timestamp: string;
  session_id: string;
  query: string;
  intent: string;
  entities_json: string;
  findings_summary: string;
  tools_used: string;
  procedure_used: string;
}

export interface Alert {
  type: string;
  severity: string;
  message: string;
  details: Record<string, unknown>;
}

export async function sendChat(message: string, sessionId?: string): Promise<ChatResponse> {
  const res = await fetch(`${API_BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, session_id: sessionId }),
  });
  if (!res.ok) throw new Error(`Chat failed: ${res.statusText}`);
  return res.json();
}

export async function getHealth() {
  const res = await fetch(`${API_BASE}/health`);
  return res.json();
}

export async function getDashboardOverview() {
  const res = await fetch(`${API_BASE}/dashboard/overview`);
  return res.json();
}

export async function getAlerts(): Promise<{ alerts: Alert[] }> {
  const res = await fetch(`${API_BASE}/dashboard/alerts`);
  return res.json();
}

export async function getEpisodicMemory() {
  return fetchWithTimeout(`${API_BASE}/memory/episodic`);
}

export async function getProceduralMemory() {
  return fetchWithTimeout(`${API_BASE}/memory/procedural`);
}

export async function getSemanticMemory() {
  return fetchWithTimeout(`${API_BASE}/memory/semantic`);
}

export async function getSessionBriefing(sessionId: string): Promise<{ briefing: string; has_briefing: boolean } | null> {
  const result = await fetchWithTimeout(`${API_BASE}/session/briefing?session_id=${encodeURIComponent(sessionId)}`, 4000);
  return result as { briefing: string; has_briefing: boolean } | null;
}

export async function runProcedure(name: string, params: Record<string, unknown> = {}) {
  const res = await fetch(`${API_BASE}/procedure/${name}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ params }),
  });
  return res.json();
}

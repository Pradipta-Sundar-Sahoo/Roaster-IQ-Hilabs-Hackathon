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
  recommended_action?: string | null;
  recommended_params?: Record<string, unknown> | null;
  details: Record<string, unknown>;
}

export interface RootCauseInsight {
  issue: string;
  explanation: string;
  severity: string;
  count: number;
}

export interface RecommendedAction {
  priority: number;
  action: string;
  procedure: string | null;
  params: Record<string, unknown>;
  reason?: string;
}

export interface IntelligenceData {
  pipeline_health_summary: string;
  health_status: string;
  root_cause_insights: RootCauseInsight[];
  recommended_actions: RecommendedAction[];
  retry_effectiveness: {
    total_retries: number;
    retry_successes?: number;
    retry_failures?: number;
    success_rate: number;
  };
  procedure_effectiveness: Record<string, {
    total_runs: number;
    resolved_rate: number | null;
    last_run: string | null;
  }>;
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

export async function getAlerts(scsThreshold?: number): Promise<{ alerts: Alert[] }> {
  const url = scsThreshold != null
    ? `${API_BASE}/alerts?scs_threshold=${scsThreshold}`
    : `${API_BASE}/dashboard/alerts`;
  const res = await fetch(url);
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

export async function getIntelligence(): Promise<IntelligenceData | null> {
  const result = await fetchWithTimeout(`${API_BASE}/dashboard/intelligence`, 10000);
  return result as IntelligenceData | null;
}

export interface PipelineReport {
  procedure: string;
  filter: string;
  narrative_summary: string;
  health_rating: string;
  summary_statistics: Record<string, number>;
  flagged_ros: Record<string, unknown>[];
  flagged_count: number;
  stage_bottlenecks: {
    stage: string;
    total: number;
    stuck: number;
    failed: number;
    avg_red_flags: number;
    avg_days_stuck: number;
    interpretation: string;
  }[];
  derived_health_metrics: Record<string, number>;
  failure_breakdown: { category: string; count: number; explanation: string }[];
  market_context: Record<string, {
    latest_scs: number;
    latest_month: string;
    latest_retry_lift: number | null;
    trend: Record<string, unknown>[];
  }>;
  retry_effectiveness: Record<string, number>;
  recommended_actions: RecommendedAction[];
  charts: Record<string, unknown>[];
  summary: string;
}

export async function getLatestReport(): Promise<PipelineReport | null> {
  const result = await fetchWithTimeout(`${API_BASE}/report/latest`, 15000);
  return result as PipelineReport | null;
}

export async function generateReport(
  params: { state?: string; org?: string; lob?: string; source_system?: string } = {}
): Promise<PipelineReport> {
  const res = await fetch(`${API_BASE}/report/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });
  return res.json();
}

export async function runProcedure(name: string, params: Record<string, unknown> = {}) {
  const res = await fetch(`${API_BASE}/procedure/${name}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ params }),
  });
  return res.json();
}

export interface CreateProcedurePayload {
  name: string;
  description: string;
  steps: { action: string; sql: string; description: string }[];
  parameters?: Record<string, { type: string; default?: unknown }>;
}

export async function createProcedure(payload: CreateProcedurePayload) {
  const res = await fetch(`${API_BASE}/memory/procedural`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

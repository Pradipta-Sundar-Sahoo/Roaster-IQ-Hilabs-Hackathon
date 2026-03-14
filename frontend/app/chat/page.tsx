"use client";

import { useState, useRef, useEffect, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { motion, AnimatePresence } from "framer-motion";
import { sendChat, getSessionBriefing, getProceduralMemory, createProcedure, type ChatResponse, type ToolCall, type ProcedureUpdate } from "@/lib/api";
import { PlotlyChart } from "@/components/charts/PlotlyChart";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Send,
  AlertTriangle,
  Globe,
  Sparkles,
  BarChart3,
  Search,
  RefreshCw,
  Activity,
  Zap,
  ChevronDown,
  ChevronRight,
  Database,
  Terminal,
  Table2,
  Plus,
  X,
} from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

interface Message {
  role: "user" | "assistant";
  content: string;
  charts?: Record<string, unknown>[];
  webResults?: ChatResponse["web_search_results"];
  memoryUpdates?: ChatResponse["memory_updates"];
  toolCalls?: ToolCall[];
  procedureUsed?: string | null;
  procedureUpdates?: ProcedureUpdate[];
  agentUsed?: string | null;
  isBriefing?: boolean;
}

const SUGGESTED_QUERIES = [
  {
    label: "Triage Stuck ROs",
    icon: AlertTriangle,
    gradient: "from-rose-500 to-red-600",
    iconBg: "bg-rose-500/10",
    iconColor: "text-rose-600",
    hover: "hover:shadow-rose-200/50",
    text: "Show me all stuck and critical ROs that need immediate attention",
  },
  {
    label: "Market Health",
    icon: BarChart3,
    gradient: "from-blue-500 to-indigo-600",
    iconBg: "bg-blue-500/10",
    iconColor: "text-blue-600",
    hover: "hover:shadow-blue-200/50",
    text: "How is the New York market doing? Show SCS% trends",
  },
  {
    label: "Quality Audit",
    icon: Search,
    gradient: "from-amber-500 to-orange-600",
    iconBg: "bg-amber-500/10",
    iconColor: "text-amber-600",
    hover: "hover:shadow-amber-200/50",
    text: "Run a record quality audit for Tennessee",
  },
  {
    label: "Retry Analysis",
    icon: RefreshCw,
    gradient: "from-emerald-500 to-teal-600",
    iconBg: "bg-emerald-500/10",
    iconColor: "text-emerald-600",
    hover: "hover:shadow-emerald-200/50",
    text: "Analyze retry effectiveness — are reprocessing attempts actually helping?",
  },
  {
    label: "Pipeline Heatmap",
    icon: Activity,
    gradient: "from-violet-500 to-purple-600",
    iconBg: "bg-violet-500/10",
    iconColor: "text-violet-600",
    hover: "hover:shadow-violet-200/50",
    text: "Show me the pipeline health heatmap for the top organizations",
  },
  {
    label: "Root Cause",
    icon: Zap,
    gradient: "from-indigo-500 to-violet-600",
    iconBg: "bg-indigo-500/10",
    iconColor: "text-indigo-600",
    hover: "hover:shadow-indigo-200/50",
    text: "Why does Tennessee have a low success rate? Trace the root cause",
  },
];

const BUILTIN_PROCEDURES: { cmd: string; name: string; trigger: string; paramHint?: string }[] = [
  { cmd: "triage", name: "triage_stuck_ros", trigger: "Run triage_stuck_ros for stuck and critical ROs", paramHint: "" },
  { cmd: "audit", name: "record_quality_audit", trigger: "Run record_quality_audit", paramHint: " for TN" },
  { cmd: "market", name: "market_health_report", trigger: "Run market_health_report", paramHint: " for NY" },
  { cmd: "retry", name: "retry_effectiveness_analysis", trigger: "Analyze retry effectiveness", paramHint: "" },
  { cmd: "report", name: "generate_pipeline_health_report", trigger: "Generate pipeline health report", paramHint: " for TN" },
  { cmd: "rootcause", name: "trace_root_cause", trigger: "Trace root cause for worst-performing market", paramHint: " or for NY" },
  { cmd: "clustering", name: "rejection_pattern_clustering", trigger: "Run rejection pattern clustering", paramHint: "" },
];

const TOOL_LABELS: Record<string, { label: string; color: string }> = {
  query_data: { label: "SQL Query", color: "bg-emerald-500/10 text-emerald-700 border-emerald-200/50" },
  run_procedure: { label: "Procedure", color: "bg-indigo-500/10 text-indigo-700 border-indigo-200/50" },
  create_chart: { label: "Chart", color: "bg-violet-500/10 text-violet-700 border-violet-200/50" },
  web_search: { label: "Web Search", color: "bg-blue-500/10 text-blue-700 border-blue-200/50" },
  recall_memory: { label: "Memory", color: "bg-amber-500/10 text-amber-700 border-amber-200/50" },
  update_procedure: { label: "Update", color: "bg-orange-500/10 text-orange-700 border-orange-200/50" },
  update_semantic_knowledge: { label: "Knowledge", color: "bg-teal-500/10 text-teal-700 border-teal-200/50" },
};

function ToolCallCard({ tc }: { tc: ToolCall }) {
  const [expanded, setExpanded] = useState(false);
  const meta = TOOL_LABELS[tc.tool] || { label: tc.tool, color: "bg-gray-500/10 text-gray-700 border-gray-200/50" };
  const hasData = tc.result?.data && Array.isArray(tc.result.data) && tc.result.data.length > 0;
  const hasError = !!tc.result?.error;
  const hasSummary = !!tc.result?.summary;

  return (
    <div className="rounded-xl overflow-hidden border border-border/50 bg-card/80 backdrop-blur-sm shadow-sm">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2.5 px-4 py-2.5 text-left hover:bg-muted/30 transition-colors cursor-pointer"
      >
        {expanded ? (
          <ChevronDown className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
        ) : (
          <ChevronRight className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
        )}
        <Terminal className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
        <Badge variant="outline" className={`text-[10px] font-semibold border ${meta.color}`}>
          {meta.label}
        </Badge>
        <span className="text-xs text-muted-foreground truncate flex-1">
          {tc.tool === "query_data" && tc.args.sql
            ? String(tc.args.sql).slice(0, 80) + (String(tc.args.sql).length > 80 ? "..." : "")
            : tc.tool === "run_procedure"
              ? String(tc.args.procedure_name || "")
              : tc.tool === "web_search"
                ? String(tc.args.query || "")
                : Object.values(tc.args).join(", ").slice(0, 60)}
        </span>
        {hasData && (
          <span className="text-[10px] text-muted-foreground shrink-0 flex items-center gap-1">
            <Table2 className="w-3 h-3" />
            {Number(tc.result?.row_count ?? 0)} rows
          </span>
        )}
        {hasError && <span className="text-[10px] text-destructive shrink-0">error</span>}
      </button>
      {expanded && (
        <div className="border-t border-border/50">
          {tc.tool === "query_data" && tc.args.sql != null && (
            <div className="px-4 py-2 bg-zinc-900 text-zinc-100 rounded-b-xl">
              <pre className="text-[11px] font-mono whitespace-pre-wrap overflow-x-auto leading-relaxed">
                {String(tc.args.sql)}
              </pre>
            </div>
          )}
          {hasError && (
            <div className="px-4 py-2 bg-destructive/10 text-destructive text-xs rounded-b-xl">
              {String(tc.result?.error ?? "")}
            </div>
          )}
          {hasSummary && !hasData && (
            <div className="px-4 py-2 text-xs text-muted-foreground rounded-b-xl">{String(tc.result?.summary ?? "")}</div>
          )}
          {hasData && (
            <DataTable columns={tc.result.columns || []} data={tc.result.data!} />
          )}
        </div>
      )}
    </div>
  );
}

function DataTable({ columns, data }: { columns: string[]; data: Record<string, unknown>[] }) {
  const [showAll, setShowAll] = useState(false);
  const displayData = showAll ? data : data.slice(0, 10);
  const cols = columns.length > 0 ? columns : Object.keys(data[0] || {});

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[11px]">
        <thead>
          <tr className="bg-muted/50">
            {cols.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-semibold text-muted-foreground whitespace-nowrap border-b border-border"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayData.map((row, ri) => (
            <tr key={ri} className={ri % 2 === 0 ? "bg-card" : "bg-muted/20"}>
              {cols.map((col) => (
                <td
                  key={col}
                  className="px-3 py-1.5 text-foreground whitespace-nowrap border-b border-border/50 max-w-[200px] truncate"
                >
                  {formatCell(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 10 && (
        <button
          onClick={() => setShowAll(!showAll)}
          className="w-full text-center py-1.5 text-[10px] text-primary hover:bg-primary/5 transition-colors cursor-pointer"
        >
          {showAll ? "Show less" : `Show all ${data.length} rows`}
        </button>
      )}
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "\u2014";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString();
    return value.toFixed(2);
  }
  return String(value);
}

function ToolCallsSection({ toolCalls }: { toolCalls: ToolCall[] }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="rounded-xl border border-border/50 bg-card/80 backdrop-blur-sm shadow-sm overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2.5 px-5 py-3 text-left hover:bg-muted/30 transition-colors cursor-pointer"
      >
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="w-4 h-4 text-muted-foreground" />
        )}
        <Database className="w-4 h-4 text-primary" />
        <span className="text-xs font-semibold text-foreground">
          {toolCalls.length} tool call{toolCalls.length > 1 ? "s" : ""}
        </span>
        <div className="flex gap-1.5 ml-auto">
          {toolCalls.map((tc, i) => {
            const meta = TOOL_LABELS[tc.tool] || { label: tc.tool, color: "bg-gray-500/10 text-gray-600 border-gray-200/50" };
            return (
              <Badge key={i} variant="outline" className={`text-[10px] border ${meta.color}`}>
                {meta.label}
              </Badge>
            );
          })}
        </div>
      </button>
      {expanded && (
        <div className="border-t border-border/50 p-3 space-y-2">
          {toolCalls.map((tc, i) => (
            <ToolCallCard key={i} tc={tc} />
          ))}
        </div>
      )}
    </div>
  );
}

function ProcedureLearningCard({ updates }: { updates: ProcedureUpdate[] }) {
  const [expanded, setExpanded] = useState(false);

  if (updates.length === 0) return null;

  const CHANGE_LABELS: Record<string, string> = {
    steps: "Steps updated",
    parameters: "Parameters updated",
    description: "Description changed",
    add_step: "New step added",
    modify_step: "Step modified",
    summary: "Change reason",
  };

  return (
    <div className="rounded-xl border border-emerald-200/60 bg-emerald-500/5 overflow-hidden shadow-sm">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2.5 px-5 py-3 text-left hover:bg-emerald-500/10 transition-colors cursor-pointer"
      >
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-emerald-600 shrink-0" />
        ) : (
          <ChevronRight className="w-4 h-4 text-emerald-600 shrink-0" />
        )}
        <Sparkles className="w-4 h-4 text-emerald-600 shrink-0" />
        <span className="text-xs font-semibold text-emerald-800">
          Procedural Learning — {updates.length} procedure{updates.length > 1 ? "s" : ""} updated
        </span>
        <div className="flex gap-1.5 ml-auto flex-wrap">
          {updates.map((u, i) => (
            <Badge
              key={i}
              variant="outline"
              className="text-[10px] border border-emerald-300/60 bg-emerald-50 text-emerald-700"
            >
              {u.procedure_name.replace(/_/g, " ")}
            </Badge>
          ))}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-emerald-200/50 divide-y divide-emerald-100/50">
          {updates.map((u, i) => (
            <div key={i} className="px-5 py-4 space-y-3">
              {/* Header: procedure name + version badge */}
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold text-emerald-900">
                  {u.procedure_name.replace(/_/g, " ")}
                </span>
                <div className="flex items-center gap-1.5 text-[11px] font-mono">
                  <span className="px-2 py-0.5 rounded-md bg-muted text-muted-foreground border border-border/50">
                    v{u.old_version}
                  </span>
                  <span className="text-muted-foreground">→</span>
                  <span className="px-2 py-0.5 rounded-md bg-emerald-100 text-emerald-800 border border-emerald-200/60 font-semibold">
                    v{u.new_version}
                  </span>
                </div>
              </div>

              {/* What changed */}
              {Object.keys(u.changes).length > 0 && (
                <div className="space-y-1.5">
                  <p className="text-[11px] font-semibold text-emerald-700 uppercase tracking-wide">
                    What changed
                  </p>
                  <ul className="space-y-1">
                    {Object.entries(u.changes).map(([key, val]) => (
                      <li key={key} className="flex items-start gap-2 text-xs text-emerald-900">
                        <span className="mt-0.5 w-1.5 h-1.5 rounded-full bg-emerald-500 shrink-0" />
                        <span>
                          <span className="font-medium">{CHANGE_LABELS[key] ?? key}:</span>{" "}
                          {String(val)}
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Why it changed */}
              {u.change_description && (
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold text-emerald-700 uppercase tracking-wide">
                    Why
                  </p>
                  <p className="text-xs text-emerald-800 leading-relaxed">{u.change_description}</p>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function CreateProcedureModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: () => void;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [sql, setSql] = useState("SELECT * FROM roster WHERE IS_STUCK = 1 LIMIT 10");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async () => {
    if (!name.trim()) {
      setError("Name is required");
      return;
    }
    if (!sql.trim()) {
      setError("SQL query is required");
      return;
    }
    setError("");
    setLoading(true);
    try {
      await createProcedure({
        name: name.trim(),
        description: description.trim() || `Custom procedure: ${name.trim()}`,
        steps: [{ action: "query", sql: sql.trim(), description: "Custom query" }],
      });
      onCreated();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="w-full max-w-lg rounded-2xl border border-border/60 bg-card shadow-2xl p-6 mx-4"
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-foreground">Create custom procedure</h3>
          <button
            type="button"
            onClick={onClose}
            className="p-1 rounded-lg hover:bg-muted/50 transition-colors cursor-pointer"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-foreground mb-1.5">Name</label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my_custom_procedure"
              className="w-full px-4 py-2.5 rounded-xl border border-border/60 bg-background text-foreground text-sm focus:outline-none focus:ring-2 focus:ring-primary/30"
            />
            <p className="text-xs text-muted-foreground mt-1">Use snake_case (e.g. failed_ros_by_state)</p>
          </div>
          <div>
            <label className="block text-sm font-medium text-foreground mb-1.5">Description</label>
            <input
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What does this procedure do?"
              className="w-full px-4 py-2.5 rounded-xl border border-border/60 bg-background text-foreground text-sm focus:outline-none focus:ring-2 focus:ring-primary/30"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-foreground mb-1.5">SQL query</label>
            <textarea
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              placeholder="SELECT ... FROM roster ..."
              rows={6}
              className="w-full px-4 py-2.5 rounded-xl border border-border/60 bg-background text-foreground text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary/30 resize-none"
            />
            <p className="text-xs text-muted-foreground mt-1">Use UPPERCASE column names. Params: {"{state}"}, {"{market}"}</p>
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </div>
        <div className="flex gap-3 mt-6">
          <button
            type="button"
            onClick={onClose}
            className="flex-1 px-4 py-2.5 rounded-xl border border-border/60 hover:bg-muted/50 transition-colors text-sm font-medium cursor-pointer"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={loading}
            className="flex-1 px-4 py-2.5 rounded-xl bg-gradient-to-r from-indigo-600 to-violet-600 text-white font-medium text-sm hover:opacity-90 disabled:opacity-50 cursor-pointer flex items-center justify-center gap-2"
          >
            <Plus className="w-4 h-4" />
            {loading ? "Creating..." : "Create"}
          </button>
        </div>
      </motion.div>
    </div>
  );
}

function ChatPageInner() {
  const searchParams = useSearchParams();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string>("");
  const [prefillHandled, setPrefillHandled] = useState(false);
  const [showScrollDown, setShowScrollDown] = useState(false);
  const [selectedSlashIndex, setSelectedSlashIndex] = useState(0);
  const [customProcedures, setCustomProcedures] = useState<{ cmd: string; name: string; trigger: string; paramHint?: string }[]>([]);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const procedureSlashCommands = [
    ...BUILTIN_PROCEDURES,
    ...customProcedures,
    { cmd: "create", name: "Create custom procedure", trigger: "__CREATE__", paramHint: "" },
  ];

  const showSlashPalette = input.startsWith("/");
  const slashQuery = input.slice(1).toLowerCase();
  const filteredProcedures = procedureSlashCommands.filter(
    (p) => !slashQuery || p.cmd.startsWith(slashQuery) || p.name.toLowerCase().includes(slashQuery)
  );
  const selectedProcedure = filteredProcedures[selectedSlashIndex] ?? filteredProcedures[0];

  const scrollToBottom = () => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
    setShowScrollDown(false);
  };

  const handleScroll = () => {
    const el = scrollContainerRef.current;
    if (!el || messages.length === 0) return;
    const { scrollTop, clientHeight, scrollHeight } = el;
    const isNearBottom = scrollTop + clientHeight >= scrollHeight - 80;
    setShowScrollDown(!isNearBottom);
  };

  useEffect(() => {
    const id = crypto.randomUUID();
    setSessionId(id);
    getSessionBriefing(id).then((data) => {
      if (data?.has_briefing && data.briefing) {
        setMessages([{ role: "assistant", content: data.briefing, isBriefing: true }]);
      }
    }).catch(() => {});
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (showSlashPalette) setSelectedSlashIndex(0);
  }, [slashQuery, showSlashPalette]);

  useEffect(() => {
    getProceduralMemory().then((data) => {
      if (data && typeof data === "object") {
        const procs = data as Record<string, { name: string; description?: string }>;
        const builtinNames = new Set(BUILTIN_PROCEDURES.map((p) => p.name));
        const custom = Object.keys(procs)
          .filter((k) => !builtinNames.has(k))
          .map((k) => ({
            cmd: k.replace(/_/g, "").slice(0, 12),
            name: k,
            trigger: `Run ${k}`,
            paramHint: "",
          }));
        setCustomProcedures(custom);
      }
    }).catch(() => {});
  }, [showCreateModal]);

  useEffect(() => {
    if (prefillHandled || !sessionId) return;
    const prefill = searchParams.get("prefill");
    if (prefill) {
      setPrefillHandled(true);
      handleSend(prefill);
    }
  }, [sessionId, prefillHandled, searchParams]);

  const handleSend = async (text?: string) => {
    const query = text || input;
    if (!query.trim()) return;

    const userMsg: Message = { role: "user", content: query };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const response = await sendChat(query, sessionId);
      const assistantMsg: Message = {
        role: "assistant",
        content: response.message,
        charts: response.charts,
        webResults: response.web_search_results,
        memoryUpdates: response.memory_updates,
        toolCalls: response.tool_calls,
        procedureUsed: response.procedure_used,
        procedureUpdates: response.procedure_updates,
        agentUsed: response.agent_used,
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Error: ${error instanceof Error ? error.message : "Unknown error"}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.3 }}
      className="flex flex-col h-screen"
    >
      {/* Header */}
      <div className="shrink-0 px-8 py-6 border-b border-border/50 bg-card/40 backdrop-blur-xl">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-indigo-500 via-violet-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20">
            <Sparkles className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-foreground tracking-tight">AI Chat</h1>
            <p className="text-sm text-muted-foreground mt-0.5">
              Ask RosterIQ about pipeline health, market metrics, or data quality
            </p>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div
        ref={scrollContainerRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto px-6 md:px-8 py-6"
      >
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center min-h-[50vh] space-y-12">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4 }}
              className="text-center space-y-4"
            >
              <div className="w-24 h-24 rounded-3xl bg-gradient-to-br from-indigo-500 via-violet-500 to-purple-600 flex items-center justify-center mx-auto shadow-xl shadow-indigo-500/25">
                <span className="text-white text-4xl font-bold">R</span>
              </div>
              <h2 className="text-3xl font-bold text-foreground">Welcome to RosterIQ</h2>
              <p className="text-muted-foreground max-w-lg text-base leading-relaxed mx-auto">
                I can analyze pipeline health, diagnose stuck ROs, correlate market metrics, and generate visual insights.
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4, delay: 0.1 }}
              className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-4xl w-full"
            >
              {SUGGESTED_QUERIES.map((q, i) => {
                const Icon = q.icon;
                return (
                  <motion.button
                    key={q.label}
                    onClick={() => handleSend(q.text)}
                    whileHover={{ y: -4, scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    transition={{ type: "spring", stiffness: 400, damping: 17 }}
                    className={`group text-left p-6 rounded-2xl border border-border/60 bg-card/80 backdrop-blur-sm shadow-sm transition-all duration-300 cursor-pointer hover:shadow-xl ${q.hover} hover:border-primary/30`}
                  >
                    <div className={`w-11 h-11 rounded-xl ${q.iconBg} flex items-center justify-center mb-3 group-hover:scale-110 transition-transform`}>
                      <Icon className={`w-5 h-5 ${q.iconColor}`} />
                    </div>
                    <h3 className="font-semibold text-foreground mb-1.5">{q.label}</h3>
                    <p className="text-sm text-muted-foreground leading-relaxed">{q.text}</p>
                  </motion.button>
                );
              })}
            </motion.div>
          </div>
        ) : (
          <div className="space-y-8 max-w-4xl mx-auto">
            <AnimatePresence mode="popLayout">
              {messages.map((msg, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.25 }}
                  className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                >
                  <div className={`max-w-[85%] ${msg.role === "user" ? "" : "space-y-4"}`}>
                    {msg.role === "user" ? (
                      <div className="bg-gradient-to-r from-indigo-600 to-violet-600 text-white rounded-2xl rounded-br-md px-6 py-4 shadow-lg shadow-indigo-500/20">
                        <div className="text-sm whitespace-pre-wrap leading-relaxed">{msg.content}</div>
                      </div>
                    ) : (
                      <>
                        <div className="flex items-center gap-2 mb-2">
                          <div
                            className={`w-9 h-9 rounded-xl flex items-center justify-center shadow-sm ${
                              msg.isBriefing
                                ? "bg-gradient-to-br from-amber-400 to-orange-500"
                                : "bg-gradient-to-br from-indigo-500 to-violet-600"
                            }`}
                          >
                            <span className="text-white text-xs font-bold">{msg.isBriefing ? "M" : "R"}</span>
                          </div>
                          <span className="text-sm font-semibold text-foreground">
                            {msg.isBriefing ? "Memory Briefing" : "RosterIQ"}
                          </span>
                          {msg.isBriefing && (
                            <Badge variant="secondary" className="text-xs">
                              Episodic Memory
                            </Badge>
                          )}
                          {msg.agentUsed && !msg.isBriefing && (
                            <Badge variant="outline" className="text-xs font-medium">
                              {msg.agentUsed}
                            </Badge>
                          )}
                          {msg.procedureUsed && (
                            <Badge className="text-xs bg-indigo-500/10 text-indigo-700 border-indigo-200/50">
                              {msg.procedureUsed}
                            </Badge>
                          )}
                        </div>

                        {msg.toolCalls && msg.toolCalls.length > 0 && (
                          <ToolCallsSection toolCalls={msg.toolCalls} />
                        )}

                        {msg.procedureUpdates && msg.procedureUpdates.length > 0 && (
                          <ProcedureLearningCard updates={msg.procedureUpdates} />
                        )}

                        <Card className="overflow-hidden border-border/50 shadow-sm">
                          <div
                            className={
                              msg.isBriefing
                                ? "bg-amber-500/5 border border-amber-200/50 p-6"
                                : "p-6"
                            }
                          >
                            <div className="prose prose-sm prose-zinc max-w-none dark:prose-invert prose-headings:text-foreground prose-p:text-muted-foreground prose-strong:text-foreground prose-a:text-primary prose-code:bg-primary/5 prose-code:text-primary prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded-md">
                              <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
                            </div>
                          </div>
                        </Card>

                        {msg.charts &&
                          msg.charts.length > 0 &&
                          msg.charts.map((chart, ci) => (
                            <PlotlyChart key={ci} data={chart as Record<string, unknown>} />
                          ))}

                        {msg.webResults && msg.webResults.length > 0 && (
                          <Card className="border-blue-200/50 bg-blue-500/5 overflow-hidden">
                            <div className="p-5">
                              <div className="flex items-center gap-2 mb-3">
                                <Globe className="w-4 h-4 text-blue-600" />
                                <p className="text-xs font-bold text-blue-800">Web Search Results</p>
                              </div>
                              {msg.webResults.map((wr, wi) => (
                                <div key={wi} className="mb-3">
                                  <p className="text-xs text-blue-600 font-medium mb-1.5">Query: {wr.query}</p>
                                  {wr.results?.map((r, ri) => (
                                    <div
                                      key={ri}
                                      className="ml-3 mt-1.5 p-3 bg-card rounded-xl border border-blue-100/50"
                                    >
                                      <a
                                        href={r.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-xs text-blue-600 font-semibold hover:underline"
                                      >
                                        {r.title}
                                      </a>
                                      <p className="text-xs text-muted-foreground line-clamp-2 mt-1">{r.content}</p>
                                    </div>
                                  ))}
                                </div>
                              ))}
                            </div>
                          </Card>
                        )}

                        {msg.memoryUpdates?.state_changes && msg.memoryUpdates.state_changes.length > 0 && (
                          <Card className="border-amber-200/50 bg-amber-500/5 overflow-hidden">
                            <div className="p-5">
                              <p className="text-xs font-bold text-amber-800 mb-2">State Changes Detected</p>
                              {msg.memoryUpdates.state_changes.map((sc, si) => (
                                <p key={si} className="text-xs text-amber-700">
                                  {sc.entity}: {sc.field} {sc.old} → {sc.new} {sc.note && `(${sc.note})`}
                                </p>
                              ))}
                            </div>
                          </Card>
                        )}
                      </>
                    )}
                  </div>
                </motion.div>
              ))}
            </AnimatePresence>

            {loading && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="flex justify-start"
              >
                <div className="rounded-2xl border border-border/50 bg-card/80 backdrop-blur-sm p-6 shadow-sm">
                  <div className="flex items-center gap-3">
                    <div className="flex gap-1.5">
                      <div className="w-2.5 h-2.5 bg-primary rounded-full animate-bounce" />
                      <div className="w-2.5 h-2.5 bg-primary rounded-full animate-bounce [animation-delay:0.15s]" />
                      <div className="w-2.5 h-2.5 bg-primary rounded-full animate-bounce [animation-delay:0.3s]" />
                    </div>
                    <span className="text-sm text-muted-foreground font-medium">Analyzing data...</span>
                  </div>
                </div>
              </motion.div>
            )}

            <div ref={scrollRef} />
          </div>
        )}
      </div>

      {/* Input — glass effect + gradient send */}
      <div className="shrink-0 relative px-6 md:px-8 py-6 bg-card/40 backdrop-blur-xl border-t border-border/50">
        <AnimatePresence>
          {showScrollDown && messages.length > 0 && (
            <motion.button
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 8 }}
              onClick={scrollToBottom}
              className="absolute left-1/2 -translate-x-1/2 bottom-full mb-3 px-4 py-2 rounded-xl bg-card/90 backdrop-blur-sm border border-border/60 shadow-lg hover:bg-card transition-colors flex items-center gap-2 text-sm font-medium text-foreground cursor-pointer z-10"
            >
              <ChevronDown className="w-4 h-4" />
              Go to bottom
            </motion.button>
          )}
        </AnimatePresence>
        <div className="max-w-4xl mx-auto flex gap-4">
          <div className="flex-1 relative">
            <input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (showSlashPalette) {
                  if (e.key === "ArrowDown") {
                    e.preventDefault();
                    setSelectedSlashIndex((i) => Math.min(i + 1, filteredProcedures.length - 1));
                    return;
                  }
                  if (e.key === "ArrowUp") {
                    e.preventDefault();
                    setSelectedSlashIndex((i) => Math.max(i - 1, 0));
                    return;
                  }
                  if (e.key === "Enter" && selectedProcedure) {
                    e.preventDefault();
                    if (selectedProcedure.trigger === "__CREATE__") {
                      setShowCreateModal(true);
                      setInput("");
                    } else {
                      setInput(selectedProcedure.trigger + (selectedProcedure.paramHint ?? ""));
                    }
                    return;
                  }
                  if (e.key === "Escape") {
                    setInput("");
                    return;
                  }
                }
                if (e.key === "Enter" && !e.shiftKey) handleSend();
              }}
              placeholder="Ask about pipeline health... or type / for procedures"
              className="w-full px-6 py-4 rounded-2xl border-2 border-border/60 bg-background/80 backdrop-blur-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary/50 focus:ring-2 focus:ring-primary/20 text-sm transition-all duration-200 shadow-sm"
              disabled={loading}
            />
            <AnimatePresence>
              {showSlashPalette && (
                <motion.div
                  initial={{ opacity: 0, y: -4 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -4 }}
                  className="absolute bottom-full left-0 right-0 mb-2 rounded-xl border border-border/60 bg-card/95 backdrop-blur-xl shadow-xl overflow-hidden z-20"
                >
                  <div className="px-3 py-2 border-b border-border/50 text-xs font-medium text-muted-foreground">
                    Procedures — select one to run
                  </div>
                  <div className="max-h-64 overflow-y-auto py-1">
                    {filteredProcedures.length === 0 ? (
                      <div className="px-4 py-3 text-sm text-muted-foreground">No matching procedure</div>
                    ) : (
                      filteredProcedures.map((p, i) => (
                        <button
                          key={p.name}
                          type="button"
                          onClick={() => {
                            if (p.trigger === "__CREATE__") {
                              setShowCreateModal(true);
                              setInput("");
                            } else {
                              setInput(p.trigger + (p.paramHint ?? ""));
                            }
                          }}
                          onMouseEnter={() => setSelectedSlashIndex(i)}
                          className={`w-full text-left px-4 py-2.5 flex items-center gap-3 transition-colors cursor-pointer ${
                            i === selectedSlashIndex ? "bg-primary/10 text-primary" : "hover:bg-muted/50"
                          }`}
                        >
                          <span className="text-xs font-mono text-muted-foreground">/{p.cmd}</span>
                          <span className="text-sm font-medium">{p.name.replace(/_/g, " ")}</span>
                        </button>
                      ))
                    )}
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
          <motion.button
            onClick={() => handleSend()}
            disabled={loading || !input.trim()}
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            className="px-8 py-4 rounded-2xl bg-gradient-to-r from-indigo-600 via-violet-600 to-purple-600 text-white font-semibold text-sm shadow-lg shadow-indigo-500/25 hover:shadow-xl hover:shadow-indigo-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-all duration-200 flex items-center gap-2"
          >
            <Send className="w-4 h-4" />
            Send
          </motion.button>
        </div>
      </div>

      <AnimatePresence>
        {showCreateModal && (
          <CreateProcedureModal
            onClose={() => setShowCreateModal(false)}
            onCreated={() => getProceduralMemory().then((data) => {
              if (data && typeof data === "object") {
                const procs = data as Record<string, unknown>;
                const builtinNames = new Set(BUILTIN_PROCEDURES.map((p) => p.name));
                const custom = Object.keys(procs)
                  .filter((k) => !builtinNames.has(k))
                  .map((k) => ({ cmd: k.replace(/_/g, "").slice(0, 12), name: k, trigger: `Run ${k}`, paramHint: "" }));
                setCustomProcedures(custom);
              }
            })}
          />
        )}
      </AnimatePresence>
    </motion.div>
  );
}

export default function ChatPage() {
  return (
    <Suspense>
      <ChatPageInner />
    </Suspense>
  );
}

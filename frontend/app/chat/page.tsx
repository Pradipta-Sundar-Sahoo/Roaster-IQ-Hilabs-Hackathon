"use client";

import { useState, useRef, useEffect } from "react";
import { sendChat, getSessionBriefing, type ChatResponse, type ToolCall } from "@/lib/api";
import { PlotlyChart } from "@/components/charts/PlotlyChart";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Send, AlertTriangle, Globe, Sparkles, BarChart3, Search, RefreshCw, Activity, Zap, ChevronDown, ChevronRight, Database, Terminal, Table2 } from "lucide-react";
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
  agentUsed?: string | null;
  isBriefing?: boolean;
}

const SUGGESTED_QUERIES = [
  {
    label: "Triage Stuck ROs",
    icon: AlertTriangle,
    color: "bg-red-50 text-red-700 border-red-100 hover:bg-red-100 hover:border-red-200",
    iconColor: "text-red-500",
    text: "Show me all stuck and critical ROs that need immediate attention",
  },
  {
    label: "Market Health",
    icon: BarChart3,
    color: "bg-blue-50 text-blue-700 border-blue-100 hover:bg-blue-100 hover:border-blue-200",
    iconColor: "text-blue-500",
    text: "How is the New York market doing? Show SCS% trends",
  },
  {
    label: "Quality Audit",
    icon: Search,
    color: "bg-amber-50 text-amber-700 border-amber-100 hover:bg-amber-100 hover:border-amber-200",
    iconColor: "text-amber-500",
    text: "Run a record quality audit for Tennessee",
  },
  {
    label: "Retry Analysis",
    icon: RefreshCw,
    color: "bg-emerald-50 text-emerald-700 border-emerald-100 hover:bg-emerald-100 hover:border-emerald-200",
    iconColor: "text-emerald-500",
    text: "Analyze retry effectiveness — are reprocessing attempts actually helping?",
  },
  {
    label: "Pipeline Heatmap",
    icon: Activity,
    color: "bg-purple-50 text-purple-700 border-purple-100 hover:bg-purple-100 hover:border-purple-200",
    iconColor: "text-purple-500",
    text: "Show me the pipeline health heatmap for the top organizations",
  },
  {
    label: "Root Cause",
    icon: Zap,
    color: "bg-indigo-50 text-indigo-700 border-indigo-100 hover:bg-indigo-100 hover:border-indigo-200",
    iconColor: "text-indigo-500",
    text: "Why does Tennessee have a low success rate? Trace the root cause",
  },
];

const TOOL_LABELS: Record<string, { label: string; color: string }> = {
  query_data: { label: "SQL Query", color: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  run_procedure: { label: "Procedure", color: "bg-indigo-50 text-indigo-700 border-indigo-200" },
  create_chart: { label: "Chart", color: "bg-purple-50 text-purple-700 border-purple-200" },
  web_search: { label: "Web Search", color: "bg-blue-50 text-blue-700 border-blue-200" },
  recall_memory: { label: "Memory", color: "bg-amber-50 text-amber-700 border-amber-200" },
  update_procedure: { label: "Update", color: "bg-orange-50 text-orange-700 border-orange-200" },
  update_semantic_knowledge: { label: "Knowledge", color: "bg-teal-50 text-teal-700 border-teal-200" },
};

function ToolCallCard({ tc }: { tc: ToolCall }) {
  const [expanded, setExpanded] = useState(false);
  const meta = TOOL_LABELS[tc.tool] || { label: tc.tool, color: "bg-gray-50 text-gray-700 border-gray-200" };
  const hasData = tc.result?.data && Array.isArray(tc.result.data) && tc.result.data.length > 0;
  const hasError = !!tc.result?.error;
  const hasSummary = !!tc.result?.summary;

  return (
    <div className="border border-gray-100 rounded-xl overflow-hidden bg-gray-50/50">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2.5 px-4 py-2.5 text-left hover:bg-gray-50 transition-colors cursor-pointer"
      >
        {expanded ? (
          <ChevronDown className="w-3.5 h-3.5 text-gray-400 shrink-0" />
        ) : (
          <ChevronRight className="w-3.5 h-3.5 text-gray-400 shrink-0" />
        )}
        <Terminal className="w-3.5 h-3.5 text-gray-400 shrink-0" />
        <Badge className={`text-[10px] font-semibold border ${meta.color}`}>{meta.label}</Badge>
        <span className="text-xs text-gray-500 truncate flex-1">
          {tc.tool === "query_data" && tc.args.sql
            ? String(tc.args.sql).slice(0, 80) + (String(tc.args.sql).length > 80 ? "..." : "")
            : tc.tool === "run_procedure"
              ? String(tc.args.procedure_name || "")
              : tc.tool === "web_search"
                ? String(tc.args.query || "")
                : Object.values(tc.args).join(", ").slice(0, 60)}
        </span>
        {hasData && (
          <span className="text-[10px] text-gray-400 shrink-0 flex items-center gap-1">
            <Table2 className="w-3 h-3" />
            {tc.result.row_count} rows
          </span>
        )}
        {hasError && (
          <span className="text-[10px] text-red-500 shrink-0">error</span>
        )}
      </button>

      {expanded && (
        <div className="border-t border-gray-100">
          {/* SQL display */}
          {tc.tool === "query_data" && tc.args.sql ? (
            <div className="px-4 py-2 bg-gray-900 text-gray-100">
              <pre className="text-[11px] font-mono whitespace-pre-wrap overflow-x-auto leading-relaxed">{String(tc.args.sql)}</pre>
            </div>
          ) : null}

          {/* Error display */}
          {hasError && (
            <div className="px-4 py-2 bg-red-50 text-red-700 text-xs">{tc.result.error}</div>
          )}

          {/* Summary display */}
          {hasSummary && !hasData && (
            <div className="px-4 py-2 text-xs text-gray-700">{tc.result.summary}</div>
          )}

          {/* Data table */}
          {hasData && <DataTable columns={tc.result.columns || []} data={tc.result.data!} />}
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
          <tr className="bg-gray-100">
            {cols.map((col) => (
              <th key={col} className="px-3 py-2 text-left font-semibold text-gray-600 whitespace-nowrap border-b border-gray-200">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayData.map((row, ri) => (
            <tr key={ri} className={ri % 2 === 0 ? "bg-white" : "bg-gray-50/50"}>
              {cols.map((col) => (
                <td key={col} className="px-3 py-1.5 text-gray-700 whitespace-nowrap border-b border-gray-50 max-w-[200px] truncate">
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
          className="w-full text-center py-1.5 text-[10px] text-indigo-600 hover:text-indigo-800 hover:bg-indigo-50/50 transition-colors cursor-pointer"
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

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string>("");
  const scrollRef = useRef<HTMLDivElement>(null);

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
    <div className="flex flex-col h-screen bg-[#f8f9fb]">
      {/* Header */}
      <div className="border-b border-gray-100 px-8 py-5 bg-white">
        <div className="flex items-center gap-3">
          <Sparkles className="w-5 h-5 text-indigo-500" />
          <div>
            <h2 className="text-xl font-bold text-gray-900">AI Chat</h2>
            <p className="text-sm text-gray-400 mt-0.5">
              Ask RosterIQ about pipeline health, market metrics, or data quality
            </p>
          </div>
        </div>
      </div>

      {/* Messages */}
      <ScrollArea className="flex-1 px-8 py-6">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full space-y-10">
            <div className="text-center space-y-4">
              <div className="w-20 h-20 rounded-3xl bg-gradient-to-br from-indigo-500 to-blue-600 flex items-center justify-center mx-auto shadow-lg shadow-indigo-200">
                <span className="text-white text-3xl font-bold">R</span>
              </div>
              <h3 className="text-3xl font-bold text-gray-900">Welcome to RosterIQ</h3>
              <p className="text-gray-500 max-w-lg text-base leading-relaxed mx-auto">
                I can analyze pipeline health, diagnose stuck ROs, correlate market metrics,
                and generate visual insights. Try one of these:
              </p>
            </div>
            <div className="grid grid-cols-2 gap-4 max-w-2xl w-full">
              {SUGGESTED_QUERIES.map((q) => {
                const Icon = q.icon;
                return (
                  <button
                    key={q.label}
                    onClick={() => handleSend(q.text)}
                    className={`text-left p-6 rounded-2xl border-2 transition-all duration-200 cursor-pointer group ${q.color}`}
                  >
                    <div className="flex items-center gap-2.5 mb-2">
                      <Icon className={`w-5 h-5 ${q.iconColor}`} />
                      <span className="font-bold text-sm">{q.label}</span>
                    </div>
                    <p className="text-sm opacity-75 leading-relaxed">{q.text}</p>
                  </button>
                );
              })}
            </div>
          </div>
        ) : (
          <div className="space-y-6 max-w-4xl mx-auto">
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <div
                  className={`max-w-[85%] ${
                    msg.role === "user"
                      ? "bg-indigo-600 text-white rounded-2xl rounded-br-sm px-6 py-4 shadow-md shadow-indigo-100"
                      : "space-y-4"
                  }`}
                >
                  {msg.role === "assistant" && (
                    <div className="flex items-center gap-2 mb-2">
                      <div className={`w-8 h-8 rounded-xl flex items-center justify-center shadow-sm ${msg.isBriefing ? "bg-gradient-to-br from-amber-400 to-orange-500" : "bg-gradient-to-br from-indigo-500 to-blue-600"}`}>
                        <span className="text-white text-xs font-bold">{msg.isBriefing ? "M" : "R"}</span>
                      </div>
                      <span className="text-sm font-semibold text-gray-800">{msg.isBriefing ? "Memory Briefing" : "RosterIQ"}</span>
                      {msg.isBriefing && (
                        <Badge className="text-xs bg-amber-50 text-amber-700 hover:bg-amber-100 border border-amber-200">
                          Episodic Memory
                        </Badge>
                      )}
                      {msg.agentUsed && !msg.isBriefing && (
                        <Badge variant="outline" className="text-xs font-medium border-gray-200 text-gray-500">
                          {msg.agentUsed}
                        </Badge>
                      )}
                      {msg.procedureUsed && (
                        <Badge className="text-xs bg-indigo-50 text-indigo-600 hover:bg-indigo-100 border border-indigo-100">
                          {msg.procedureUsed}
                        </Badge>
                      )}
                    </div>
                  )}

                  {/* Tool Calls — collapsible, shown before the response text */}
                  {msg.toolCalls && msg.toolCalls.length > 0 && (
                    <ToolCallsSection toolCalls={msg.toolCalls} />
                  )}

                  <div className={msg.role === "assistant" ? (msg.isBriefing ? "bg-amber-50 rounded-2xl border border-amber-100 p-6 shadow-sm" : "bg-white rounded-2xl border border-gray-100 p-6 shadow-sm") : ""}>
                    {msg.role === "assistant" ? (
                      <div className="prose prose-sm prose-gray max-w-none prose-headings:text-gray-900 prose-headings:font-bold prose-h3:text-base prose-h3:mt-4 prose-h3:mb-2 prose-h2:text-lg prose-h2:mt-5 prose-h2:mb-3 prose-p:text-gray-700 prose-p:leading-relaxed prose-li:text-gray-700 prose-strong:text-gray-900 prose-a:text-indigo-600 prose-code:text-indigo-600 prose-code:bg-indigo-50 prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded-md prose-code:text-xs prose-code:font-medium prose-code:before:content-none prose-code:after:content-none prose-table:text-sm prose-th:bg-gray-50 prose-th:px-3 prose-th:py-2 prose-td:px-3 prose-td:py-2 prose-td:border-gray-100 prose-th:border-gray-200 prose-hr:border-gray-100">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>
                          {msg.content}
                        </ReactMarkdown>
                      </div>
                    ) : (
                      <div className="text-sm whitespace-pre-wrap leading-relaxed">
                        {msg.content}
                      </div>
                    )}
                  </div>

                  {msg.charts && msg.charts.length > 0 && (
                    <div className="space-y-4">
                      {msg.charts.map((chart, ci) => (
                        <PlotlyChart key={ci} data={chart} />
                      ))}
                    </div>
                  )}

                  {msg.webResults && msg.webResults.length > 0 && (
                    <Card className="bg-blue-50/60 border-blue-100 p-5 rounded-2xl shadow-sm">
                      <div className="flex items-center gap-2 mb-3">
                        <Globe className="w-4 h-4 text-blue-500" />
                        <p className="text-xs font-bold text-blue-800">Web Search Results</p>
                      </div>
                      {msg.webResults.map((wr, wi) => (
                        <div key={wi} className="mb-3">
                          <p className="text-xs text-blue-600 font-medium mb-1.5">Query: {wr.query}</p>
                          {wr.results?.map((r, ri) => (
                            <div key={ri} className="ml-3 mt-1.5 p-3 bg-white rounded-xl border border-blue-50">
                              <a
                                href={r.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-xs text-blue-600 font-semibold hover:underline"
                              >
                                {r.title}
                              </a>
                              <p className="text-xs text-gray-500 line-clamp-2 mt-1">{r.content}</p>
                            </div>
                          ))}
                        </div>
                      ))}
                    </Card>
                  )}

                  {msg.memoryUpdates?.state_changes && msg.memoryUpdates.state_changes.length > 0 && (
                    <Card className="bg-amber-50/60 border-amber-100 p-5 rounded-2xl shadow-sm">
                      <p className="text-xs font-bold text-amber-800 mb-2">State Changes Detected</p>
                      {msg.memoryUpdates.state_changes.map((sc, si) => (
                        <p key={si} className="text-xs text-amber-700">
                          {sc.entity}: {sc.field} {sc.old} → {sc.new} {sc.note && `(${sc.note})`}
                        </p>
                      ))}
                    </Card>
                  )}
                </div>
              </div>
            ))}

            {loading && (
              <div className="flex justify-start">
                <div className="bg-white rounded-2xl border border-gray-100 p-6 shadow-sm">
                  <div className="flex items-center gap-3">
                    <div className="flex gap-1.5">
                      <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" />
                      <div className="w-2.5 h-2.5 bg-indigo-400 rounded-full animate-bounce [animation-delay:0.15s]" />
                      <div className="w-2.5 h-2.5 bg-indigo-300 rounded-full animate-bounce [animation-delay:0.3s]" />
                    </div>
                    <span className="text-sm text-gray-400 font-medium">Analyzing data...</span>
                  </div>
                </div>
              </div>
            )}

            <div ref={scrollRef} />
          </div>
        )}
      </ScrollArea>

      {/* Input */}
      <div className="border-t border-gray-100 p-5 bg-white">
        <div className="max-w-4xl mx-auto flex gap-3">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
            placeholder="Ask about pipeline health, market metrics, stuck ROs..."
            className="flex-1 px-6 py-4 rounded-2xl border-2 border-gray-100 bg-gray-50/50 text-gray-900 placeholder:text-gray-400 focus:outline-none focus:border-indigo-300 focus:bg-white text-sm transition-all duration-200"
            disabled={loading}
          />
          <button
            onClick={() => handleSend()}
            disabled={loading || !input.trim()}
            className="px-8 py-4 rounded-2xl bg-indigo-600 text-white font-semibold text-sm hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-all duration-200 shadow-md shadow-indigo-200 hover:shadow-lg hover:shadow-indigo-200 flex items-center gap-2"
          >
            <Send className="w-4 h-4" />
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

function ToolCallsSection({ toolCalls }: { toolCalls: ToolCall[] }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="rounded-2xl border border-gray-100 bg-white shadow-sm overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-2.5 px-5 py-3 text-left hover:bg-gray-50/50 transition-colors cursor-pointer"
      >
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-400" />
        )}
        <Database className="w-4 h-4 text-indigo-400" />
        <span className="text-xs font-semibold text-gray-600">
          {toolCalls.length} tool call{toolCalls.length > 1 ? "s" : ""}
        </span>
        <div className="flex gap-1.5 ml-auto">
          {toolCalls.map((tc, i) => {
            const meta = TOOL_LABELS[tc.tool] || { label: tc.tool, color: "bg-gray-50 text-gray-600 border-gray-200" };
            return (
              <Badge key={i} className={`text-[10px] border ${meta.color}`}>
                {meta.label}
              </Badge>
            );
          })}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-gray-100 p-3 space-y-2">
          {toolCalls.map((tc, i) => (
            <ToolCallCard key={i} tc={tc} />
          ))}
        </div>
      )}
    </div>
  );
}

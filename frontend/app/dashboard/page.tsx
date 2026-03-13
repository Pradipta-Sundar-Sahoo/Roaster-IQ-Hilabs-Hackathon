"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import {
  getDashboardOverview,
  getAlerts,
  getIntelligence,
  getLatestReport,
  getDashboardOptions,
  getDashboardChart,
} from "@/lib/api";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { PlotlyChart } from "@/components/charts/PlotlyChart";
import {
  LayoutDashboard,
  Layers,
  AlertTriangle,
  XCircle,
  ShieldAlert,
  Bell,
  Brain,
  Target,
  RefreshCw,
  Play,
  TrendingDown,
  Lightbulb,
  Activity,
  FileText,
  ChevronDown,
  ChevronRight,
  BarChart3,
  MapPin,
  Clock,
} from "lucide-react";
import type { Alert, IntelligenceData, PipelineReport } from "@/lib/api";

interface DashboardData {
  total_ros: number;
  stuck_ros: number;
  failed_ros: number;
  red_health_flags: Record<string, number>;
  latest_month: string;
  market_summary: { MARKET: string; SCS_PERCENT: number }[];
}

const severityStyles: Record<string, string> = {
  high: "bg-red-50 text-red-600 border-red-100",
  medium: "bg-amber-50 text-amber-600 border-amber-100",
  info: "bg-gray-50 text-gray-500 border-gray-200",
};

const healthStatusColors: Record<string, { bg: string; text: string; ring: string }> = {
  healthy: { bg: "bg-emerald-50", text: "text-emerald-700", ring: "ring-emerald-200" },
  warning: { bg: "bg-amber-50", text: "text-amber-700", ring: "ring-amber-200" },
  degraded: { bg: "bg-orange-50", text: "text-orange-700", ring: "ring-orange-200" },
  critical: { bg: "bg-red-50", text: "text-red-700", ring: "ring-red-200" },
};

const MONTH_LABELS: Record<string, string> = {
  "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May", "06": "Jun",
  "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
};
function monthLabel(mmYyyy: string): string {
  const [mm, yyyy] = mmYyyy.split("-");
  return `${MONTH_LABELS[mm] || mm} ${yyyy}`;
}

function getMonthOptions(apiMonths: string[]): string[] {
  if (apiMonths?.length) return apiMonths;
  const out: string[] = [];
  const now = new Date();
  for (let i = 0; i < 48; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const yyyy = d.getFullYear();
    out.push(`${mm}-${yyyy}`);
  }
  return out;
}

type TabId = "overview" | "charts";
type TimeFilter = "all" | "7d" | "1m" | "custom";

export default function DashboardPage() {
  const router = useRouter();
  const [data, setData] = useState<DashboardData | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [intel, setIntel] = useState<IntelligenceData | null>(null);
  const [report, setReport] = useState<PipelineReport | null>(null);
  const [reportExpanded, setReportExpanded] = useState(false);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<TabId>("overview");

  const [filterState, setFilterState] = useState<string>("");
  const [filterMarket, setFilterMarket] = useState<string>("");
  const [filterTime, setFilterTime] = useState<TimeFilter>("all");
  const [filterMonths, setFilterMonths] = useState<number>(6);
  const [filterMonthsMode, setFilterMonthsMode] = useState<"preset" | "custom">("preset");
  const [filterFromMonth, setFilterFromMonth] = useState<string>("");
  const [filterToMonth, setFilterToMonth] = useState<string>("");
  const [options, setOptions] = useState<{ states: string[]; markets: string[]; months: string[] }>({
    states: [],
    markets: [],
    months: [],
  });

  const [charts, setCharts] = useState<{
    heatmap: Record<string, unknown> | null;
    market_trend: Record<string, unknown> | null;
    retry_lift: Record<string, unknown> | null;
    stuck_tracker: Record<string, unknown> | null;
  }>({ heatmap: null, market_trend: null, retry_lift: null, stuck_tracker: null });
  const [chartsLoading, setChartsLoading] = useState(false);

  const loadData = useCallback(() => {
    setLoading(true);
    const useCustomRange = filterTime === "custom" && filterFromMonth && filterToMonth;
    const overviewParams =
      filterState || filterTime !== "all" || useCustomRange
        ? {
            state: filterState || undefined,
            time_filter: useCustomRange ? "all" : filterTime,
            from_month: useCustomRange ? filterFromMonth : undefined,
            to_month: useCustomRange ? filterToMonth : undefined,
          }
        : undefined;
    Promise.all([
      getDashboardOverview(overviewParams),
      getAlerts(),
      getIntelligence(),
      getLatestReport(),
    ])
      .then(([overview, alertData, intelligence, latestReport]) => {
        setData(overview);
        setAlerts(alertData?.alerts || []);
        setIntel(intelligence);
        setReport(latestReport);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [filterState, filterTime, filterFromMonth, filterToMonth]);

  const loadCharts = useCallback(() => {
    setChartsLoading(true);
    const useCustomRange = filterMonthsMode === "custom" && filterFromMonth && filterToMonth;
    const rosterCustom = filterTime === "custom" && filterFromMonth && filterToMonth;
    const rosterParams = {
      state: filterState || undefined,
      time_filter: rosterCustom ? "all" : filterTime,
      from_month: rosterCustom ? filterFromMonth : undefined,
      to_month: rosterCustom ? filterToMonth : undefined,
    };
    const metricsParams = {
      market: filterMarket || undefined,
      months: useCustomRange ? undefined : filterMonths,
      from_month: useCustomRange ? filterFromMonth : undefined,
      to_month: useCustomRange ? filterToMonth : undefined,
    };
    Promise.all([
      getDashboardChart("heatmap", rosterParams),
      getDashboardChart("market_trend", metricsParams),
      getDashboardChart("retry_lift", metricsParams),
      getDashboardChart("stuck_tracker", rosterParams),
    ])
      .then(([h, mt, rl, st]) => {
        setCharts({
          heatmap: h.chart || null,
          market_trend: mt.chart || null,
          retry_lift: rl.chart || null,
          stuck_tracker: st.chart || null,
        });
      })
      .catch(console.error)
      .finally(() => setChartsLoading(false));
  }, [filterState, filterMarket, filterTime, filterMonths, filterMonthsMode, filterFromMonth, filterToMonth]);

  useEffect(() => {
    getDashboardOptions().then(setOptions).catch(() => {});
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (activeTab === "charts") loadCharts();
  }, [activeTab, loadCharts]);

  const navigateToChat = (message: string) => {
    const encoded = encodeURIComponent(message);
    router.push(`/chat?prefill=${encoded}`);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gradient-to-br from-slate-50 via-white to-indigo-50/30">
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex flex-col items-center gap-4"
        >
          <div className="flex gap-2">
            <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" />
            <div className="w-2.5 h-2.5 bg-violet-500 rounded-full animate-bounce [animation-delay:0.15s]" />
            <div className="w-2.5 h-2.5 bg-indigo-400 rounded-full animate-bounce [animation-delay:0.3s]" />
          </div>
          <p className="text-slate-500 text-sm font-medium">Loading dashboard...</p>
        </motion.div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-full bg-gradient-to-br from-slate-50 via-white to-indigo-50/30">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="text-center space-y-3"
        >
          <XCircle className="w-12 h-12 text-slate-300 mx-auto" />
          <p className="text-slate-500">Failed to load dashboard. Is the backend running?</p>
        </motion.div>
      </div>
    );
  }

  const totalRedFlags = Object.values(data.red_health_flags).reduce((a, b) => a + b, 0);
  const hStatus = intel?.health_status || "healthy";
  const hColors = healthStatusColors[hStatus] || healthStatusColors.healthy;

  const kpis = [
    { label: "Total ROs", value: data.total_ros.toLocaleString(), icon: Layers, color: "text-indigo-600", bg: "bg-indigo-500/10", ring: "ring-indigo-200/50" },
    { label: "Stuck ROs", value: data.stuck_ros, icon: AlertTriangle, color: "text-amber-600", bg: "bg-amber-500/10", ring: "ring-amber-200/50" },
    { label: "Failed ROs", value: data.failed_ros.toLocaleString(), icon: XCircle, color: "text-red-500", bg: "bg-red-500/10", ring: "ring-red-200/50" },
    { label: "Red Health Flags", value: totalRedFlags.toLocaleString(), icon: ShieldAlert, color: "text-rose-500", bg: "bg-rose-500/10", ring: "ring-rose-200/50" },
  ];

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.4, ease: "easeOut" }}
      className="p-8 space-y-8 min-h-screen bg-gradient-to-br from-slate-50/80 via-white to-indigo-50/20"
    >
      {/* Header + Filters */}
      <div className="space-y-4">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-lg shadow-indigo-200/50">
              <LayoutDashboard className="w-6 h-6 text-white" />
            </div>
            <div>
              <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Pipeline Dashboard</h2>
              <p className="text-sm text-slate-500 mt-0.5">Overview of roster pipeline operations</p>
            </div>
          </div>
          <button
            onClick={() => loadData()}
            className="flex items-center gap-2 px-4 py-2 rounded-xl bg-indigo-50 text-indigo-600 text-sm font-medium hover:bg-indigo-100 transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </button>
        </div>

        {/* Interactive Filters */}
        <Card className="bg-white/80 border border-gray-100 p-4 rounded-2xl">
          <div className="flex flex-wrap items-center gap-4">
            {activeTab === "overview" && (
              <>
                <div className="flex items-center gap-2">
                  <MapPin className="w-4 h-4 text-gray-500" />
                  <select
                    value={filterState}
                    onChange={(e) => setFilterState(e.target.value)}
                    className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="">All states</option>
                    {options.states.map((s) => (
                      <option key={s} value={s}>{s}</option>
                    ))}
                  </select>
                </div>
                <div className="flex items-center gap-2">
                  <Clock className="w-4 h-4 text-gray-500" />
                  <select
                    value={filterTime}
                    onChange={(e) => setFilterTime(e.target.value as TimeFilter)}
                    className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="all">All time</option>
                    <option value="7d">Last 7 days</option>
                    <option value="1m">Last month</option>
                    <option value="custom">Custom range</option>
                  </select>
                </div>
                {filterTime === "custom" && (
                  <>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-500">From:</span>
                      <select
                        value={filterFromMonth}
                        onChange={(e) => setFilterFromMonth(e.target.value)}
                        className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                      >
                        <option value="">Select month</option>
                        {getMonthOptions(options.months || []).map((m) => (
                          <option key={m} value={m}>{monthLabel(m)}</option>
                        ))}
                      </select>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-500">To:</span>
                      <select
                        value={filterToMonth}
                        onChange={(e) => setFilterToMonth(e.target.value)}
                        className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                      >
                        <option value="">Select month</option>
                        {getMonthOptions(options.months || []).map((m) => (
                          <option key={m} value={m}>{monthLabel(m)}</option>
                        ))}
                      </select>
                    </div>
                  </>
                )}
              </>
            )}
            {activeTab === "charts" && (
              <>
                <div className="flex items-center gap-2">
                  <BarChart3 className="w-4 h-4 text-gray-500" />
                  <select
                    value={filterMarket}
                    onChange={(e) => setFilterMarket(e.target.value)}
                    className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="">All markets</option>
                    {options.markets.map((m) => (
                      <option key={m} value={m}>{m}</option>
                    ))}
                  </select>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-gray-500">Trend:</span>
                  <select
                    value={filterMonthsMode === "custom" ? "custom" : filterMonths}
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v === "custom") setFilterMonthsMode("custom");
                      else {
                        setFilterMonthsMode("preset");
                        setFilterMonths(Number(v));
                      }
                    }}
                    className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value={3}>3 months</option>
                    <option value={6}>6 months</option>
                    <option value={12}>12 months</option>
                    <option value={18}>18 months</option>
                    <option value={24}>24 months</option>
                    <option value={36}>36 months</option>
                    <option value="custom">Custom range</option>
                  </select>
                </div>
                {filterMonthsMode === "custom" && (
                  <>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-500">From:</span>
                      <select
                        value={filterFromMonth}
                        onChange={(e) => setFilterFromMonth(e.target.value)}
                        className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                      >
                        <option value="">Select month</option>
                        {getMonthOptions(options.months || []).map((m) => (
                          <option key={m} value={m}>{monthLabel(m)}</option>
                        ))}
                      </select>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-500">To:</span>
                      <select
                        value={filterToMonth}
                        onChange={(e) => setFilterToMonth(e.target.value)}
                        className="px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                      >
                        <option value="">Select month</option>
                        {getMonthOptions(options.months || []).map((m) => (
                          <option key={m} value={m}>{monthLabel(m)}</option>
                        ))}
                      </select>
                    </div>
                  </>
                )}
              </>
            )}
            <div className="flex gap-2 ml-auto">
              <button
                onClick={() => setActiveTab("overview")}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors ${
                  activeTab === "overview" ? "bg-indigo-600 text-white" : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
              >
                Overview
              </button>
              <button
                onClick={() => setActiveTab("charts")}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors ${
                  activeTab === "charts" ? "bg-indigo-600 text-white" : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
              >
                Charts
              </button>
            </div>
          </div>
        </Card>
      </div>

      {/* Overview Tab */}
      {activeTab === "overview" && (
        <>
      {/* Pipeline Health Banner */}
      {intel && (
        <Card className={`${hColors.bg} border-0 ring-1 ${hColors.ring} p-5 rounded-2xl shadow-sm`}>
          <div className="flex items-center gap-3">
            <Activity className={`w-5 h-5 ${hColors.text}`} />
            <div>
              <p className={`text-sm font-bold ${hColors.text}`}>
                Pipeline Status: {hStatus.toUpperCase()}
              </p>
              <p className="text-sm text-gray-600 mt-0.5">{intel.pipeline_health_summary}</p>
            </div>
          </div>
        </Card>
      )}

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-5">
        {kpis.map((kpi) => {
          const Icon = kpi.icon;
          return (
            <Card key={kpi.label} className={`bg-white border-0 ring-1 p-6 rounded-2xl shadow-sm ${kpi.ring}`}>
              <div className="flex items-center justify-between mb-4">
                <p className="text-sm font-medium text-gray-500">{kpi.label}</p>
                <div className={`w-10 h-10 rounded-xl ${kpi.bg} flex items-center justify-center`}>
                  <Icon className={`w-5 h-5 ${kpi.color}`} />
                </div>
              </div>
              <p className={`text-3xl font-bold ${kpi.color}`}>{kpi.value}</p>
            </Card>
          );
        })}
      </div>

      {/* Alerts with Actions */}
      {alerts.length > 0 && (
        <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
          <div className="flex items-center gap-2 mb-4">
            <Bell className="w-4 h-4 text-indigo-500" />
            <h3 className="text-sm font-bold text-gray-800">Proactive Alerts</h3>
            <Badge className="text-xs bg-red-50 text-red-600 border-red-100">{alerts.length}</Badge>
          </div>
          <div className="space-y-2.5">
            {alerts.map((alert, i) => (
              <div
                key={i}
                className="flex items-start gap-3 p-4 rounded-xl bg-gray-50/80 border border-gray-100"
              >
                <Badge className={severityStyles[alert.severity] || severityStyles.info}>
                  {alert.severity}
                </Badge>
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-gray-700 leading-relaxed">{alert.message}</p>
                  <p className="text-xs text-gray-400 mt-1">{alert.type}</p>
                </div>
                {alert.recommended_action && (
                  <button
                    onClick={() => {
                      const paramsStr = alert.recommended_params
                        ? Object.entries(alert.recommended_params)
                            .map(([k, v]) => `${k}=${v}`)
                            .join(", ")
                        : "";
                      const msg = paramsStr
                        ? `Run ${alert.recommended_action} with ${paramsStr}`
                        : `Run ${alert.recommended_action}`;
                      navigateToChat(msg);
                    }}
                    className="shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-indigo-50 text-indigo-600 text-xs font-semibold hover:bg-indigo-100 transition-colors"
                  >
                    <Play className="w-3 h-3" />
                    Run Action
                  </button>
                )}
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Intelligence Panels */}
      {intel && (
        <div className="grid grid-cols-2 gap-6">
          {/* Root Cause Insights */}
          <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Brain className="w-4 h-4 text-purple-500" />
              <h3 className="text-sm font-bold text-gray-800">Root Cause Insights</h3>
            </div>
            <div className="space-y-3 max-h-96 overflow-y-auto pr-1">
              {intel.root_cause_insights.length === 0 && (
                <p className="text-sm text-gray-400">No significant issues detected.</p>
              )}
              {intel.root_cause_insights.map((rc, i) => (
                <div key={i} className="p-3.5 rounded-xl bg-gray-50/80 border border-gray-100 space-y-1.5">
                  <div className="flex items-start gap-2">
                    <Badge className={severityStyles[rc.severity] || severityStyles.info}>
                      {rc.severity}
                    </Badge>
                    <p className="text-sm font-medium text-gray-800">{rc.issue}</p>
                  </div>
                  <p className="text-xs text-gray-500 leading-relaxed pl-1">{rc.explanation}</p>
                </div>
              ))}
            </div>
          </Card>

          {/* Recommended Actions */}
          <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Target className="w-4 h-4 text-emerald-500" />
              <h3 className="text-sm font-bold text-gray-800">Recommended Actions</h3>
            </div>
            <div className="space-y-2.5">
              {intel.recommended_actions.map((ra, i) => (
                <div key={i} className="flex items-center gap-3 p-3.5 rounded-xl bg-gray-50/80 border border-gray-100">
                  <div className="w-7 h-7 rounded-lg bg-indigo-50 flex items-center justify-center shrink-0">
                    <span className="text-xs font-bold text-indigo-500">#{ra.priority}</span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-700">{ra.action}</p>
                    <p className="text-xs text-gray-400 mt-0.5 font-mono">{ra.procedure}</p>
                  </div>
                  <button
                    onClick={() => {
                      const paramsStr = Object.entries(ra.params)
                        .map(([k, v]) => `${k}=${v}`)
                        .join(", ");
                      const msg = paramsStr
                        ? `Run ${ra.procedure} with ${paramsStr}`
                        : `Run ${ra.procedure}`;
                      navigateToChat(msg);
                    }}
                    className="shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-50 text-emerald-600 text-xs font-semibold hover:bg-emerald-100 transition-colors"
                  >
                    <Play className="w-3 h-3" />
                    Run
                  </button>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}

      {/* Retry Effectiveness + Procedure Effectiveness */}
      {intel && (
        <div className="grid grid-cols-2 gap-6">
          {/* Retry Effectiveness */}
          <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <RefreshCw className="w-4 h-4 text-blue-500" />
              <h3 className="text-sm font-bold text-gray-800">Retry Effectiveness</h3>
            </div>
            <div className="grid grid-cols-3 gap-4">
              <div className="text-center p-3 rounded-xl bg-blue-50/60">
                <p className="text-2xl font-bold text-blue-600">
                  {intel.retry_effectiveness.total_retries.toLocaleString()}
                </p>
                <p className="text-xs text-gray-500 mt-1">Total Retries</p>
              </div>
              <div className="text-center p-3 rounded-xl bg-emerald-50/60">
                <p className="text-2xl font-bold text-emerald-600">
                  {intel.retry_effectiveness.success_rate}%
                </p>
                <p className="text-xs text-gray-500 mt-1">Success Rate</p>
              </div>
              <div className="text-center p-3 rounded-xl bg-red-50/60">
                <p className="text-2xl font-bold text-red-500">
                  {(intel.retry_effectiveness.retry_failures ?? 0).toLocaleString()}
                </p>
                <p className="text-xs text-gray-500 mt-1">Still Failed</p>
              </div>
            </div>
            {intel.retry_effectiveness.success_rate < 50 && (
              <div className="mt-3 flex items-center gap-2 p-2.5 rounded-lg bg-amber-50 border border-amber-100">
                <TrendingDown className="w-3.5 h-3.5 text-amber-500 shrink-0" />
                <p className="text-xs text-amber-700">
                  Low retry success rate — retries may not be effective for current failure patterns.
                </p>
              </div>
            )}
          </Card>

          {/* Procedure Effectiveness */}
          <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Lightbulb className="w-4 h-4 text-amber-500" />
              <h3 className="text-sm font-bold text-gray-800">Procedure Effectiveness</h3>
            </div>
            <div className="space-y-3">
              {Object.entries(intel.procedure_effectiveness).map(([name, eff]) => (
                <div key={name} className="flex items-center gap-3 p-3 rounded-xl bg-gray-50/80 border border-gray-100">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-700 font-mono">{name}</p>
                    <p className="text-xs text-gray-400 mt-0.5">
                      {eff.total_runs} run{eff.total_runs !== 1 ? "s" : ""}
                      {eff.last_run ? ` · Last: ${new Date(eff.last_run).toLocaleDateString()}` : ""}
                    </p>
                  </div>
                  <div className="text-right shrink-0">
                    {eff.total_runs > 0 ? (
                      <span className={`text-sm font-bold ${
                        (eff.resolved_rate ?? 0) >= 50 ? "text-emerald-600" : "text-amber-600"
                      }`}>
                        {eff.resolved_rate ?? 0}% resolved
                      </span>
                    ) : (
                      <span className="text-xs text-gray-400">No runs yet</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}

      {/* Operational Report */}
      {report && (
        <Card className="bg-white border-0 ring-1 ring-gray-100 rounded-2xl shadow-sm overflow-hidden">
          <button
            onClick={() => setReportExpanded(!reportExpanded)}
            className="w-full flex items-center gap-3 p-6 text-left hover:bg-gray-50/50 transition-colors cursor-pointer"
          >
            {reportExpanded ? (
              <ChevronDown className="w-4 h-4 text-gray-400 shrink-0" />
            ) : (
              <ChevronRight className="w-4 h-4 text-gray-400 shrink-0" />
            )}
            <FileText className="w-4 h-4 text-indigo-500" />
            <h3 className="text-sm font-bold text-gray-800">
              Pipeline Health Report
            </h3>
            <Badge className={`text-xs ml-2 ${
              report.health_rating === "CRITICAL" ? "bg-red-50 text-red-600 border-red-100" :
              report.health_rating === "DEGRADED" ? "bg-orange-50 text-orange-600 border-orange-100" :
              report.health_rating === "WARNING" ? "bg-amber-50 text-amber-600 border-amber-100" :
              "bg-emerald-50 text-emerald-600 border-emerald-100"
            }`}>
              {report.health_rating}
            </Badge>
            <span className="ml-auto text-xs text-gray-400 font-normal">
              {report.filter} · {report.flagged_count} flagged ROs
            </span>
          </button>

          {reportExpanded && (
            <div className="px-6 pb-6 space-y-5 border-t border-gray-100 pt-5">
              {/* Summary Stats */}
              <div className="grid grid-cols-5 gap-3">
                {[
                  { label: "Total ROs", value: (report.summary_statistics.total_ros ?? 0).toLocaleString(), color: "text-indigo-600" },
                  { label: "Failed", value: `${(report.summary_statistics.failed_ros ?? 0).toLocaleString()} (${report.summary_statistics.failure_rate ?? 0}%)`, color: "text-red-500" },
                  { label: "Stuck", value: `${(report.summary_statistics.stuck_ros ?? 0).toLocaleString()} (${report.summary_statistics.stuck_rate ?? 0}%)`, color: "text-amber-600" },
                  { label: "Critical", value: (report.summary_statistics.critical_count ?? 0).toLocaleString(), color: "text-rose-600" },
                  { label: "Avg Health", value: String(report.summary_statistics.avg_health_score ?? "N/A"), color: "text-emerald-600" },
                ].map((s) => (
                  <div key={s.label} className="text-center p-3 rounded-xl bg-gray-50/80 border border-gray-100">
                    <p className={`text-lg font-bold ${s.color}`}>{s.value}</p>
                    <p className="text-xs text-gray-400 mt-0.5">{s.label}</p>
                  </div>
                ))}
              </div>

              {/* Stage Bottlenecks */}
              {report.stage_bottlenecks.length > 0 && (
                <div>
                  <h4 className="text-xs font-bold text-gray-700 mb-2 uppercase tracking-wider">Stage Bottlenecks</h4>
                  <div className="space-y-2">
                    {report.stage_bottlenecks.slice(0, 6).map((b, i) => (
                      <div key={i} className="flex items-start gap-3 p-3 rounded-xl bg-gray-50/80 border border-gray-100">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-sm font-semibold text-gray-800">{b.stage}</span>
                            <span className="text-xs text-gray-400">{b.total} ROs</span>
                          </div>
                          <div className="flex gap-3 text-xs text-gray-500 mb-1">
                            <span className="text-red-500 font-medium">{b.stuck} stuck</span>
                            <span className="text-amber-500 font-medium">{b.failed} failed</span>
                            <span>{b.avg_red_flags} avg RED flags</span>
                          </div>
                          <p className="text-xs text-gray-400">{b.interpretation}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Failure Breakdown */}
              {report.failure_breakdown.length > 0 && (
                <div>
                  <h4 className="text-xs font-bold text-gray-700 mb-2 uppercase tracking-wider">Failure Categories</h4>
                  <div className="space-y-2">
                    {report.failure_breakdown.map((fb, i) => (
                      <div key={i} className="flex items-start gap-3 p-3 rounded-xl bg-gray-50/80 border border-gray-100">
                        <Badge className="bg-red-50 text-red-600 border-red-100 shrink-0">
                          {fb.count}
                        </Badge>
                        <div>
                          <p className="text-sm font-medium text-gray-700">{fb.category}</p>
                          <p className="text-xs text-gray-400 mt-0.5">{fb.explanation}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Report Recommended Actions */}
              {report.recommended_actions.length > 0 && (
                <div>
                  <h4 className="text-xs font-bold text-gray-700 mb-2 uppercase tracking-wider">Report Recommendations</h4>
                  <div className="space-y-2">
                    {report.recommended_actions.map((ra, i) => (
                      <div key={i} className="flex items-center gap-3 p-3 rounded-xl bg-gray-50/80 border border-gray-100">
                        <div className="w-6 h-6 rounded-lg bg-indigo-50 flex items-center justify-center shrink-0">
                          <span className="text-xs font-bold text-indigo-500">#{ra.priority}</span>
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm text-gray-700">{ra.action}</p>
                          {ra.reason && <p className="text-xs text-gray-400 mt-0.5">{ra.reason}</p>}
                        </div>
                        {ra.procedure && (
                          <button
                            onClick={() => {
                              const paramsStr = Object.entries(ra.params)
                                .map(([k, v]) => `${k}=${v}`)
                                .join(", ");
                              const msg = paramsStr
                                ? `Run ${ra.procedure} with ${paramsStr}`
                                : `Run ${ra.procedure}`;
                              navigateToChat(msg);
                            }}
                            className="shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-50 text-emerald-600 text-xs font-semibold hover:bg-emerald-100 transition-colors"
                          >
                            <Play className="w-3 h-3" />
                            Run
                          </button>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Market Context */}
              {Object.keys(report.market_context).length > 0 && (
                <div>
                  <h4 className="text-xs font-bold text-gray-700 mb-2 uppercase tracking-wider">Market Context</h4>
                  <div className="grid grid-cols-3 gap-3">
                    {Object.entries(report.market_context).map(([mkt, ctx]) => (
                      <div key={mkt} className="p-3 rounded-xl bg-gray-50/80 border border-gray-100 text-center">
                        <p className="text-lg font-bold text-gray-800">{mkt}</p>
                        <p className={`text-xl font-bold mt-1 ${
                          ctx.latest_scs >= 95 ? "text-emerald-600" :
                          ctx.latest_scs >= 85 ? "text-amber-600" : "text-red-500"
                        }`}>
                          {ctx.latest_scs}%
                        </p>
                        <p className="text-xs text-gray-400 mt-0.5">SCS · {ctx.latest_month}</p>
                        {ctx.latest_retry_lift != null && (
                          <p className={`text-xs mt-1 ${ctx.latest_retry_lift >= 0 ? "text-emerald-500" : "text-red-500"}`}>
                            Retry lift: {ctx.latest_retry_lift > 0 ? "+" : ""}{ctx.latest_retry_lift}%
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </Card>
      )}

      <div className="grid grid-cols-2 gap-6">
        {/* Market Summary */}
        <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
          <h3 className="text-sm font-bold text-gray-800 mb-4">
            Market Success Rates
            <span className="text-gray-400 font-normal ml-2">({data.latest_month})</span>
          </h3>
          <div className="space-y-3 max-h-96 overflow-y-auto pr-2">
            {data.market_summary.map((m, i) => (
              <div key={i} className="flex items-center justify-between py-1.5">
                <span className="text-sm font-medium text-gray-700">{m.MARKET}</span>
                <div className="flex items-center gap-3">
                  <div className="w-36 bg-gray-100 rounded-full h-2.5">
                    <div
                      className={`h-2.5 rounded-full transition-all ${
                        m.SCS_PERCENT >= 95
                          ? "bg-emerald-400"
                          : m.SCS_PERCENT >= 85
                          ? "bg-amber-400"
                          : "bg-red-400"
                      }`}
                      style={{ width: `${Math.min(m.SCS_PERCENT, 100)}%` }}
                    />
                  </div>
                  <span
                    className={`text-sm font-mono font-semibold w-16 text-right ${
                      m.SCS_PERCENT >= 95
                        ? "text-emerald-600"
                        : m.SCS_PERCENT >= 85
                        ? "text-amber-600"
                        : "text-red-500"
                    }`}
                  >
                    {m.SCS_PERCENT}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </Card>

        {/* Red Health Flags by Stage */}
        <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
          <h3 className="text-sm font-bold text-gray-800 mb-4">
            Red Health Flags by Stage
          </h3>
          <div className="space-y-4">
            {Object.entries(data.red_health_flags)
              .sort(([, a], [, b]) => b - a)
              .map(([stage, count]) => {
                const label = stage
                  .replace("_HEALTH", "")
                  .replace(/_/g, " ")
                  .replace(/\b\w/g, (c) => c.toUpperCase());
                const pct = data.total_ros > 0 ? (count / data.total_ros) * 100 : 0;
                return (
                  <div key={stage}>
                    <div className="flex justify-between text-sm mb-1.5">
                      <span className="text-gray-600 font-medium">{label}</span>
                      <span className="text-red-500 font-mono font-semibold">{count.toLocaleString()}</span>
                    </div>
                    <div className="w-full bg-gray-100 rounded-full h-2">
                      <div
                        className="bg-red-400 h-2 rounded-full transition-all"
                        style={{ width: `${Math.min(pct * 5, 100)}%` }}
                      />
                    </div>
                  </div>
                );
              })}
          </div>
        </Card>
      </div>
        </>
      )}

      {/* Charts Tab */}
      {activeTab === "charts" && (
        <div className="space-y-6">
          {chartsLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="flex gap-2">
                <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" />
                <div className="w-2.5 h-2.5 bg-violet-500 rounded-full animate-bounce [animation-delay:0.15s]" />
                <div className="w-2.5 h-2.5 bg-indigo-400 rounded-full animate-bounce [animation-delay:0.3s]" />
              </div>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-2 gap-6">
                <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <h3 className="text-sm font-bold text-gray-800 mb-4">Pipeline Stage Health Heatmap</h3>
                  <p className="text-xs text-gray-500 mb-3">
                    {filterState ? `State: ${filterState}` : "All states"} · {filterTime === "7d" ? "Last 7 days" : filterTime === "1m" ? "Last month" : filterTime === "custom" && filterFromMonth && filterToMonth ? `${monthLabel(filterFromMonth)} – ${monthLabel(filterToMonth)}` : "All time"}
                  </p>
                  <div className="h-[400px]">
                    {charts.heatmap ? <PlotlyChart data={charts.heatmap} /> : <p className="text-sm text-gray-400">No data</p>}
                  </div>
                </Card>
                <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <h3 className="text-sm font-bold text-gray-800 mb-4">Market SCS% Trend</h3>
                  <p className="text-xs text-gray-500 mb-3">
                    {filterMarket ? `Market: ${filterMarket}` : "All markets"} · {filterMonthsMode === "custom" && filterFromMonth && filterToMonth ? `${monthLabel(filterFromMonth)} – ${monthLabel(filterToMonth)}` : `Last ${filterMonths} months`}
                  </p>
                  <div className="h-[400px]">
                    {charts.market_trend ? <PlotlyChart data={charts.market_trend} /> : <p className="text-sm text-gray-400">No data</p>}
                  </div>
                </Card>
              </div>
              <div className="grid grid-cols-2 gap-6">
                <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <h3 className="text-sm font-bold text-gray-800 mb-4">Retry Lift by Market</h3>
                  <p className="text-xs text-gray-500 mb-3">
                    {filterMarket ? `Market: ${filterMarket}` : "All markets"} · {filterMonthsMode === "custom" && filterFromMonth && filterToMonth ? `${monthLabel(filterFromMonth)} – ${monthLabel(filterToMonth)}` : `Last ${filterMonths} months`}
                  </p>
                  <div className="h-[400px]">
                    {charts.retry_lift ? <PlotlyChart data={charts.retry_lift} /> : <p className="text-sm text-gray-400">No data</p>}
                  </div>
                </Card>
                <Card className="bg-white border-0 ring-1 ring-gray-100 p-6 rounded-2xl shadow-sm">
                  <h3 className="text-sm font-bold text-gray-800 mb-4">Stuck RO Tracker</h3>
                  <p className="text-xs text-gray-500 mb-3">
                    {filterState ? `State: ${filterState}` : "All states"} · {filterTime === "7d" ? "Last 7 days" : filterTime === "1m" ? "Last month" : filterTime === "custom" && filterFromMonth && filterToMonth ? `${monthLabel(filterFromMonth)} – ${monthLabel(filterToMonth)}` : "All time"}
                  </p>
                  <div className="h-[400px]">
                    {charts.stuck_tracker ? <PlotlyChart data={charts.stuck_tracker} /> : <p className="text-sm text-gray-400">No stuck ROs</p>}
                  </div>
                </Card>
              </div>
              <div className="flex items-center justify-between">
                <button
                  onClick={() => navigateToChat("Show me pipeline health heatmap and stuck ROs")}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl bg-indigo-50 text-indigo-600 text-sm font-medium hover:bg-indigo-100 transition-colors"
                >
                  <Play className="w-4 h-4" />
                  Ask in Chat
                </button>
                <button
                  onClick={loadCharts}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl bg-gray-100 text-gray-700 text-sm font-medium hover:bg-gray-200 transition-colors"
                >
                  <RefreshCw className="w-4 h-4" />
                  Reload charts
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </motion.div>
  );
}

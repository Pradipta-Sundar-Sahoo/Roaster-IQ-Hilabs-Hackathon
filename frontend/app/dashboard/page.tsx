"use client";

import { useEffect, useState } from "react";
import { getDashboardOverview, getAlerts, type Alert } from "@/lib/api";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { LayoutDashboard, Layers, AlertTriangle, XCircle, ShieldAlert, Bell } from "lucide-react";

interface DashboardData {
  total_ros: number;
  stuck_ros: number;
  failed_ros: number;
  red_health_flags: Record<string, number>;
  latest_month: string;
  market_summary: { MARKET: string; SCS_PERCENT: number }[];
}

export default function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([getDashboardOverview(), getAlerts()])
      .then(([overview, alertData]) => {
        setData(overview);
        setAlerts(alertData.alerts || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-[#f8f9fb]">
        <div className="flex flex-col items-center gap-3">
          <div className="flex gap-1.5">
            <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full animate-bounce" />
            <div className="w-2.5 h-2.5 bg-indigo-400 rounded-full animate-bounce [animation-delay:0.15s]" />
            <div className="w-2.5 h-2.5 bg-indigo-300 rounded-full animate-bounce [animation-delay:0.3s]" />
          </div>
          <p className="text-gray-400 text-sm">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-full bg-[#f8f9fb]">
        <div className="text-center space-y-2">
          <XCircle className="w-10 h-10 text-gray-300 mx-auto" />
          <p className="text-gray-400">Failed to load dashboard. Is the backend running?</p>
        </div>
      </div>
    );
  }

  const totalRedFlags = Object.values(data.red_health_flags).reduce((a, b) => a + b, 0);

  const kpis = [
    { label: "Total ROs", value: data.total_ros.toLocaleString(), icon: Layers, color: "text-indigo-600", bg: "bg-indigo-50", ring: "ring-indigo-100" },
    { label: "Stuck ROs", value: data.stuck_ros, icon: AlertTriangle, color: "text-amber-600", bg: "bg-amber-50", ring: "ring-amber-100" },
    { label: "Failed ROs", value: data.failed_ros.toLocaleString(), icon: XCircle, color: "text-red-500", bg: "bg-red-50", ring: "ring-red-100" },
    { label: "Red Health Flags", value: totalRedFlags.toLocaleString(), icon: ShieldAlert, color: "text-rose-500", bg: "bg-rose-50", ring: "ring-rose-100" },
  ];

  return (
    <div className="p-8 space-y-8 bg-[#f8f9fb] min-h-screen">
      {/* Header */}
      <div className="flex items-center gap-3">
        <LayoutDashboard className="w-5 h-5 text-indigo-500" />
        <div>
          <h2 className="text-xl font-bold text-gray-900">Pipeline Dashboard</h2>
          <p className="text-sm text-gray-400">Overview of roster pipeline operations</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-5">
        {kpis.map((kpi) => {
          const Icon = kpi.icon;
          return (
            <Card key={kpi.label} className="bg-white border-0 ring-1 p-6 rounded-2xl shadow-sm ${kpi.ring}">
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

      {/* Alerts */}
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
                <Badge
                  className={
                    alert.severity === "high"
                      ? "bg-red-50 text-red-600 border-red-100"
                      : alert.severity === "medium"
                      ? "bg-amber-50 text-amber-600 border-amber-100"
                      : "bg-gray-50 text-gray-500 border-gray-200"
                  }
                >
                  {alert.severity}
                </Badge>
                <div>
                  <p className="text-sm text-gray-700 leading-relaxed">{alert.message}</p>
                  <p className="text-xs text-gray-400 mt-1">{alert.type}</p>
                </div>
              </div>
            ))}
          </div>
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
    </div>
  );
}

"use client";

import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

interface PlotlyChartProps {
  data: Record<string, unknown>;
}

export function PlotlyChart({ data }: PlotlyChartProps) {
  if (!data) return null;

  const plotData = (data as { data?: Record<string, unknown>[] }).data || [];
  const layout = {
    ...((data as { layout?: Record<string, unknown> }).layout || {}),
    paper_bgcolor: "rgba(255,255,255,0)",
    plot_bgcolor: "rgba(248,249,251,0.6)",
    font: { color: "#4b5563", size: 12, family: "Inter, system-ui, sans-serif" },
    margin: { t: 40, r: 20, b: 60, l: 60 },
    xaxis: {
      ...((data as { layout?: { xaxis?: Record<string, unknown> } }).layout?.xaxis || {}),
      gridcolor: "rgba(229,231,235,0.8)",
      linecolor: "rgba(229,231,235,0.8)",
      zerolinecolor: "rgba(229,231,235,0.8)",
    },
    yaxis: {
      ...((data as { layout?: { yaxis?: Record<string, unknown> } }).layout?.yaxis || {}),
      gridcolor: "rgba(229,231,235,0.8)",
      linecolor: "rgba(229,231,235,0.8)",
      zerolinecolor: "rgba(229,231,235,0.8)",
    },
  };

  return (
    <div className="bg-white rounded-2xl border border-gray-100 p-3 shadow-sm">
      <Plot
        data={plotData as Plotly.Data[]}
        layout={layout as Partial<Plotly.Layout>}
        config={{ responsive: true, displayModeBar: false }}
        style={{ width: "100%", height: "100%" }}
      />
    </div>
  );
}

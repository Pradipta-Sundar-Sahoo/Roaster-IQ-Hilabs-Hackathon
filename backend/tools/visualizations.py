"""Visualization tools — Plotly chart generators returned as JSON for frontend rendering."""

import json
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np


def _fig_to_json(fig) -> dict:
    """Convert Plotly figure to JSON-serializable dict."""
    return json.loads(fig.to_json())


def create_health_heatmap(df: pd.DataFrame) -> dict:
    """Pipeline stage health heatmap — orgs × stages, colored by health flag."""
    if df.empty:
        return None

    health_cols = [
        "PRE_PROCESSING_HEALTH", "MAPPING_APROVAL_HEALTH", "ISF_GEN_HEALTH",
        "DART_GEN_HEALTH", "DART_REVIEW_HEALTH", "DART_UI_VALIDATION_HEALTH", "SPS_LOAD_HEALTH"
    ]
    stage_labels = [
        "Pre-Processing", "Mapping Approval", "ISF Generation",
        "DART Generation", "DART Review", "DART UI Validation", "SPS Load"
    ]

    # Map health flags to numeric: Green=0, Yellow=1, Red=2
    health_map = {"GREEN": 0, "YELLOW": 1, "RED": 2}

    # Aggregate by org — take the worst health flag
    agg_df = df.groupby("ORG_NM")[health_cols].agg(
        lambda x: x.map(health_map).max() if not x.isna().all() else -1
    ).head(25)

    # Truncate org names for readability
    agg_df.index = [name[:40] + "..." if len(name) > 40 else name for name in agg_df.index]

    fig = go.Figure(data=go.Heatmap(
        z=agg_df.values,
        x=stage_labels,
        y=agg_df.index,
        colorscale=[
            [0, "#2ecc71"],      # Green
            [0.5, "#f39c12"],    # Yellow
            [1, "#e74c3c"],      # Red
        ],
        zmin=0, zmax=2,
        text=agg_df.values,
        texttemplate="%{text}",
        hovertemplate="Org: %{y}<br>Stage: %{x}<br>Health: %{z}<extra></extra>",
        colorbar=dict(
            title="Health",
            tickvals=[0, 1, 2],
            ticktext=["Green", "Yellow", "Red"],
        ),
    ))
    fig.update_layout(
        title="Pipeline Stage Health Heatmap",
        xaxis_title="Pipeline Stage",
        yaxis_title="Organization",
        height=max(400, len(agg_df) * 25),
        margin=dict(l=250),
    )
    return _fig_to_json(fig)


def create_failure_breakdown(stats_df: pd.DataFrame, failure_df: pd.DataFrame = None) -> dict:
    """Failure status breakdown — stacked bar per state."""
    if stats_df.empty:
        return None

    fig = go.Figure()

    if failure_df is not None and not failure_df.empty:
        # Stacked bar by failure type
        for _, row in failure_df.iterrows():
            status = row.get("FAILURE_STATUS", "Unknown") or "Unknown"
            fig.add_trace(go.Bar(
                name=str(status)[:30],
                x=[str(status)[:30]],
                y=[row["cnt"]],
            ))
        fig.update_layout(title="Failure Status Distribution", barmode="stack")
    else:
        # Bar chart of failure rates by state
        fig = px.bar(
            stats_df.head(20),
            x="CNT_STATE",
            y="failure_rate",
            title="File Failure Rate by State (%)",
            labels={"CNT_STATE": "State", "failure_rate": "Failure Rate (%)"},
            color="failure_rate",
            color_continuous_scale="RdYlGn_r",
        )

    fig.update_layout(
        height=450,
        xaxis_title="State / Failure Type",
        yaxis_title="Count / Rate",
    )
    return _fig_to_json(fig)


def create_duration_anomaly(df: pd.DataFrame) -> dict:
    """Duration anomaly chart — actual vs average duration per stage with outlier highlighting."""
    if df.empty:
        return None

    # Focus on DART generation as it has the most data
    stage_pairs = [
        ("DART_GEN_DURATION", "AVG_DART_GEN_DURATION", "DART Generation"),
        ("ISF_GEN_DURATION", "AVG_ISF_GEN_DURATION", "ISF Generation"),
        ("SPS_LOAD_DURATION", "AVG_SPS_LOAD_DURATION", "SPS Load"),
    ]

    fig = go.Figure()

    for actual_col, avg_col, label in stage_pairs:
        if actual_col in df.columns and avg_col in df.columns:
            valid = df[[actual_col, avg_col]].dropna()
            if valid.empty:
                continue

            is_anomaly = valid[actual_col] > 2 * valid[avg_col]

            fig.add_trace(go.Scatter(
                x=valid[avg_col],
                y=valid[actual_col],
                mode="markers",
                name=f"{label} (normal)",
                marker=dict(
                    size=6,
                    color="green",
                    opacity=0.5,
                ),
                text=df.loc[valid.index, "ORG_NM"] if "ORG_NM" in df.columns else None,
                hovertemplate=f"{label}<br>Avg: %{{x:.1f}} min<br>Actual: %{{y:.1f}} min<br>%{{text}}<extra></extra>",
            ))

            anomalies = valid[is_anomaly]
            if not anomalies.empty:
                fig.add_trace(go.Scatter(
                    x=anomalies[avg_col],
                    y=anomalies[actual_col],
                    mode="markers",
                    name=f"{label} (anomaly)",
                    marker=dict(
                        size=10,
                        color="red",
                        symbol="diamond",
                    ),
                    text=df.loc[anomalies.index, "ORG_NM"] if "ORG_NM" in df.columns else None,
                ))

    # Add reference line (y = 2x)
    max_val = 100
    fig.add_trace(go.Scatter(
        x=[0, max_val],
        y=[0, 2 * max_val],
        mode="lines",
        name="2x threshold",
        line=dict(dash="dash", color="orange"),
    ))

    fig.update_layout(
        title="Stage Duration Anomalies (Actual vs Historical Average)",
        xaxis_title="Historical Average Duration (min)",
        yaxis_title="Actual Duration (min)",
        height=500,
    )
    return _fig_to_json(fig)


def create_market_trend(df: pd.DataFrame, market: str = None) -> dict:
    """Market SCS% trend — monthly success rate by market with 95% threshold."""
    if df.empty:
        return None

    if market:
        df = df[df["MARKET"] == market] if "MARKET" in df.columns else df

    fig = go.Figure()

    if "MARKET" in df.columns:
        markets = df["MARKET"].unique()
        for m in markets[:10]:  # Limit to 10 markets
            m_data = df[df["MARKET"] == m]
            fig.add_trace(go.Scatter(
                x=m_data["MONTH"],
                y=m_data["SCS_PERCENT"],
                mode="lines+markers",
                name=str(m),
                hovertemplate=f"Market: {m}<br>Month: %{{x}}<br>SCS%%: %{{y:.2f}}%<extra></extra>",
            ))
    else:
        fig.add_trace(go.Scatter(
            x=df["MONTH"],
            y=df["SCS_PERCENT"],
            mode="lines+markers",
            name="SCS %",
        ))

    # 95% threshold line
    fig.add_hline(
        y=95,
        line_dash="dash",
        line_color="red",
        annotation_text="95% Threshold",
    )

    fig.update_layout(
        title=f"Market Transaction Success Rate Trend{' — ' + market if market else ''}",
        xaxis_title="Month",
        yaxis_title="Success Rate (%)",
        height=450,
        yaxis=dict(range=[min(df["SCS_PERCENT"].min() - 2, 90), 100]),
    )
    return _fig_to_json(fig)


def create_retry_lift(df: pd.DataFrame) -> dict:
    """Retry lift chart — first-iteration vs overall success counts by market."""
    if df.empty:
        return None

    # Get latest month per market
    latest = df.sort_values("MONTH", ascending=False).groupby("MARKET").first().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=latest["MARKET"],
        y=latest["FIRST_ITER_SCS_CNT"],
        name="First Iteration Success",
        marker_color="#3498db",
    ))
    fig.add_trace(go.Bar(
        x=latest["MARKET"],
        y=latest["NEXT_ITER_SCS_CNT"] - latest["FIRST_ITER_SCS_CNT"],
        name="Retry Recovery",
        marker_color="#2ecc71",
    ))

    fig.update_layout(
        title="Retry Effectiveness by Market — First Pass vs Recovery",
        xaxis_title="Market",
        yaxis_title="Transaction Count",
        barmode="stack",
        height=450,
    )
    return _fig_to_json(fig)


def create_stuck_tracker(df: pd.DataFrame) -> dict:
    """Stuck RO tracker — ROs plotted by DAYS_STUCK, colored by PRIORITY. Uses UPPERCASE column names."""
    if df.empty:
        return None

    days_col = "DAYS_STUCK" if "DAYS_STUCK" in df.columns else "days_stuck"
    red_col = "RED_COUNT" if "RED_COUNT" in df.columns else "red_count"
    pri_col = "PRIORITY" if "PRIORITY" in df.columns else "priority"

    priority_colors = {
        "CRITICAL": "#e74c3c",
        "HIGH": "#e67e22",
        "MEDIUM": "#f1c40f",
        "LOW": "#2ecc71",
    }

    fig = go.Figure()

    for priority, color in priority_colors.items():
        p_data = df[df[pri_col] == priority] if pri_col in df.columns else pd.DataFrame()
        if not p_data.empty:
            fig.add_trace(go.Scatter(
                x=p_data["ORG_NM"].apply(lambda x: x[:30] + "..." if len(str(x)) > 30 else x),
                y=p_data[days_col],
                mode="markers",
                name=f"{priority.title()} Priority",
                marker=dict(
                    size=p_data.get(red_col, pd.Series([5]*len(p_data))) * 5 + 8,
                    color=color,
                    line=dict(width=1, color="black"),
                ),
                text=p_data.apply(
                    lambda r, dc=days_col, rc=red_col: f"RO: {r['RO_ID']}<br>State: {r['CNT_STATE']}<br>Stage: {r['LATEST_STAGE_NM']}<br>Days: {r.get(dc, 'N/A')}<br>Red Flags: {r.get(rc, 'N/A')}",
                    axis=1,
                ),
                hovertemplate="%{text}<extra></extra>",
            ))

    fig.update_layout(
        title="Stuck RO Tracker — Priority & Days Stuck",
        xaxis_title="Organization",
        yaxis_title="Days Stuck",
        height=450,
    )
    return _fig_to_json(fig)


def create_report_generator_chart(data: dict, report_type: str) -> dict:
    """Generate a chart for the operational report."""
    if report_type == "state_overview":
        # Combine multiple metrics into a summary dashboard
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="number+delta",
            value=data.get("total_ros", 0),
            title={"text": "Total ROs"},
            domain={"x": [0, 0.33], "y": [0.5, 1]},
        ))
        fig.add_trace(go.Indicator(
            mode="number+delta",
            value=data.get("stuck_ros", 0),
            title={"text": "Stuck ROs"},
            domain={"x": [0.33, 0.66], "y": [0.5, 1]},
        ))
        fig.add_trace(go.Indicator(
            mode="number+delta",
            value=data.get("failed_ros", 0),
            title={"text": "Failed ROs"},
            domain={"x": [0.66, 1], "y": [0.5, 1]},
        ))
        fig.update_layout(title="Pipeline Overview", height=300)
        return _fig_to_json(fig)

    return None

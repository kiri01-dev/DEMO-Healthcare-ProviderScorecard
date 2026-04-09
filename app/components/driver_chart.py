"""Driver attribution horizontal bar chart."""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from app.config import DRIVER_COLORS, COLORS


def render_driver_chart(drivers: list[dict]):
    """Render a horizontal bar chart of drivers sorted by absolute contribution."""
    available = [d for d in drivers if d["available"] and abs(d["contribution_wrvu"]) > 0.1]
    if not available:
        st.info("No driver data available for this period comparison.")
        return

    df = pd.DataFrame(available).sort_values("contribution_wrvu", ascending=True)
    df["color"] = df["driver_category"].map(DRIVER_COLORS).fillna(COLORS["gray_dark"])
    df["label"] = df.apply(
        lambda r: f"{r['driver_name']} ({r['contribution_wrvu']:+.0f} wRVU)", axis=1
    )

    fig = go.Figure(go.Bar(
        x=df["contribution_wrvu"],
        y=df["label"],
        orientation="h",
        marker_color=df["color"],
        text=df["contribution_wrvu"].apply(lambda v: f"{v:+.0f}"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>%{x:+.1f} wRVU<extra></extra>",
    ))

    fig.add_vline(x=0, line_color=COLORS["gray_dark"], line_width=1)

    fig.update_layout(
        title=dict(text="Performance Drivers (wRVU contribution)", font=dict(size=13)),
        xaxis_title="wRVU Change",
        yaxis_title=None,
        height=max(200, len(available) * 55 + 80),
        margin=dict(l=0, r=60, t=40, b=30),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        font=dict(family="Arial", size=11),
        xaxis=dict(gridcolor="#EEEEEE", zeroline=True, zerolinecolor="#CCCCCC"),
    )

    st.plotly_chart(fig, use_container_width=True)

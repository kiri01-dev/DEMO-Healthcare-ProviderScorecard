"""
Opportunity Dashboard — David's ranked view of provider performance.
"""
import datetime

import streamlit as st
import pandas as pd
import plotly.express as px

from app.config import COLORS, DEFAULT_PERIOD
from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy


def render_dashboard(providers_df: pd.DataFrame, provider_month_df: pd.DataFrame):
    """Render the Opportunity Dashboard page."""

    # ── Header ────────────────────────────────────────────────────────────────
    col_title, col_info = st.columns([3, 1])
    with col_title:
        st.markdown(
            f"<h2 style='color:{COLORS['meridian_blue']};margin-bottom:4px;'>"
            f"Provider Opportunity Dashboard</h2>",
            unsafe_allow_html=True,
        )
        st.caption("Ranked by peer-normalized opportunity score. Select a provider to view their full report card.")
    with col_info:
        st.markdown(
            f"<div style='background:{COLORS['meridian_gold']};padding:8px 12px;border-radius:6px;"
            f"text-align:center;'><span style='color:white;font-size:11px;font-weight:bold;'>"
            f"Data as of Nov 2024</span></div>",
            unsafe_allow_html=True,
        )

    st.divider()

    period = st.session_state.get("epd_selected_period", DEFAULT_PERIOD)
    specialty_filter = st.session_state.get("epd_selected_specialty", "All")

    # ── Build ranked table ────────────────────────────────────────────────────
    with st.spinner("Computing opportunity scores..."):
        ranked_df = _build_ranked_table(providers_df, provider_month_df, period, specialty_filter)

    if ranked_df.empty:
        st.warning("No provider data available for the selected filters.")
        return

    # ── Summary tiles ─────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Providers Shown", len(ranked_df))
    with c2:
        below_target = (ranked_df["Signal"].str.contains("Below Target")).sum()
        st.metric("Below Target", int(below_target))
    with c3:
        avg_score = ranked_df["Opp. Score"].mean()
        st.metric("Avg Opportunity Score", f"{avg_score:.0f}")
    with c4:
        st.metric("Period", _fmt_period(period))

    st.markdown("---")

    # ── Ranked table ──────────────────────────────────────────────────────────
    st.markdown(f"**All Providers — {_fmt_period(period)}**")

    # Prominent selection instruction banner
    st.markdown(
        """
        <div style='background:#EEF2FF;border-left:4px solid #012169;border-radius:4px;
        padding:10px 14px;margin-bottom:8px;display:flex;align-items:center;gap:8px;'>
        <span style='font-size:18px;'>☑</span>
        <span style='font-size:13px;color:#012169;font-weight:600;'>
        Select a provider — tick the checkbox at the left of any row to open their full report card.
        </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    display_cols = [
        "Rank", "Provider", "Specialty", "wRVU (Actual)", "Peer Median",
        "Gap", "Opp. Score", "Top Driver", "Confidence", "Signal",
    ]
    max_score = max(int(ranked_df["Opp. Score"].max()) + 10, 10)

    event = st.dataframe(
        ranked_df[display_cols],
        use_container_width=True,
        hide_index=True,
        height=min(600, 42 + len(ranked_df) * 35),
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Opp. Score": st.column_config.ProgressColumn(
                "Opp. Score",
                min_value=0,
                max_value=max_score,
                format="%d",
            ),
        },
    )

    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        selected_id = ranked_df.iloc[selected_idx]["provider_id"]
        st.session_state.epd_selected_provider = selected_id
        st.session_state.epd_current_page = "drilldown"
        st.rerun()

    # ── Distribution chart ────────────────────────────────────────────────────
    with st.expander("Opportunity Score Distribution", expanded=True):
        fig = px.bar(
            ranked_df.sort_values("Opp. Score", ascending=False),
            x="Provider",
            y="Opp. Score",
            color="Specialty",
            color_discrete_map={
                "Internal Medicine": COLORS["meridian_blue"],
                "Cardiology":        COLORS["meridian_gold"],
            },
            title="Provider Opportunity Score",
        )
        fig.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=40, b=60),
            plot_bgcolor="white",
            paper_bgcolor="white",
            xaxis_tickangle=-45,
            font=dict(family="Arial", size=10),
        )
        st.plotly_chart(fig, use_container_width=True)


def _build_ranked_table(
    providers_df: pd.DataFrame,
    provider_month_df: pd.DataFrame,
    period: str,
    specialty_filter: str,
) -> pd.DataFrame:
    """Compute opportunity scores for all providers in the selected period."""
    period_data = provider_month_df[provider_month_df["service_month"] == period]

    filtered_providers = providers_df.copy()
    if specialty_filter != "All":
        filtered_providers = filtered_providers[
            filtered_providers["specialty"] == specialty_filter
        ]

    rows = []
    for _, provider in filtered_providers.iterrows():
        pid = provider["provider_id"]
        p_data = period_data[period_data["provider_id"] == pid]
        if p_data.empty:
            continue

        actual_wrvu = float(p_data["total_wrvu"].values[0])
        enc_count = (
            int(p_data["encounter_count"].values[0])
            if "encounter_count" in p_data.columns
            else 0
        )

        cohort = get_cohort(pid, period, providers_df, provider_month_df)
        stats = get_cohort_stats(cohort["peer_ids"], period, provider_month_df)
        percentile = get_percentile(actual_wrvu, cohort["peer_ids"], period, provider_month_df)

        conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
        adeq = compute_adequacy(percentile, conf["score"], cohort["cohort_n"])

        gap = actual_wrvu - stats["median"]
        opp_score = max(0.0, -gap) * (conf["score"] / 100)

        signal_labels = {
            "green":       "On Track",
            "yellow":      "Watch",
            "red":         "Below Target",
            "unavailable": "N/A",
        }

        rows.append({
            "provider_id":   pid,
            "Rank":          0,
            "Provider":      provider["provider_name"],
            "Specialty":     provider["specialty"],
            "wRVU (Actual)": actual_wrvu,
            "Peer Median":   stats["median"],
            "Gap":           gap,
            "Opp. Score":    opp_score,
            "Top Driver":    "See detail",
            "Confidence":    f"{conf['score']}/100",
            "Signal":        signal_labels.get(adeq["signal"], "N/A"),
        })

    if not rows:
        return pd.DataFrame()

    df = (
        pd.DataFrame(rows)
        .sort_values("Opp. Score", ascending=False)
        .reset_index(drop=True)
    )
    df["Rank"] = range(1, len(df) + 1)
    return df


def _fmt_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY'."""
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")

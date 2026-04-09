"""
Provider Drilldown — Report Card + Explanation View.
"""
import datetime
import os

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from app.config import COLORS, DEFAULT_PERIOD, ALL_PERIODS, BASELINE_MONTHS
from analytics.data_layer import (
    get_connection, load_cpt_mix, load_pos_mix, load_denial_summary, load_em_distribution,
)
from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.driver_attribution import compute_drivers
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy
from analytics.narrative_engine import generate_narrative, get_intervention_suggestions
from app.components.kpi_tiles import render_kpi_tiles
from app.components.adequacy_badge import render_adequacy_badge
from app.components.driver_chart import render_driver_chart
from app.components.evidence_panels import render_evidence_panels


def render_drilldown(
    providers_df: pd.DataFrame,
    provider_month_df: pd.DataFrame,
    charges_df: pd.DataFrame,
    cpt_ref_df: pd.DataFrame,
):
    """Render the Provider Drilldown page."""
    provider_id = st.session_state.get("epd_selected_provider")
    if not provider_id:
        st.error("No provider selected. Return to the dashboard.")
        if st.button("Back to Dashboard"):
            st.session_state.epd_current_page = "dashboard"
            st.rerun()
        return

    period = st.session_state.get("epd_selected_period", DEFAULT_PERIOD)
    provider = providers_df[providers_df["provider_id"] == provider_id].iloc[0]

    # ── Breadcrumb ────────────────────────────────────────────────────────────
    if st.button("Back to Opportunity Dashboard"):
        st.session_state.epd_current_page = "dashboard"
        st.session_state.epd_selected_provider = None
        st.rerun()

    st.markdown(
        f"<h2 style='color:{COLORS['meridian_blue']};margin-bottom:2px;'>"
        f"{provider['provider_name']}</h2>"
        f"<span style='color:{COLORS['gray_dark']};font-size:13px;'>"
        f"{provider['specialty']} &middot; {provider['primary_site_id']} "
        f"&middot; {_fmt_period(period)}</span>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ── Analytics computations ────────────────────────────────────────────────
    p_month = provider_month_df[provider_month_df["provider_id"] == provider_id]
    current_row = p_month[p_month["service_month"] == period]
    if current_row.empty:
        st.warning(f"No data for {provider['provider_name']} in {_fmt_period(period)}.")
        return

    current_wrvu = float(current_row["total_wrvu"].values[0])
    enc_count = (
        int(current_row["encounter_count"].values[0])
        if "encounter_count" in current_row.columns
        else 0
    )
    charges_val = (
        float(current_row["total_charges"].values[0])
        if "total_charges" in current_row.columns
        else 0.0
    )

    # Prior period
    period_idx = ALL_PERIODS.index(period)
    prior_period = ALL_PERIODS[period_idx - 1] if period_idx > 0 else period
    prior_row = p_month[p_month["service_month"] == prior_period]
    prior_wrvu = float(prior_row["total_wrvu"].values[0]) if not prior_row.empty else current_wrvu

    # Baseline (6-month rolling avg)
    baseline_months = ALL_PERIODS[max(0, period_idx - BASELINE_MONTHS):period_idx]
    baseline_data = p_month[p_month["service_month"].isin(baseline_months)]
    baseline_wrvu = (
        float(baseline_data["total_wrvu"].mean()) if not baseline_data.empty else current_wrvu
    )

    # Cohort, confidence, adequacy
    cohort = get_cohort(provider_id, period, providers_df, provider_month_df)
    stats = get_cohort_stats(cohort["peer_ids"], period, provider_month_df)
    percentile = get_percentile(current_wrvu, cohort["peer_ids"], period, provider_month_df)
    conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
    adeq = compute_adequacy(percentile, conf["score"], cohort["cohort_n"])

    # Load contextual data from DB
    con = get_connection()
    periods_pair = [prior_period, period]
    cpt_current = load_cpt_mix(con, provider_id, [period])
    cpt_prior = load_cpt_mix(con, provider_id, [prior_period])
    pos_current = load_pos_mix(con, provider_id, [period])
    pos_prior = load_pos_mix(con, provider_id, [prior_period])
    em_current = load_em_distribution(con, provider_id, [period])
    em_prior = load_em_distribution(con, provider_id, [prior_period])
    denial_df = load_denial_summary(con, provider_id, periods_pair)
    con.close()

    # Driver attribution
    pos_combined = pd.concat([pos_current, pos_prior]) if not (pos_current.empty and pos_prior.empty) else pd.DataFrame()
    drivers = compute_drivers(
        provider_id, period, prior_period, charges_df, cpt_ref_df,
        pos_combined,
        denial_df if not denial_df.empty else None,
    )

    # ── Row 1: KPI tiles ──────────────────────────────────────────────────────
    denial_count = (
        denial_df[denial_df["service_month"] == period]["denial_count"].sum()
        if not denial_df.empty
        else None
    )
    denial_rate = (
        float(denial_count) / enc_count
        if (denial_count is not None and enc_count > 0)
        else None
    )
    render_kpi_tiles(current_wrvu, enc_count, charges_val, denial_rate)
    st.markdown("")

    # ── Row 2: Trend + Adequacy badge ─────────────────────────────────────────
    col_trend, col_adequacy = st.columns([6, 4])
    with col_trend:
        _render_trend_chart(p_month, period, provider["provider_name"])
    with col_adequacy:
        st.markdown(f"**Performance Signal — {_fmt_period(period)}**")
        render_adequacy_badge(adeq, conf, cohort)

    st.markdown("---")

    # ── Explain Performance ───────────────────────────────────────────────────
    with st.expander("Explain Performance", expanded=True):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        narrative = generate_narrative(
            provider_name=provider["provider_name"],
            period=period,
            current_wrvu=current_wrvu,
            prior_wrvu=prior_wrvu,
            baseline_wrvu=baseline_wrvu,
            cohort_stats=stats,
            percentile=percentile,
            drivers=drivers,
            adequacy=adeq,
            confidence=conf,
            use_api=bool(api_key),
            api_key=api_key,
        )

        st.markdown(
            f"<div style='background:{COLORS['gray_light']};padding:16px;border-radius:8px;"
            f"border-left:4px solid {COLORS['meridian_gold']};font-size:14px;line-height:1.6;'>"
            f"{narrative}</div>",
            unsafe_allow_html=True,
        )

        st.markdown("**Performance Drivers**")
        st.caption(f"Comparing {_fmt_period(prior_period)} to {_fmt_period(period)}")
        render_driver_chart(drivers)

        st.markdown("**Evidence Panels**")
        render_evidence_panels(
            provider_id, period, prior_period,
            cpt_current, cpt_prior,
            em_current, em_prior,
            pos_current, pos_prior,
        )

    # ── Suggested Interventions ───────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**Suggested Interventions**")
    suggestions = get_intervention_suggestions(drivers, provider["specialty"])

    role_colors = {
        "Provider": COLORS["meridian_blue"],
        "Coding":   COLORS["green"],
        "Ops":      COLORS["yellow"],
    }
    cols = st.columns(len(suggestions))
    for col, sug in zip(cols, suggestions):
        with col:
            role_color = role_colors.get(sug["role"], COLORS["gray_dark"])
            st.markdown(
                f"<div style='border:1px solid #ddd;border-radius:8px;padding:14px;'>"
                f"<span style='background:{role_color};color:white;padding:2px 8px;"
                f"border-radius:4px;font-size:11px;font-weight:bold;'>{sug['role']}</span>"
                f"<br><br>"
                f"<b style='font-size:13px;'>{sug['title']}</b><br>"
                f"<span style='font-size:11px;color:{COLORS['gray_dark']};'>"
                f"{sug['rationale']}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )


def _render_trend_chart(p_month: pd.DataFrame, current_period: str, provider_name: str):
    """Render 12-month wRVU trend with 6-month rolling average."""
    df = p_month.sort_values("service_month").copy()
    df["rolling_avg"] = df["total_wrvu"].rolling(window=6, min_periods=1).mean()

    bar_colors = [
        COLORS["red"] if m == current_period else COLORS["meridian_blue"]
        for m in df["service_month"]
    ]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["service_month"],
        y=df["total_wrvu"],
        name="wRVU",
        marker_color=bar_colors,
        opacity=0.85,
    ))
    fig.add_trace(go.Scatter(
        x=df["service_month"],
        y=df["rolling_avg"],
        name="6-mo avg",
        line=dict(color=COLORS["meridian_gold"], width=2, dash="dash"),
    ))
    fig.update_layout(
        title=dict(text="12-Month wRVU Trend", font=dict(size=12)),
        height=260,
        margin=dict(l=0, r=0, t=40, b=30),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", y=1.1),
        yaxis_title="wRVU",
        xaxis_title=None,
        font=dict(family="Arial", size=10),
        xaxis=dict(tickangle=-45, type="category"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _fmt_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY'."""
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")

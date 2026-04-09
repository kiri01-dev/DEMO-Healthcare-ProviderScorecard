"""Evidence drill-down panel tabs."""
import datetime

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from app.config import COLORS


def render_evidence_panels(
    provider_id: str,
    current_period: str,
    prior_period: str,
    cpt_mix_current: pd.DataFrame,
    cpt_mix_prior: pd.DataFrame,
    em_current: pd.DataFrame,
    em_prior: pd.DataFrame,
    pos_current: pd.DataFrame,
    pos_prior: pd.DataFrame,
):
    """Render tabbed evidence panels: CPT Mix, E&M Levels, Site of Service."""
    tab1, tab2, tab3 = st.tabs(["CPT Mix", "E&M Levels", "Site of Service"])

    with tab1:
        _render_cpt_mix(cpt_mix_current, cpt_mix_prior, current_period, prior_period)

    with tab2:
        _render_em_levels(em_current, em_prior, current_period, prior_period)

    with tab3:
        _render_pos_mix(pos_current, pos_prior, current_period, prior_period)


def _render_cpt_mix(
    current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str
):
    """CPT family mix shift — top movers table."""
    st.markdown("**CPT Family Mix Shift — Top Movers**")

    if current.empty and prior.empty:
        st.info("No CPT data available.")
        return

    c_fam = (
        current.groupby("cpt_family")
        .agg(total_wrvu=("total_wrvu", "sum"), units=("total_units", "sum"))
        .reset_index()
    )
    p_fam = (
        prior.groupby("cpt_family")
        .agg(total_wrvu=("total_wrvu", "sum"), units=("total_units", "sum"))
        .reset_index()
    )
    c_fam.columns = ["cpt_family", "current_wrvu", "current_units"]
    p_fam.columns = ["cpt_family", "prior_wrvu", "prior_units"]

    merged = pd.merge(c_fam, p_fam, on="cpt_family", how="outer").fillna(0)
    merged["delta_wrvu"] = merged["current_wrvu"] - merged["prior_wrvu"]
    merged = merged.sort_values("delta_wrvu", ascending=True)

    cur_lbl = _fmt_period(cur_period)
    pri_lbl = _fmt_period(pri_period)
    display = merged.rename(columns={
        "cpt_family":   "CPT Family",
        "prior_wrvu":   f"wRVU {pri_lbl}",
        "current_wrvu": f"wRVU {cur_lbl}",
        "delta_wrvu":   "Change",
    })[["CPT Family", f"wRVU {pri_lbl}", f"wRVU {cur_lbl}", "Change"]]

    def _color_change(val):
        if isinstance(val, (int, float)):
            if val < 0:
                return f"color: {COLORS['red']}"
            if val > 0:
                return f"color: {COLORS['green']}"
        return ""

    styled = display.style.format({
        f"wRVU {pri_lbl}": "{:.0f}",
        f"wRVU {cur_lbl}": "{:.0f}",
        "Change": "{:+.0f}",
    }).map(_color_change, subset=["Change"])

    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_em_levels(
    current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str
):
    """E&M level distribution — grouped bar chart."""
    st.markdown("**E&M Level Distribution**")

    em_codes = ["99212", "99213", "99214", "99215"]
    em_labels = {
        "99212": "99212 (Low)",
        "99213": "99213 (Low-Mod)",
        "99214": "99214 (Mod)",
        "99215": "99215 (High)",
    }

    c_agg = current.groupby("cpt_code")["units"].sum().reindex(em_codes, fill_value=0)
    p_agg = prior.groupby("cpt_code")["units"].sum().reindex(em_codes, fill_value=0)

    if c_agg.sum() == 0 and p_agg.sum() == 0:
        st.info("No E&M data available for this provider.")
        return

    fig = go.Figure(data=[
        go.Bar(
            name=_fmt_period(pri_period),
            x=[em_labels[c] for c in em_codes],
            y=p_agg.values,
            marker_color=COLORS["gray_dark"],
            opacity=0.7,
        ),
        go.Bar(
            name=_fmt_period(cur_period),
            x=[em_labels[c] for c in em_codes],
            y=c_agg.values,
            marker_color=COLORS["meridian_blue"],
        ),
    ])
    fig.update_layout(
        barmode="group",
        height=280,
        margin=dict(l=0, r=0, t=30, b=30),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", y=1.1),
        yaxis_title="Visit Count",
        font=dict(family="Arial", size=11),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_pos_mix(
    current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str
):
    """Site-of-service distribution — pie comparison."""
    st.markdown("**Site-of-Service Distribution**")

    if current.empty and prior.empty:
        st.info("No site-of-service data available.")
        return

    palette = [COLORS["meridian_blue"], COLORS["meridian_gold"], COLORS["green"]]
    col1, col2 = st.columns(2)

    with col1:
        st.caption(_fmt_period(pri_period))
        if not prior.empty:
            fig = px.pie(
                prior, values="encounter_count", names="pos_label",
                color_discrete_sequence=palette,
            )
            fig.update_layout(
                height=220, margin=dict(l=0, r=0, t=0, b=0),
                showlegend=True, font=dict(family="Arial", size=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.caption(_fmt_period(cur_period))
        if not current.empty:
            fig = px.pie(
                current, values="encounter_count", names="pos_label",
                color_discrete_sequence=palette,
            )
            fig.update_layout(
                height=220, margin=dict(l=0, r=0, t=0, b=0),
                showlegend=True, font=dict(family="Arial", size=10),
            )
            st.plotly_chart(fig, use_container_width=True)


def _fmt_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY'."""
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")

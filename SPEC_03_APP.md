# SPEC_03 — Session 3: Streamlit App (UI)
## Meridian Physician Division Provider Performance App (PoC)

> **Claude Code Session 3 of 3.**
> Prerequisite: Sessions 1 and 2 complete. All unit tests from Session 2 pass.
> Read `CLAUDE.md`, `PRD_PoC.md`, and `ARCHITECTURE.md` before starting.
> Working directory: `APP_Emory PoC v2/`. Activate `.venv` before running any Python.

---

## Session Goal

By the end of this session, a working Streamlit app must be running locally with:

- [ ] `app/config.py` present (created in Session 2 — verify, do not overwrite)
- [ ] `app/main.py` — Streamlit entry point with sidebar, routing, and data loading
- [ ] `app/pages/opportunity_dashboard.py` — David's ranked opportunity view
- [ ] `app/pages/provider_drilldown.py` — Provider Report Card + Explanation View
- [ ] `app/components/kpi_tiles.py` — KPI metric tile widgets
- [ ] `app/components/adequacy_badge.py` — Adequacy signal badge
- [ ] `app/components/driver_chart.py` — Horizontal driver attribution chart
- [ ] `app/components/evidence_panels.py` — Tabbed evidence drill-downs
- [ ] `tests/smoke_test.py` — validates all 20 providers return complete outputs
- [ ] `RUNME.md` — one-command Windows launch instructions
- [ ] App loads without errors and demo journey completes end-to-end

---

## Step 1: Create `app/main.py`

```python
"""
Meridian Physician Division — Provider Performance App
Entry point. Run with: streamlit run app/main.py
"""
import streamlit as st
from pathlib import Path
import sys
import os

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env before anything else
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# Must be first Streamlit call
st.set_page_config(
    page_title="Meridian Physician Performance",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

import duckdb
import pandas as pd
from app.config import DB_PATH, ALL_PERIODS, DEFAULT_PERIOD, COLORS
from analytics.data_layer import (
    get_connection, load_all_providers, load_provider_month_summary,
)


# ── Session State Initialization ─────────────────────────────────────────────
def init_session_state():
    defaults = {
        "epd_current_page":        "dashboard",
        "epd_selected_provider":   None,
        "epd_selected_period":     DEFAULT_PERIOD,
        "epd_selected_specialty":  "All",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ── Data Loading (cached) ─────────────────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    """Open a single shared DuckDB connection (read-only)."""
    return get_connection(read_only=True)


@st.cache_data(ttl=3600)
def load_providers():
    con = get_db_connection()
    return load_all_providers(con)


@st.cache_data(ttl=3600)
def load_provider_months():
    con = get_db_connection()
    return load_provider_month_summary(con)


@st.cache_data(ttl=3600)
def load_all_charges():
    con = get_db_connection()
    return con.execute("SELECT * FROM fact_charge_line").df()


@st.cache_data(ttl=3600)
def load_cpt_reference():
    con = get_db_connection()
    return con.execute("SELECT * FROM dim_cpt").df()


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar(providers_df, provider_month_df):
    with st.sidebar:
        # Logo / header
        st.markdown(
            f"""<div style='background-color:{COLORS["meridian_blue"]};padding:16px;border-radius:6px;margin-bottom:12px;'>
            <span style='color:{COLORS["meridian_gold"]};font-weight:bold;font-size:16px;'>🏥 Meridian Physician Division</span><br>
            <span style='color:white;font-size:12px;'>Provider Performance PoC</span>
            </div>""",
            unsafe_allow_html=True
        )

        # Data freshness
        st.caption(f"📅 Data as of: {max(ALL_PERIODS)}")

        st.divider()

        # Navigation
        st.markdown("**Navigation**")
        if st.button("📊 Opportunity Dashboard", use_container_width=True,
                     type="primary" if st.session_state.epd_current_page == "dashboard" else "secondary"):
            st.session_state.epd_current_page = "dashboard"
            st.session_state.epd_selected_provider = None
            st.rerun()

        st.divider()

        # Filters (apply globally)
        st.markdown("**Filters**")

        specialties = ["All"] + sorted(providers_df["specialty"].unique().tolist())
        selected_specialty = st.selectbox(
            "Specialty",
            specialties,
            index=specialties.index(st.session_state.epd_selected_specialty),
            key="sidebar_specialty"
        )
        st.session_state.epd_selected_specialty = selected_specialty

        selected_period = st.selectbox(
            "Period",
            ALL_PERIODS[::-1],  # Most recent first
            index=ALL_PERIODS[::-1].index(st.session_state.epd_selected_period),
            key="sidebar_period",
            format_func=lambda p: _format_period(p)
        )
        st.session_state.epd_selected_period = selected_period

        st.divider()
        st.caption("PoC build — synthetic data only")


def _format_period(period: str) -> str:
    import datetime
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    init_session_state()

    # Load data
    try:
        providers_df = load_providers()
        provider_month_df = load_provider_months()
        charges_df = load_all_charges()
        cpt_ref_df = load_cpt_reference()
    except Exception as e:
        st.error(f"Unable to load data. Ensure Session 1 completed successfully. Error: {e}")
        st.stop()

    # Render sidebar
    render_sidebar(providers_df, provider_month_df)

    # Route to correct page
    if st.session_state.epd_current_page == "dashboard":
        from app.pages.opportunity_dashboard import render_dashboard
        render_dashboard(providers_df, provider_month_df)

    elif st.session_state.epd_current_page == "drilldown":
        from app.pages.provider_drilldown import render_drilldown
        render_drilldown(
            providers_df, provider_month_df, charges_df, cpt_ref_df
        )


if __name__ == "__main__":
    main()
```

---

## Step 2: Create `app/components/kpi_tiles.py`

```python
"""KPI metric tile components."""
import streamlit as st
from app.config import COLORS


def render_kpi_tiles(wrvu: float, encounters: int, charges: float, denial_rate: float | None):
    """Render a row of 4 KPI metric tiles."""
    cols = st.columns(4)

    with cols[0]:
        _tile("wRVU", f"{wrvu:,.0f}", "Work Relative Value Units", COLORS["meridian_blue"])
    with cols[1]:
        _tile("Encounters", f"{encounters:,}", "Patient encounters / visits", COLORS["meridian_blue"])
    with cols[2]:
        _tile("Gross Charges", f"${charges:,.0f}", "Total charges billed", COLORS["meridian_blue"])
    with cols[3]:
        if denial_rate is not None:
            color = COLORS["red"] if denial_rate > 0.10 else COLORS["meridian_blue"]
            _tile("Denial Rate", f"{denial_rate:.1%}", "Claims denied / total claims", color)
        else:
            _tile("Denial Rate", "N/A", "Data not available", COLORS["gray_dark"])


def _tile(label: str, value: str, subtitle: str, color: str):
    st.markdown(
        f"""<div style='background:{color};padding:16px;border-radius:8px;text-align:center;'>
        <div style='color:white;font-size:11px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase;'>{label}</div>
        <div style='color:white;font-size:28px;font-weight:bold;margin:6px 0;'>{value}</div>
        <div style='color:rgba(255,255,255,0.7);font-size:10px;'>{subtitle}</div>
        </div>""",
        unsafe_allow_html=True
    )
```

---

## Step 3: Create `app/components/adequacy_badge.py`

```python
"""Adequacy signal badge component."""
import streamlit as st
from app.config import ADEQUACY_CONFIG, COLORS


def render_adequacy_badge(adequacy: dict, confidence: dict, cohort_info: dict):
    """Render the adequacy signal badge with confidence score and cohort info."""
    signal = adequacy.get("signal", "unavailable")
    if signal == "unavailable":
        cfg = {"hex": "#888888", "label": "Insufficient Data", "bg": "#F5F5F5"}
    else:
        cfg = ADEQUACY_CONFIG[signal]

    # Badge
    st.markdown(
        f"""<div style='background:{cfg["bg"]};border-left:4px solid {cfg["hex"]};
        padding:12px 16px;border-radius:4px;margin-bottom:8px;'>
        <span style='color:{cfg["hex"]};font-weight:bold;font-size:18px;'>
        {_signal_emoji(signal)} {cfg["label"]}</span><br>
        <span style='color:{COLORS["gray_dark"]};font-size:12px;'>{adequacy.get("rationale","")}</span>
        </div>""",
        unsafe_allow_html=True
    )

    # Confidence + cohort
    col1, col2 = st.columns(2)
    with col1:
        conf_color = COLORS["green"] if confidence["score"] >= 75 else (
            COLORS["yellow"] if confidence["score"] >= 45 else COLORS["red"])
        st.markdown(
            f"**Confidence:** <span style='color:{conf_color};font-weight:bold;'>"
            f"{confidence['score']}/100 ({confidence['level']})</span>",
            unsafe_allow_html=True
        )
    with col2:
        n = cohort_info.get("cohort_n", 0)
        warn = " ⚠️" if n < 5 else ""
        st.markdown(f"**Peer cohort:** {n} providers{warn}")

    if confidence["caveats"]:
        with st.expander("⚠️ Data quality notes", expanded=False):
            for caveat in confidence["caveats"]:
                st.caption(f"• {caveat}")

    # Cohort definition
    st.caption(f"📌 Peers: {cohort_info.get('cohort_definition','')}")
    if cohort_info.get("fallback_used"):
        st.warning(f"Fallback cohort used: {cohort_info.get('cohort_definition','')}")


def _signal_emoji(signal: str) -> str:
    return {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(signal, "⚪")
```

---

## Step 4: Create `app/components/driver_chart.py`

```python
"""Driver attribution horizontal bar chart."""
import streamlit as st
import plotly.express as px
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
```

---

## Step 5: Create `app/components/evidence_panels.py`

```python
"""Evidence drill-down panel tabs."""
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
    tab1, tab2, tab3 = st.tabs(["📋 CPT Mix", "📊 E&M Levels", "🏥 Site of Service"])

    with tab1:
        _render_cpt_mix(cpt_mix_current, cpt_mix_prior, current_period, prior_period)

    with tab2:
        _render_em_levels(em_current, em_prior, current_period, prior_period)

    with tab3:
        _render_pos_mix(pos_current, pos_prior, current_period, prior_period)


def _render_cpt_mix(current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str):
    """CPT family mix shift — top movers table."""
    st.markdown("**CPT Family Mix Shift — Top Movers**")

    if current.empty and prior.empty:
        st.info("No CPT data available.")
        return

    # Aggregate by CPT family
    c_fam = current.groupby("cpt_family").agg(total_wrvu=("total_wrvu","sum"), units=("total_units","sum")).reset_index()
    p_fam = prior.groupby("cpt_family").agg(total_wrvu=("total_wrvu","sum"), units=("total_units","sum")).reset_index()
    c_fam.columns = ["cpt_family", "current_wrvu", "current_units"]
    p_fam.columns = ["cpt_family", "prior_wrvu", "prior_units"]

    merged = pd.merge(c_fam, p_fam, on="cpt_family", how="outer").fillna(0)
    merged["delta_wrvu"] = merged["current_wrvu"] - merged["prior_wrvu"]
    merged = merged.sort_values("delta_wrvu", ascending=True)

    # Display table
    display = merged.rename(columns={
        "cpt_family": "CPT Family",
        "prior_wrvu": f"wRVU {_fmt_period(pri_period)}",
        "current_wrvu": f"wRVU {_fmt_period(cur_period)}",
        "delta_wrvu": "Change",
    })[["CPT Family", f"wRVU {_fmt_period(pri_period)}", f"wRVU {_fmt_period(cur_period)}", "Change"]]
    display = display.style.format({
        f"wRVU {_fmt_period(pri_period)}": "{:.0f}",
        f"wRVU {_fmt_period(cur_period)}": "{:.0f}",
        "Change": "{:+.0f}",
    }).applymap(lambda v: f"color: {COLORS['red']}" if isinstance(v, (int, float)) and v < 0
                         else (f"color: {COLORS['green']}" if isinstance(v, (int, float)) and v > 0 else ""),
                subset=["Change"])
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_em_levels(current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str):
    """E&M level distribution — grouped bar chart."""
    st.markdown("**E&M Level Distribution**")

    em_codes = ["99212", "99213", "99214", "99215"]
    em_labels = {"99212": "99212 (Low)", "99213": "99213 (Low-Mod)", "99214": "99214 (Mod)", "99215": "99215 (High)"}

    c_agg = current.groupby("cpt_code")["units"].sum().reindex(em_codes, fill_value=0)
    p_agg = prior.groupby("cpt_code")["units"].sum().reindex(em_codes, fill_value=0)

    if c_agg.sum() == 0 and p_agg.sum() == 0:
        st.info("No E&M data available for this provider.")
        return

    fig = go.Figure(data=[
        go.Bar(name=_fmt_period(pri_period), x=[em_labels[c] for c in em_codes],
               y=p_agg.values, marker_color=COLORS["gray_dark"], opacity=0.7),
        go.Bar(name=_fmt_period(cur_period), x=[em_labels[c] for c in em_codes],
               y=c_agg.values, marker_color=COLORS["meridian_blue"]),
    ])
    fig.update_layout(
        barmode="group", height=280, margin=dict(l=0, r=0, t=30, b=30),
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", y=1.1),
        yaxis_title="Visit Count", font=dict(family="Arial", size=11),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_pos_mix(current: pd.DataFrame, prior: pd.DataFrame, cur_period: str, pri_period: str):
    """Site-of-service distribution — pie / donut comparison."""
    st.markdown("**Site-of-Service Distribution**")

    if current.empty and prior.empty:
        st.info("No site-of-service data available.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.caption(_fmt_period(pri_period))
        if not prior.empty:
            fig = px.pie(prior, values="encounter_count", names="pos_label",
                         color_discrete_sequence=[COLORS["meridian_blue"], COLORS["meridian_gold"], COLORS["green"]])
            fig.update_layout(height=220, margin=dict(l=0, r=0, t=0, b=0),
                              showlegend=True, font=dict(family="Arial", size=10))
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.caption(_fmt_period(cur_period))
        if not current.empty:
            fig = px.pie(current, values="encounter_count", names="pos_label",
                         color_discrete_sequence=[COLORS["meridian_blue"], COLORS["meridian_gold"], COLORS["green"]])
            fig.update_layout(height=220, margin=dict(l=0, r=0, t=0, b=0),
                              showlegend=True, font=dict(family="Arial", size=10))
            st.plotly_chart(fig, use_container_width=True)


def _fmt_period(period: str) -> str:
    import datetime
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")
```

---

## Step 6: Create `app/pages/opportunity_dashboard.py`

```python
"""
Opportunity Dashboard — David's ranked view of provider performance.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from app.config import COLORS, ADEQUACY_CONFIG, DEFAULT_PERIOD
from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy


def render_dashboard(providers_df: pd.DataFrame, provider_month_df: pd.DataFrame):
    """Render the Opportunity Dashboard page."""

    # ── Header ────────────────────────────────────────────────────────────────
    col_title, col_info = st.columns([3, 1])
    with col_title:
        st.markdown(
            f"<h2 style='color:{COLORS['meridian_blue']};margin-bottom:4px;'>Provider Opportunity Dashboard</h2>",
            unsafe_allow_html=True
        )
        st.caption("Ranked by peer-normalized opportunity score. Click any provider to view details.")
    with col_info:
        st.markdown(
            f"<div style='background:{COLORS['meridian_gold']};padding:8px 12px;border-radius:6px;"
            f"text-align:center;'><span style='color:white;font-size:11px;font-weight:bold;'>"
            f"📅 Data as of Nov 2024</span></div>",
            unsafe_allow_html=True
        )

    st.divider()

    # ── Get filter state ──────────────────────────────────────────────────────
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
        below_target = (ranked_df["Signal"] == "🔴 Below Target").sum()
        st.metric("Below Target", below_target, delta=None)
    with c3:
        avg_score = ranked_df["Opp. Score"].mean()
        st.metric("Avg Opportunity Score", f"{avg_score:.0f}")
    with c4:
        st.metric("Period", _fmt_period(period))

    st.markdown("---")

    # ── Ranked Table ──────────────────────────────────────────────────────────
    st.markdown(f"**All Providers — {_fmt_period(period)}**")

    # Make rows clickable via a selectbox workaround
    provider_options = [f"{row['Provider']} ({row['Specialty']})" for _, row in ranked_df.iterrows()]
    selected_option = st.selectbox(
        "Select a provider to drill into →",
        ["— Select a provider —"] + provider_options,
        key="dashboard_provider_select"
    )

    if selected_option != "— Select a provider —":
        idx = provider_options.index(selected_option)
        selected_id = ranked_df.iloc[idx]["provider_id"]
        st.session_state.epd_selected_provider = selected_id
        st.session_state.epd_current_page = "drilldown"
        st.rerun()

    # Display table (styled)
    display_cols = ["Rank", "Provider", "Specialty", "wRVU (Actual)", "Peer Median", "Gap", "Opp. Score", "Top Driver", "Confidence", "Signal"]
    st.dataframe(
        ranked_df[display_cols].style
            .background_gradient(subset=["Opp. Score"], cmap="Blues")
            .format({"wRVU (Actual)": "{:.0f}", "Peer Median": "{:.0f}", "Gap": "{:+.0f}", "Opp. Score": "{:.0f}"}),
        use_container_width=True,
        hide_index=True,
        height=min(600, 42 + len(ranked_df) * 35),
    )

    # ── Mini chart: opportunity score distribution ────────────────────────────
    with st.expander("📈 Opportunity Score Distribution", expanded=False):
        fig = px.bar(
            ranked_df.sort_values("Opp. Score", ascending=False),
            x="Provider", y="Opp. Score", color="Specialty",
            color_discrete_map={"Internal Medicine": COLORS["meridian_blue"], "Cardiology": COLORS["meridian_gold"]},
            title="Provider Opportunity Score",
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=60),
                          plot_bgcolor="white", paper_bgcolor="white",
                          xaxis_tickangle=-45, font=dict(family="Arial", size=10))
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
        filtered_providers = filtered_providers[filtered_providers["specialty"] == specialty_filter]

    rows = []
    for _, provider in filtered_providers.iterrows():
        pid = provider["provider_id"]
        p_data = period_data[period_data["provider_id"] == pid]
        if p_data.empty:
            continue

        actual_wrvu = p_data["total_wrvu"].values[0]
        enc_count = int(p_data["encounter_count"].values[0]) if "encounter_count" in p_data.columns else 0

        # Cohort
        cohort = get_cohort(pid, period, providers_df, provider_month_df)
        stats = get_cohort_stats(cohort["peer_ids"], period, provider_month_df)
        percentile = get_percentile(actual_wrvu, cohort["peer_ids"], period, provider_month_df)

        # Confidence + adequacy
        conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
        adeq = compute_adequacy(percentile, conf["score"], cohort["cohort_n"])

        # Gap and opportunity score
        gap = actual_wrvu - stats["median"]
        opp_score = max(0, -gap) * (conf["score"] / 100)  # Higher = bigger opportunity

        signal_labels = {"green": "🟢 On Track", "yellow": "🟡 Watch", "red": "🔴 Below Target", "unavailable": "⚪ N/A"}

        rows.append({
            "provider_id":    pid,
            "Rank":           0,  # will fill after sort
            "Provider":       provider["provider_name"],
            "Specialty":      provider["specialty"],
            "wRVU (Actual)":  actual_wrvu,
            "Peer Median":    stats["median"],
            "Gap":            gap,
            "Opp. Score":     opp_score,
            "Top Driver":     "See detail",
            "Confidence":     f"{conf['score']}/100",
            "Signal":         signal_labels.get(adeq["signal"], "⚪ N/A"),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("Opp. Score", ascending=False).reset_index(drop=True)
    df["Rank"] = range(1, len(df) + 1)
    return df


def _fmt_period(period: str) -> str:
    import datetime
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")
```

---

## Step 7: Create `app/pages/provider_drilldown.py`

```python
"""
Provider Drilldown — Report Card + Explanation View.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from app.config import COLORS, DEFAULT_PERIOD, ALL_PERIODS, BASELINE_MONTHS
from analytics.data_layer import (
    get_connection, load_cpt_mix, load_pos_mix, load_denial_summary, load_em_distribution
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


def render_drilldown(providers_df, provider_month_df, charges_df, cpt_ref_df):
    """Render the Provider Drilldown page."""
    provider_id = st.session_state.get("epd_selected_provider")
    if not provider_id:
        st.error("No provider selected. Return to the dashboard.")
        if st.button("← Back to Dashboard"):
            st.session_state.epd_current_page = "dashboard"
            st.rerun()
        return

    period = st.session_state.get("epd_selected_period", DEFAULT_PERIOD)
    provider = providers_df[providers_df["provider_id"] == provider_id].iloc[0]

    # ── Breadcrumb + header ───────────────────────────────────────────────────
    if st.button("← Back to Opportunity Dashboard"):
        st.session_state.epd_current_page = "dashboard"
        st.session_state.epd_selected_provider = None
        st.rerun()

    st.markdown(
        f"<h2 style='color:{COLORS['meridian_blue']};margin-bottom:2px;'>{provider['provider_name']}</h2>"
        f"<span style='color:{COLORS['gray_dark']};font-size:13px;'>{provider['specialty']} · {provider['primary_site_id']} · {_fmt_period(period)}</span>",
        unsafe_allow_html=True
    )
    st.markdown("---")

    # ── Compute analytics ─────────────────────────────────────────────────────
    p_month = provider_month_df[provider_month_df["provider_id"] == provider_id]
    current_row = p_month[p_month["service_month"] == period]
    if current_row.empty:
        st.warning(f"No data for {provider['provider_name']} in {_fmt_period(period)}.")
        return

    current_wrvu = float(current_row["total_wrvu"].values[0])
    enc_count = int(current_row["encounter_count"].values[0]) if "encounter_count" in current_row.columns else 0

    # Prior period
    period_idx = ALL_PERIODS.index(period)
    prior_period = ALL_PERIODS[period_idx - 1] if period_idx > 0 else period
    prior_row = p_month[p_month["service_month"] == prior_period]
    prior_wrvu = float(prior_row["total_wrvu"].values[0]) if not prior_row.empty else current_wrvu

    # Baseline (6-month rolling avg)
    baseline_months = ALL_PERIODS[max(0, period_idx - BASELINE_MONTHS):period_idx]
    baseline_data = p_month[p_month["service_month"].isin(baseline_months)]
    baseline_wrvu = float(baseline_data["total_wrvu"].mean()) if not baseline_data.empty else current_wrvu

    # Cohort, confidence, adequacy
    cohort = get_cohort(provider_id, period, providers_df, provider_month_df)
    stats = get_cohort_stats(cohort["peer_ids"], period, provider_month_df)
    percentile = get_percentile(current_wrvu, cohort["peer_ids"], period, provider_month_df)
    conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
    adeq = compute_adequacy(percentile, conf["score"], cohort["cohort_n"])

    # Load contextual data
    con = get_connection()
    periods_pair = [prior_period, period]
    cpt_current = load_cpt_mix(con, provider_id, [period])
    cpt_prior = load_cpt_mix(con, provider_id, [prior_period])
    pos_current = load_pos_mix(con, provider_id, [period])
    pos_prior = load_pos_mix(con, provider_id, [prior_period])
    em_current = load_em_distribution(con, provider_id, [period])
    em_prior = load_em_distribution(con, provider_id, [prior_period])
    denial_df = load_denial_summary(con, provider_id, periods_pair)

    # Driver attribution
    drivers = compute_drivers(
        provider_id, period, prior_period, charges_df, cpt_ref_df,
        pd.concat([pos_current, pos_prior]), denial_df if not denial_df.empty else None
    )

    # ── Row 1: KPI Tiles ──────────────────────────────────────────────────────
    charges = float(current_row.get("total_charges", pd.Series([0])).values[0]) if "total_charges" in current_row.columns else 0
    denial_count = denial_df[denial_df["service_month"] == period]["denial_count"].sum() if not denial_df.empty else None
    total_claims = enc_count
    denial_rate = (denial_count / total_claims) if (denial_count and total_claims > 0) else None

    render_kpi_tiles(current_wrvu, enc_count, charges, denial_rate)
    st.markdown("")

    # ── Row 2: Trend + Adequacy ───────────────────────────────────────────────
    col_trend, col_adequacy = st.columns([6, 4])

    with col_trend:
        _render_trend_chart(p_month, period, provider["provider_name"])

    with col_adequacy:
        st.markdown(f"**Performance Signal — {_fmt_period(period)}**")
        render_adequacy_badge(adeq, conf, cohort)

    st.markdown("---")

    # ── Explanation View ──────────────────────────────────────────────────────
    with st.expander("💡 Explain Performance", expanded=True):

        # Narrative
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
            unsafe_allow_html=True
        )

        # Driver chart
        st.markdown("**Performance Drivers**")
        st.caption(f"Comparing {_fmt_period(prior_period)} → {_fmt_period(period)}")
        render_driver_chart(drivers)

        # Evidence panels
        st.markdown("**Evidence Panels**")
        render_evidence_panels(
            provider_id, period, prior_period,
            cpt_current, cpt_prior,
            em_current, em_prior,
            pos_current, pos_prior,
        )

    # ── Suggested Interventions ───────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**💼 Suggested Interventions**")
    suggestions = get_intervention_suggestions(drivers, provider["specialty"])

    role_colors = {"Provider": COLORS["meridian_blue"], "Coding": COLORS["green"], "Ops": COLORS["yellow"]}
    cols = st.columns(len(suggestions))
    for i, (col, sug) in enumerate(zip(cols, suggestions)):
        with col:
            role_color = role_colors.get(sug["role"], COLORS["gray_dark"])
            st.markdown(
                f"<div style='border:1px solid #ddd;border-radius:8px;padding:14px;height:140px;'>"
                f"<span style='background:{role_color};color:white;padding:2px 8px;border-radius:4px;"
                f"font-size:11px;font-weight:bold;'>{sug['role']}</span><br><br>"
                f"<b style='font-size:13px;'>{sug['title']}</b><br>"
                f"<span style='font-size:11px;color:{COLORS['gray_dark']};'>{sug['rationale']}</span>"
                f"</div>",
                unsafe_allow_html=True
            )


def _render_trend_chart(p_month: pd.DataFrame, current_period: str, provider_name: str):
    """Render 12-month wRVU trend with 6-month rolling average."""
    df = p_month.sort_values("service_month").copy()
    df["rolling_avg"] = df["total_wrvu"].rolling(window=6, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["service_month"], y=df["total_wrvu"],
        name="wRVU", marker_color=COLORS["meridian_blue"], opacity=0.7,
    ))
    fig.add_trace(go.Scatter(
        x=df["service_month"], y=df["rolling_avg"],
        name="6-mo avg", line=dict(color=COLORS["meridian_gold"], width=2, dash="dash"),
    ))

    # Highlight current period
    fig.add_vline(
        x=current_period, line_color=COLORS["red"], line_width=2,
        annotation_text="Selected", annotation_position="top right",
    )

    fig.update_layout(
        title=dict(text=f"12-Month wRVU Trend", font=dict(size=12)),
        height=260, margin=dict(l=0, r=0, t=40, b=30),
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", y=1.1),
        yaxis_title="wRVU", xaxis_title=None,
        font=dict(family="Arial", size=10),
        xaxis=dict(tickangle=-45),
    )
    st.plotly_chart(fig, use_container_width=True)


def _fmt_period(period: str) -> str:
    import datetime
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")
```

---

## Step 8: Create `tests/smoke_test.py`

```python
"""
Smoke test — validates all 20 providers return complete analytics outputs.
Run with: python tests/smoke_test.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
from app.config import DB_PATH, ALL_PERIODS, DEFAULT_PERIOD
from analytics.data_layer import (
    get_connection, load_all_providers, load_provider_month_summary,
    load_cpt_mix, load_pos_mix, load_denial_summary
)
from analytics import (
    get_cohort, get_cohort_stats, get_percentile,
    compute_drivers, compute_confidence, compute_adequacy, generate_narrative
)

ERRORS = []
PERIOD = DEFAULT_PERIOD
PRIOR_PERIOD = "2024-10"


def check(condition: bool, message: str, provider_id: str = ""):
    if not condition:
        ERRORS.append(f"  FAIL [{provider_id}] {message}")


def main():
    print(f"\n=== Smoke Test — All Providers, Period: {PERIOD} ===\n")

    con = get_connection()
    providers = load_all_providers(con)
    pm = load_provider_month_summary(con)
    charges = con.execute("SELECT * FROM fact_charge_line").df()
    cpt_ref = con.execute("SELECT * FROM dim_cpt").df()

    check(len(providers) == 20, f"Expected 20 providers, got {len(providers)}")
    check(len(pm[pm["service_month"] == PERIOD]) == 20, f"Expected 20 rows for {PERIOD}")

    for _, provider in providers.iterrows():
        pid = provider["provider_id"]
        print(f"  Testing {pid} ({provider['specialty']})... ", end="")

        # Get data
        p_data = pm[(pm["provider_id"] == pid) & (pm["service_month"] == PERIOD)]
        check(not p_data.empty, f"No data for period {PERIOD}", pid)
        if p_data.empty:
            print("SKIP")
            continue

        current_wrvu = float(p_data["total_wrvu"].values[0])
        enc_count = int(p_data.get("encounter_count", pd.Series([30])).values[0])

        # Cohort
        cohort = get_cohort(pid, PERIOD, providers, pm)
        check("peer_ids" in cohort, "Missing peer_ids", pid)
        check(cohort["cohort_n"] >= 0, "cohort_n negative", pid)
        check(pid not in cohort["peer_ids"], "Provider in own cohort", pid)

        # Stats
        stats = get_cohort_stats(cohort["peer_ids"], PERIOD, pm)
        check(stats["n"] >= 0, "Cohort stats n negative", pid)

        # Percentile
        pctile = get_percentile(current_wrvu, cohort["peer_ids"], PERIOD, pm)
        check(0 <= pctile <= 100, f"Percentile out of range: {pctile}", pid)

        # Confidence
        conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
        check("score" in conf, "Missing confidence score", pid)
        check(0 <= conf["score"] <= 100, f"Confidence score out of range: {conf['score']}", pid)

        # Adequacy
        adeq = compute_adequacy(pctile, conf["score"], cohort["cohort_n"])
        check("signal" in adeq, "Missing adequacy signal", pid)
        check(adeq["signal"] in ("green","yellow","red","unavailable"), f"Invalid signal: {adeq['signal']}", pid)

        # Drivers
        pos = load_pos_mix(con, pid, [PERIOD, PRIOR_PERIOD])
        denial = load_denial_summary(con, pid, [PERIOD, PRIOR_PERIOD])
        pos_combined = pd.concat([pos]) if not pos.empty else pd.DataFrame()
        drivers = compute_drivers(pid, PERIOD, PRIOR_PERIOD, charges, cpt_ref,
                                   pos_combined, denial if not denial.empty else None)
        check(len(drivers) >= 2, f"Too few drivers: {len(drivers)}", pid)

        # Narrative
        prior_row = pm[(pm["provider_id"] == pid) & (pm["service_month"] == PRIOR_PERIOD)]
        prior_wrvu = float(prior_row["total_wrvu"].values[0]) if not prior_row.empty else current_wrvu
        narrative = generate_narrative(
            provider["provider_name"], PERIOD, current_wrvu, prior_wrvu, current_wrvu,
            stats, pctile, drivers, adeq, conf, use_api=False
        )
        check(isinstance(narrative, str) and len(narrative) > 50, "Narrative too short or wrong type", pid)

        print("OK" if not any(pid in e for e in ERRORS) else "FAIL")

    print(f"\n{'=' * 50}")
    if ERRORS:
        print(f"FAILED — {len(ERRORS)} errors:")
        for err in ERRORS:
            print(err)
        sys.exit(1)
    else:
        print(f"ALL TESTS PASSED ✓  ({len(providers)} providers validated)")


if __name__ == "__main__":
    main()
```

---

## Step 9: Create `RUNME.md`

````markdown
# How to Run the Meridian Provider Performance App

## Prerequisites
- Python 3.10+ installed
- Sessions 1–3 completed (data generated, analytics built, app built)

## Launch (Windows PowerShell)

```powershell
# 1. Navigate to the app folder
cd "APP_Emory PoC v2"

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Set your API key (first time only — edit .env file)
#    Copy .env.example to .env and add your ANTHROPIC_API_KEY

# 4. Launch the app
streamlit run app/main.py
```

The app opens automatically at: **http://localhost:8501**

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ExecutionPolicy` error | Run: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| `duckdb.IOException: data/meridian_poc.duckdb not found` | Run Session 1 first: `python scripts/generate_synthetic_data.py && python scripts/load_to_duckdb.py` |
| `ModuleNotFoundError` | Ensure venv is activated: `.\.venv\Scripts\Activate.ps1` |
| Blank narrative / "API error" | Check `.env` has valid `ANTHROPIC_API_KEY`, or app works without it (template mode) |
| Port 8501 in use | `streamlit run app/main.py --server.port 8502` |

## Demo Script (for David Reyes walkthrough)
1. Open app → **Opportunity Dashboard** loads with all 20 providers
2. Filter by **Cardiology** → see 10 providers ranked by opportunity
3. Select **any red-signal provider** → opens Provider Report Card
4. Click **Explain Performance** → read narrative + driver chart
5. Scroll to **Suggested Interventions** → review 2–3 role-tagged actions
6. Return to dashboard → filter by **Internal Medicine** → repeat
````

---

## Step 10: Run Smoke Tests and Verify App

```powershell
# Run smoke tests
python tests/smoke_test.py

# Launch the app
streamlit run app/main.py
```

Manually verify the demo script in `RUNME.md` completes without errors.

---

## Acceptance Tests

All must pass before closing this session:

1. `python tests/smoke_test.py` exits with 0 errors
2. App launches at `http://localhost:8501` without Python errors
3. Opportunity Dashboard shows all 20 providers (or filtered subset)
4. Clicking any provider navigates to Provider Drilldown
5. Provider Drilldown shows: KPI tiles, adequacy badge, trend chart
6. Expanding "Explain Performance" shows: narrative text, driver chart, 3 evidence tabs
7. Suggested Interventions shows 2–3 role-tagged cards
8. "Back to Dashboard" button returns to dashboard
9. No raw Python tracebacks visible anywhere in the UI

---

## End of Session 3

When all acceptance tests pass:
1. Run the full demo script from `RUNME.md`
2. Report any providers where narrative quality seems low
3. Confirm app is stable and ready for demo with David Reyes

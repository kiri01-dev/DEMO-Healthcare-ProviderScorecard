"""
Meridian Physician Division — Provider Performance App
Entry point. Run with: streamlit run app/main.py
"""
import sys
import os
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env before anything else
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# Must be first Streamlit call
import streamlit as st
st.set_page_config(
    page_title="Meridian Physician Performance",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

import datetime
import pandas as pd
from app.config import DB_PATH, ALL_PERIODS, DEFAULT_PERIOD, COLORS
from analytics.data_layer import get_connection, load_all_providers, load_provider_month_summary


# ── Session State Initialization ──────────────────────────────────────────────
def init_session_state():
    """Initialize session state keys with defaults."""
    defaults = {
        "epd_current_page":       "dashboard",
        "epd_selected_provider":  None,
        "epd_selected_period":    DEFAULT_PERIOD,
        "epd_selected_specialty": "All",
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
    """Load all providers from dim_provider."""
    con = get_db_connection()
    return load_all_providers(con)


@st.cache_data(ttl=3600)
def load_provider_months():
    """Load all monthly provider KPI summary rows."""
    con = get_db_connection()
    return load_provider_month_summary(con)


@st.cache_data(ttl=3600)
def load_all_charges():
    """Load all charge lines (used for driver attribution)."""
    con = get_db_connection()
    return con.execute("SELECT * FROM fact_charge_line").df()


@st.cache_data(ttl=3600)
def load_cpt_reference():
    """Load CPT reference table."""
    con = get_db_connection()
    return con.execute("SELECT * FROM dim_cpt").df()


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar(providers_df: pd.DataFrame, provider_month_df: pd.DataFrame):
    """Render the sidebar with navigation, filters, and metadata."""
    with st.sidebar:
        st.markdown(
            f"""<div style='background-color:{COLORS["meridian_blue"]};padding:16px;
            border-radius:6px;margin-bottom:12px;'>
            <span style='color:{COLORS["meridian_gold"]};font-weight:bold;font-size:16px;'>
            Meridian Physician Division</span><br>
            <span style='color:white;font-size:12px;'>Provider Performance PoC</span>
            </div>""",
            unsafe_allow_html=True,
        )

        st.caption(f"Data as of: {max(ALL_PERIODS)}")
        st.divider()

        # Navigation
        st.markdown("**Navigation**")
        btn_type = "primary" if st.session_state.epd_current_page == "dashboard" else "secondary"
        if st.button("Opportunity Dashboard", use_container_width=True, type=btn_type):
            st.session_state.epd_current_page = "dashboard"
            st.session_state.epd_selected_provider = None
            st.rerun()

        st.divider()

        # Filters
        st.markdown("**Filters**")
        specialties = ["All"] + sorted(providers_df["specialty"].unique().tolist())
        current_spec = st.session_state.epd_selected_specialty
        spec_idx = specialties.index(current_spec) if current_spec in specialties else 0
        selected_specialty = st.selectbox(
            "Specialty", specialties, index=spec_idx, key="sidebar_specialty"
        )
        st.session_state.epd_selected_specialty = selected_specialty

        periods_desc = ALL_PERIODS[::-1]  # Most recent first
        current_period = st.session_state.epd_selected_period
        period_idx = periods_desc.index(current_period) if current_period in periods_desc else 0
        selected_period = st.selectbox(
            "Period",
            periods_desc,
            index=period_idx,
            key="sidebar_period",
            format_func=_format_period,
        )
        st.session_state.epd_selected_period = selected_period

        st.divider()
        st.caption("PoC build — synthetic data only")


def _format_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY'."""
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    """App entry point — initialize state, load data, render sidebar, route pages."""
    init_session_state()

    try:
        providers_df = load_providers()
        provider_month_df = load_provider_months()
        charges_df = load_all_charges()
        cpt_ref_df = load_cpt_reference()
    except Exception as e:
        st.error(
            f"Unable to load data. Ensure Session 1 completed successfully. Error: {e}"
        )
        st.stop()

    render_sidebar(providers_df, provider_month_df)

    if st.session_state.epd_current_page == "dashboard":
        from app.views.opportunity_dashboard import render_dashboard
        render_dashboard(providers_df, provider_month_df)

    elif st.session_state.epd_current_page == "drilldown":
        from app.views.provider_drilldown import render_drilldown
        render_drilldown(providers_df, provider_month_df, charges_df, cpt_ref_df)


if __name__ == "__main__":
    main()

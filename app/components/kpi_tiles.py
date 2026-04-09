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
    """Render a single styled KPI tile."""
    st.markdown(
        f"""<div style='background:{color};padding:16px;border-radius:8px;text-align:center;'>
        <div style='color:white;font-size:11px;font-weight:600;letter-spacing:0.5px;
        text-transform:uppercase;'>{label}</div>
        <div style='color:white;font-size:28px;font-weight:bold;margin:6px 0;'>{value}</div>
        <div style='color:rgba(255,255,255,0.7);font-size:10px;'>{subtitle}</div>
        </div>""",
        unsafe_allow_html=True,
    )

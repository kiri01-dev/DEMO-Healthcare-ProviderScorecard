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

    # Main badge
    st.markdown(
        f"""<div style='background:{cfg["bg"]};border-left:4px solid {cfg["hex"]};
        padding:12px 16px;border-radius:4px;margin-bottom:8px;'>
        <span style='color:{cfg["hex"]};font-weight:bold;font-size:18px;'>
        {_signal_emoji(signal)} {cfg["label"]}</span><br>
        <span style='color:{COLORS["gray_dark"]};font-size:12px;'>
        {adequacy.get("rationale", "")}</span>
        </div>""",
        unsafe_allow_html=True,
    )

    # Confidence + cohort row
    col1, col2 = st.columns(2)
    with col1:
        conf_score = confidence["score"]
        conf_color = (
            COLORS["green"] if conf_score >= 75
            else (COLORS["yellow"] if conf_score >= 45 else COLORS["red"])
        )
        st.markdown(
            f"**Confidence:** <span style='color:{conf_color};font-weight:bold;'>"
            f"{conf_score}/100 ({confidence['level']})</span>",
            unsafe_allow_html=True,
        )
    with col2:
        n = cohort_info.get("cohort_n", 0)
        warn = " [!]" if n < 5 else ""
        st.markdown(f"**Peer cohort:** {n} providers{warn}")

    if confidence.get("caveats"):
        with st.expander("Data quality notes", expanded=False):
            for caveat in confidence["caveats"]:
                st.caption(f"- {caveat}")

    # Cohort definition
    st.caption(f"Peers: {cohort_info.get('cohort_definition', '')}")
    if cohort_info.get("fallback_used"):
        st.warning(f"Fallback cohort used: {cohort_info.get('cohort_definition', '')}")


def _signal_emoji(signal: str) -> str:
    """Return a text label for the signal (no Unicode emoji to avoid encoding issues)."""
    return {"green": "[ON TRACK]", "yellow": "[WATCH]", "red": "[BELOW TARGET]"}.get(
        signal, "[N/A]"
    )

"""Adequacy signal computation (Green / Yellow / Red)."""
from app.config import ADEQUACY_THRESHOLDS, ADEQUACY_CONFIG


def compute_adequacy(percentile: float, confidence_score: int, cohort_n: int) -> dict:
    """
    Compute adequacy signal based on percentile rank and confidence.

    Rules:
    - > 75th percentile  -> Green  ("On Track")
    - 25th-75th          -> Yellow ("Watch")
    - < 25th percentile  -> Red    ("Below Target")
    - If confidence < 40 OR cohort_n < 3: downgrade one level and note it
    - If cohort_n < 3: signal is 'unavailable'

    Returns signal, color_hex, label, bg, rationale.
    """
    if cohort_n < 3:
        return {
            "signal":    "unavailable",
            "color_hex": "#888888",
            "label":     "Insufficient Data",
            "bg":        "#F5F5F5",
            "rationale": f"Peer cohort too small (n={cohort_n}) to make a reliable comparison.",
        }

    if percentile > ADEQUACY_THRESHOLDS["green"]:
        raw_signal = "green"
        rationale = (
            f"Performance ranks above the 75th percentile of peers ({percentile:.0f}th percentile)."
        )
    elif percentile >= ADEQUACY_THRESHOLDS["yellow"]:
        raw_signal = "yellow"
        rationale = (
            f"Performance is within the middle range of peers ({percentile:.0f}th percentile)."
        )
    else:
        raw_signal = "red"
        rationale = (
            f"Performance ranks below the 25th percentile of peers ({percentile:.0f}th percentile)."
        )

    # Downgrade if low confidence
    final_signal = raw_signal
    if confidence_score < 40:
        downgrade_map = {"green": "yellow", "yellow": "red", "red": "red"}
        final_signal = downgrade_map[raw_signal]
        rationale += " Note: low data confidence — signal downgraded one level."

    cfg = ADEQUACY_CONFIG[final_signal]
    return {
        "signal":    final_signal,
        "color_hex": cfg["hex"],
        "label":     cfg["label"],
        "bg":        cfg["bg"],
        "rationale": rationale,
    }

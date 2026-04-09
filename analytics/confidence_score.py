"""Confidence scoring for provider performance conclusions."""
from typing import List

from app.config import CONFIDENCE_WEIGHTS, MIN_COHORT_SIZE, COHORT_WARNING_SIZE


def compute_confidence(
    cohort_n: int,
    current_encounters: int,
    prior_encounters: int,
    missing_fields: List[str] = None,
    is_partial_period: bool = False,
) -> dict:
    """
    Compute a 0-100 confidence score.

    Scoring:
    - Cohort size >= 5: +40 pts | 3-4: +20 pts | <3: +0 pts
    - Encounter volume >= 20 in both periods: +30 pts
    - No missing key fields: +20 pts
    - Full period (not partial month): +10 pts

    Returns score, level (High/Moderate/Low), and plain-language caveats.
    """
    missing_fields = missing_fields or []
    score = 0
    caveats = []

    # Cohort size
    if cohort_n >= COHORT_WARNING_SIZE:
        score += CONFIDENCE_WEIGHTS["cohort_adequate"]
    elif cohort_n >= MIN_COHORT_SIZE:
        score += CONFIDENCE_WEIGHTS["cohort_minimal"]
        caveats.append(
            f"Small peer cohort (n={cohort_n}) — comparison may have limited statistical power."
        )
    else:
        caveats.append(
            f"Very small peer cohort (n={cohort_n}) — adequacy signal suppressed."
        )

    # Volume sufficiency
    if current_encounters >= 20 and prior_encounters >= 20:
        score += CONFIDENCE_WEIGHTS["volume_adequate"]
    elif current_encounters >= 10 or prior_encounters >= 10:
        score += CONFIDENCE_WEIGHTS["volume_adequate"] // 2
        caveats.append(
            "Low encounter volume in one or both periods — results may be less stable."
        )
    else:
        caveats.append(
            "Very low encounter volume — results should be interpreted with caution."
        )

    # Field completeness
    if not missing_fields:
        score += CONFIDENCE_WEIGHTS["fields_complete"]
    else:
        for field in missing_fields:
            caveats.append(f"{field} data not available — that driver analysis is excluded.")

    # Period completeness
    if not is_partial_period:
        score += CONFIDENCE_WEIGHTS["full_period"]
    else:
        caveats.append(
            "Current period is not yet complete — metrics may change as more data arrives."
        )

    level = "High" if score >= 75 else ("Moderate" if score >= 45 else "Low")
    return {"score": score, "level": level, "caveats": caveats}

"""
Narrative generation engine.
Primary: template-based (always works, no API required).
Enrichment: Claude API (if ANTHROPIC_API_KEY set and use_api=True).
"""
import os
from typing import List


def generate_narrative(
    provider_name: str,
    period: str,
    current_wrvu: float,
    prior_wrvu: float,
    baseline_wrvu: float,
    cohort_stats: dict,
    percentile: float,
    drivers: List[dict],
    adequacy: dict,
    confidence: dict,
    use_api: bool = True,
    api_key: str | None = None,
) -> str:
    """
    Generate a 2-6 sentence narrative explanation of provider performance.
    Tries Claude API enrichment first; falls back to template on any failure.
    """
    template = _build_template(
        provider_name, period, current_wrvu, prior_wrvu, baseline_wrvu,
        cohort_stats, percentile, drivers, adequacy, confidence,
    )

    if use_api and (api_key or os.getenv("ANTHROPIC_API_KEY")):
        enriched = _call_claude_api(template, api_key or os.getenv("ANTHROPIC_API_KEY"))
        return enriched if enriched else template
    return template


def _build_template(
    provider_name: str,
    period: str,
    current_wrvu: float,
    prior_wrvu: float,
    baseline_wrvu: float,
    cohort_stats: dict,
    percentile: float,
    drivers: List[dict],
    adequacy: dict,
    confidence: dict,
) -> str:
    """Build a structured 5-part narrative from computed values."""
    from analytics.cohort_engine import _format_period  # local import to avoid circular

    pct_change = ((current_wrvu - prior_wrvu) / prior_wrvu * 100) if prior_wrvu > 0 else 0.0
    direction = "up" if pct_change > 0 else "down"
    abs_pct = abs(pct_change)
    period_label = _format_period(period)
    baseline_delta_pct = (
        (current_wrvu - baseline_wrvu) / baseline_wrvu * 100 if baseline_wrvu > 0 else 0.0
    )

    # 1. Outcome statement
    part1 = (
        f"In {period_label}, {provider_name}'s wRVUs were {current_wrvu:.0f}, "
        f"{direction} {abs_pct:.1f}% from the prior month ({prior_wrvu:.0f}) "
        f"and {baseline_delta_pct:+.1f}% vs. "
        f"the 6-month baseline of {baseline_wrvu:.0f}."
    )

    # 2. Peer comparison
    if cohort_stats["n"] > 0 and adequacy["signal"] != "unavailable":
        part2 = (
            f"Compared to {cohort_stats['n']} peers, performance ranked at the "
            f"{percentile:.0f}th percentile (peer median: {cohort_stats['median']:.0f} wRVU) — "
            f"{adequacy['label']}."
        )
    else:
        part2 = "Peer comparison is not available due to an insufficient peer cohort."

    # 3. Top drivers
    available_drivers = [
        d for d in drivers if d["available"] and abs(d["contribution_wrvu"]) > 0.5
    ]
    top_drivers = available_drivers[:3]
    if top_drivers:
        driver_parts = [
            f"{d['driver_name']} ({d['contribution_wrvu']:+.0f} wRVU)" for d in top_drivers
        ]
        part3 = f"The primary contributors were: {', '.join(driver_parts)}."
    else:
        part3 = "Insufficient data to decompose performance drivers for this period."

    # 4. Confidence and caveats
    caveat_text = (
        confidence["caveats"][0]
        if confidence["caveats"]
        else "No data quality issues detected."
    )
    part4 = f"Confidence: {confidence['level']} ({confidence['score']}/100). {caveat_text}"

    # 5. Recommended actions (based on top driver)
    part5 = _get_action_suggestion(top_drivers, provider_name)

    return " ".join([part1, part2, part3, part4, part5])


def _get_action_suggestion(top_drivers: List[dict], provider_name: str) -> str:
    """Generate a suggested next step based on the top driver category."""
    if not top_drivers:
        return "Review with your department chief to identify opportunities."

    top = top_drivers[0]
    suggestions = {
        "Volume":  "Consider reviewing scheduling capacity and referral patterns to restore encounter volume.",
        "Mix":     "Review documentation practices — ensure complexity and time are fully captured for higher-acuity visits.",
        "Setting": "Evaluate whether site-of-service shifts are clinically appropriate or operationally driven.",
        "Coding":  "A targeted documentation review may be warranted — E&M level selection appears to have shifted downward.",
        "Denials": "An authorization and coding review is recommended to address the elevated denial rate.",
    }
    return suggestions.get(
        top["driver_category"], "Review with your department chief to identify opportunities."
    )


def _call_claude_api(template_text: str, api_key: str) -> str | None:
    """Enrich the template narrative via Claude API. Returns None on any error."""
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        system = (
            "You are a healthcare revenue cycle analyst. You receive a draft narrative "
            "about a physician's performance and must refine it to be clearer, more natural, "
            "and more actionable — without changing the facts, numbers, or recommendations. "
            "Keep it 3-5 sentences. Never suggest upcoding; use 'appropriate documentation' language. "
            "Return only the refined narrative text, no preamble."
        )
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=350,
            system=system,
            messages=[{"role": "user", "content": f"Refine this narrative:\n\n{template_text}"}],
        )
        return response.content[0].text.strip()
    except Exception:
        return None  # Silent fallback


def get_intervention_suggestions(drivers: List[dict], specialty: str) -> List[dict]:
    """
    Generate 2-3 role-tagged intervention suggestions based on top drivers.
    Returns list of dicts: {role, title, rationale, driver_category, contribution_wrvu}
    """
    suggestions = []
    seen_categories: set = set()

    role_map = {
        "Volume": {
            "positive": (
                "Provider",
                "Sustain scheduling and referral momentum",
                "Encounter volume increased — confirm scheduling capacity can maintain this level and protect referral pipelines.",
            ),
            "negative": (
                "Provider",
                "Review scheduling capacity and referral pipeline",
                "Encounter volume declined — check for scheduling gaps or referral routing changes.",
            ),
        },
        "Mix": {
            "positive": (
                "Provider",
                "Reinforce documentation of high-complexity visits",
                "CPT mix shifted toward higher-wRVU codes — ensure documentation supports ongoing complexity, time, and MDM capture.",
            ),
            "negative": (
                "Provider",
                "Review documentation of visit complexity",
                "CPT family mix shifted toward lower-wRVU codes — ensure complexity, time, and MDM are fully documented.",
            ),
        },
        "Setting": {
            "positive": (
                "Ops",
                "Evaluate site-of-service utilization",
                "A favorable shift in site-of-service was detected — confirm this reflects sustainable operational patterns.",
            ),
            "negative": (
                "Ops",
                "Evaluate site-of-service utilization",
                "A shift in site-of-service was detected — confirm whether this reflects patient or operational changes.",
            ),
        },
        "Coding": {
            "positive": (
                "Coding",
                "Maintain E&M documentation standards",
                "E&M level selection improved vs. prior period — sustain documentation practices that support appropriate complexity capture.",
            ),
            "negative": (
                "Coding",
                "Conduct targeted E&M documentation review",
                "E&M level selection trended downward vs. prior period and peers — a targeted audit may identify documentation improvement opportunities.",
            ),
        },
        "Denials": {
            "positive": (
                "Coding",
                "Monitor and sustain denial rate improvement",
                "Denial rate improved this period — review what changed and reinforce those practices to maintain the gains.",
            ),
            "negative": (
                "Coding",
                "Address elevated denial rate",
                "Denial rate increased significantly — review top denial categories for authorization and coding patterns.",
            ),
        },
    }

    available_drivers = [
        d for d in drivers if d["available"] and abs(d["contribution_wrvu"]) > 0.5
    ]
    for driver in available_drivers[:3]:
        cat = driver["driver_category"]
        if cat in seen_categories or cat not in role_map:
            continue
        direction = "positive" if driver["contribution_wrvu"] >= 0 else "negative"
        role, title, rationale = role_map[cat][direction]
        suggestions.append(
            {
                "role":             role,
                "title":            title,
                "rationale":        rationale,
                "driver_category":  cat,
                "contribution_wrvu": driver["contribution_wrvu"],
            }
        )
        seen_categories.add(cat)
        if len(suggestions) >= 3:
            break

    # Ensure at least 2 suggestions
    if len(suggestions) < 2:
        suggestions.append(
            {
                "role":             "Provider",
                "title":            "Schedule a performance review",
                "rationale":        "Discuss period results with department chief to identify context and opportunities.",
                "driver_category":  "General",
                "contribution_wrvu": 0.0,
            }
        )

    return suggestions

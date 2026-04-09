# SPEC_02 — Session 2: Analytics & Narrative Engine
## Meridian Physician Division Provider Performance App (PoC)

> **Claude Code Session 2 of 3.**
> Prerequisite: Session 1 complete. `data/meridian_poc.duckdb` exists and all acceptance tests passed.
> Read `CLAUDE.md` and `ARCHITECTURE.md` before starting.
> Working directory: `APP_Emory PoC v2/`. Activate `.venv` before running any Python.

---

## Session Goal

By the end of this session, the following must exist and all unit tests must pass:

- [ ] `analytics/__init__.py`
- [ ] `analytics/data_layer.py` — all DuckDB query functions
- [ ] `analytics/cohort_engine.py` — cohort selection and benchmarking
- [ ] `analytics/driver_attribution.py` — variance decomposition
- [ ] `analytics/confidence_score.py` — confidence scoring
- [ ] `analytics/adequacy_signal.py` — Green/Yellow/Red signal
- [ ] `analytics/narrative_engine.py` — template + Claude API narratives
- [ ] `app/config.py` — constants and color palette
- [ ] `tests/test_analytics.py` — unit tests for all analytics modules
- [ ] All unit tests pass: `python -m pytest tests/test_analytics.py -v`

---

## Step 1: Create `app/config.py`

```python
"""App-wide constants and configuration."""
from pathlib import Path

# Paths
ROOT_DIR = Path(__file__).parent.parent
DB_PATH = str(ROOT_DIR / "data" / "meridian_poc.duckdb")
DATA_DIR = ROOT_DIR / "data"

# Data settings
DEFAULT_PERIOD = "2024-11"
PRIOR_PERIOD = "2024-10"
BASELINE_MONTHS = 6
ALL_PERIODS = [f"2024-{m:02d}" for m in range(1, 13)]

# Cohort settings
MIN_COHORT_SIZE = 3
COHORT_WARNING_SIZE = 5

# Adequacy thresholds (percentile)
ADEQUACY_THRESHOLDS = {"green": 75.0, "yellow": 25.0}  # >75=green, 25-75=yellow, <25=red

# Confidence score weights
CONFIDENCE_WEIGHTS = {
    "cohort_adequate":    40,   # cohort_n >= 5
    "cohort_minimal":     20,   # cohort_n 3–4
    "volume_adequate":    30,   # >= 20 encounters per period
    "fields_complete":    20,   # no missing key fields
    "full_period":        10,   # not a partial month
}

# Colors
COLORS = {
    "meridian_blue":   "#012169",
    "meridian_gold":   "#F2A900",
    "green":        "#1A7A4A",
    "green_light":  "#D6F0E3",
    "yellow":       "#B45309",
    "yellow_light": "#FEF3C7",
    "red":          "#B71C1C",
    "red_light":    "#FDECEA",
    "gray_dark":    "#444444",
    "gray_light":   "#F5F5F5",
    "white":        "#FFFFFF",
}

ADEQUACY_CONFIG = {
    "green":  {"hex": "#1A7A4A", "label": "On Track",     "bg": "#D6F0E3"},
    "yellow": {"hex": "#B45309", "label": "Watch",         "bg": "#FEF3C7"},
    "red":    {"hex": "#B71C1C", "label": "Below Target",  "bg": "#FDECEA"},
}

DRIVER_COLORS = {
    "Volume":        "#012169",
    "Mix":           "#F2A900",
    "Setting":       "#1A7A4A",
    "Coding":        "#B45309",
    "Denials":       "#B71C1C",
    "Lag":           "#888888",
}
```

---

## Step 2: Create `analytics/__init__.py`

```python
"""Meridian Physician Division — Analytics Layer."""
from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.driver_attribution import compute_drivers
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy
from analytics.narrative_engine import generate_narrative

__all__ = [
    "get_cohort", "get_cohort_stats", "get_percentile",
    "compute_drivers", "compute_confidence", "compute_adequacy",
    "generate_narrative",
]
```

---

## Step 3: Create `analytics/data_layer.py`

This module contains ALL DuckDB queries. No other module queries the database directly.

```python
"""
Data access layer — all DuckDB queries live here.
All functions accept a duckdb.DuckDBPyConnection and return pd.DataFrame or simple values.
"""
import duckdb
import pandas as pd
from pathlib import Path
from app.config import DB_PATH


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Open and return a DuckDB connection."""
    return duckdb.connect(DB_PATH, read_only=read_only)


def load_all_providers(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all providers from dim_provider."""
    return con.execute("SELECT * FROM dim_provider ORDER BY provider_id").df()


def load_provider_month_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all rows from v_provider_month (pre-aggregated monthly KPIs)."""
    return con.execute("SELECT * FROM v_provider_month ORDER BY provider_id, service_month").df()


def load_cpt_mix(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return CPT mix data for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_cpt_mix_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months
    ).df()


def load_pos_mix(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return place-of-service mix for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_pos_mix_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months
    ).df()


def load_denial_summary(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return denial summary for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_denial_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months
    ).df()


def load_em_distribution(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return E&M level distribution for a provider (99212-99215 family only)."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"""
        SELECT service_month, cpt_code, SUM(total_units) AS units, SUM(total_wrvu) AS total_wrvu
        FROM v_cpt_mix_month
        WHERE provider_id = ?
          AND service_month IN ({placeholders})
          AND cpt_code IN ('99212','99213','99214','99215')
        GROUP BY service_month, cpt_code
        ORDER BY service_month, cpt_code
        """,
        [provider_id] + months
    ).df()
```

---

## Step 4: Create `analytics/cohort_engine.py`

Implement the full cohort selection logic with fallback chain.

```python
"""
Cohort engine — peer selection and benchmarking.
All functions are pure (no DB calls). Receive DataFrames, return dicts.
"""
import pandas as pd
import numpy as np
from typing import List
from app.config import MIN_COHORT_SIZE


def get_cohort(
    provider_id: str,
    period: str,
    providers_df: pd.DataFrame,
    provider_month_df: pd.DataFrame
) -> dict:
    """
    Select the best peer cohort for a provider in a given period.
    Fallback chain: specialty + site_type → specialty only → all providers in period.

    Returns:
        peer_ids: list of provider IDs in cohort (excludes the target provider)
        cohort_definition: plain-language description
        cohort_n: number of peers
        fallback_used: bool
        fallback_level: str — 'none' | 'specialty_only' | 'all'
    """
    target = providers_df[providers_df["provider_id"] == provider_id].iloc[0]
    specialty = target["specialty"]
    site_id = target["primary_site_id"]

    # Providers with data in this period
    active_in_period = provider_month_df[provider_month_df["service_month"] == period]["provider_id"].unique()
    pool = providers_df[
        (providers_df["provider_id"].isin(active_in_period)) &
        (providers_df["provider_id"] != provider_id)
    ]

    # Level 1: same specialty + same site
    cohort = pool[
        (pool["specialty"] == specialty) &
        (pool["primary_site_id"] == site_id)
    ]
    if len(cohort) >= MIN_COHORT_SIZE:
        return {
            "peer_ids": cohort["provider_id"].tolist(),
            "cohort_definition": f"{specialty}, {site_id}, {_format_period(period)}",
            "cohort_n": len(cohort),
            "fallback_used": False,
            "fallback_level": "none",
        }

    # Level 2: same specialty, any site
    cohort = pool[pool["specialty"] == specialty]
    if len(cohort) >= MIN_COHORT_SIZE:
        return {
            "peer_ids": cohort["provider_id"].tolist(),
            "cohort_definition": f"{specialty}, All Sites, {_format_period(period)}",
            "cohort_n": len(cohort),
            "fallback_used": True,
            "fallback_level": "specialty_only",
        }

    # Level 3: all providers (last resort)
    return {
        "peer_ids": pool["provider_id"].tolist(),
        "cohort_definition": f"All Specialties, All Sites, {_format_period(period)}",
        "cohort_n": len(pool),
        "fallback_used": True,
        "fallback_level": "all",
    }


def get_cohort_stats(
    peer_ids: List[str],
    period: str,
    provider_month_df: pd.DataFrame,
    kpi: str = "total_wrvu"
) -> dict:
    """Compute distribution statistics for the cohort in a given period."""
    peer_data = provider_month_df[
        (provider_month_df["provider_id"].isin(peer_ids)) &
        (provider_month_df["service_month"] == period)
    ][kpi].dropna()

    if len(peer_data) == 0:
        return {"median": 0, "mean": 0, "p25": 0, "p75": 0, "min": 0, "max": 0, "n": 0}

    return {
        "median": float(np.median(peer_data)),
        "mean":   float(np.mean(peer_data)),
        "p25":    float(np.percentile(peer_data, 25)),
        "p75":    float(np.percentile(peer_data, 75)),
        "min":    float(np.min(peer_data)),
        "max":    float(np.max(peer_data)),
        "n":      int(len(peer_data)),
    }


def get_percentile(value: float, peer_ids: List[str], period: str, provider_month_df: pd.DataFrame, kpi: str = "total_wrvu") -> float:
    """Return the percentile rank (0–100) of value within the peer distribution."""
    peer_values = provider_month_df[
        (provider_month_df["provider_id"].isin(peer_ids)) &
        (provider_month_df["service_month"] == period)
    ][kpi].dropna().values

    if len(peer_values) == 0:
        return 50.0
    return float(np.mean(peer_values <= value) * 100)


def _format_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY' for display."""
    import datetime
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")
```

---

## Step 5: Create `analytics/driver_attribution.py`

Implement variance decomposition. This is the core analytics module.

```python
"""
Driver attribution — decomposes period-over-period wRVU change into ranked drivers.
Uses a shift-share / decomposition approach.
"""
import pandas as pd
import numpy as np
from typing import List


def compute_drivers(
    provider_id: str,
    current_period: str,
    prior_period: str,
    charge_line_df: pd.DataFrame,
    cpt_ref_df: pd.DataFrame,
    pos_df: pd.DataFrame,
    denial_df: pd.DataFrame | None = None,
) -> List[dict]:
    """
    Decompose wRVU change from prior_period to current_period into drivers.

    Decomposition approach:
    1. Volume Effect: change in encounter count × prior average wRVU per encounter
    2. Mix Effect: change in CPT family composition × wRVU delta within each family
    3. Setting Effect: shift between POS codes × wRVU delta between settings
    4. Coding Effect: E&M level shift (upward/downward) weighted by wRVU difference
    5. Denials Effect: change in denial rate × average charge per denied claim (if denial_df provided)

    Returns list of driver dicts sorted by abs(contribution_wrvu) descending.
    """
    # Filter to provider and periods
    current = charge_line_df[
        (charge_line_df["provider_id"] == provider_id) &
        (charge_line_df["service_month"] == current_period)
    ]
    prior = charge_line_df[
        (charge_line_df["provider_id"] == provider_id) &
        (charge_line_df["service_month"] == prior_period)
    ]

    current_wrvu = current["total_wrvu"].sum()
    prior_wrvu = prior["total_wrvu"].sum()
    total_change = current_wrvu - prior_wrvu

    drivers = []

    # ── 1. Volume Effect ──────────────────────────────────────────────────────
    current_enc = current["encounter_id"].nunique() if "encounter_id" in current.columns else len(current)
    prior_enc   = prior["encounter_id"].nunique()   if "encounter_id" in prior.columns   else len(prior)
    prior_wrvu_per_enc = (prior_wrvu / prior_enc) if prior_enc > 0 else 0
    volume_effect = (current_enc - prior_enc) * prior_wrvu_per_enc

    drivers.append(_make_driver("Volume Effect", "Volume", volume_effect, total_change))

    # ── 2. Mix Effect (CPT family) ────────────────────────────────────────────
    # Compare CPT family distribution weighted by wRVU values
    current_fam = current.groupby("cpt_family")["total_wrvu"].sum()
    prior_fam   = prior.groupby("cpt_family")["total_wrvu"].sum()
    all_families = set(current_fam.index) | set(prior_fam.index)

    mix_effect = 0.0
    for fam in all_families:
        c_wrvu = current_fam.get(fam, 0.0)
        p_wrvu = prior_fam.get(fam, 0.0)
        # Normalize by volume to isolate mix (remove pure volume component)
        c_share = c_wrvu / current_wrvu if current_wrvu > 0 else 0
        p_share = p_wrvu / prior_wrvu  if prior_wrvu > 0 else 0
        mix_effect += (c_share - p_share) * prior_wrvu

    drivers.append(_make_driver("Mix Effect (CPT Family)", "Mix", mix_effect, total_change))

    # ── 3. Setting Effect (POS) ───────────────────────────────────────────────
    if pos_df is not None and not pos_df.empty:
        current_pos = pos_df[pos_df["service_month"] == current_period][pos_df["provider_id"] == provider_id]
        prior_pos   = pos_df[pos_df["service_month"] == prior_period][pos_df["provider_id"] == provider_id]

        c_pos_wrvu = current_pos.groupby("pos_code")["total_wrvu"].sum()
        p_pos_wrvu = prior_pos.groupby("pos_code")["total_wrvu"].sum()
        all_pos = set(c_pos_wrvu.index) | set(p_pos_wrvu.index)

        setting_effect = 0.0
        for pos in all_pos:
            c_share = (c_pos_wrvu.get(pos, 0) / current_wrvu) if current_wrvu > 0 else 0
            p_share = (p_pos_wrvu.get(pos, 0) / prior_wrvu) if prior_wrvu > 0 else 0
            setting_effect += (c_share - p_share) * prior_wrvu

        drivers.append(_make_driver("Setting Effect (Site-of-Service)", "Setting", setting_effect, total_change))

    # ── 4. Coding Effect (E&M level shift) ───────────────────────────────────
    em_codes = ["99212", "99213", "99214", "99215"]
    em_wrvu  = {"99212": 0.70, "99213": 1.30, "99214": 1.92, "99215": 2.80}

    c_em = current[current["cpt_code"].isin(em_codes)].groupby("cpt_code")["units"].sum()
    p_em = prior[prior["cpt_code"].isin(em_codes)].groupby("cpt_code")["units"].sum()
    c_em_total = c_em.sum()
    p_em_total = p_em.sum()

    coding_effect = 0.0
    if c_em_total > 0 and p_em_total > 0:
        c_avg_wrvu = sum(em_wrvu.get(k, 0) * c_em.get(k, 0) for k in em_codes) / c_em_total
        p_avg_wrvu = sum(em_wrvu.get(k, 0) * p_em.get(k, 0) for k in em_codes) / p_em_total
        coding_effect = (c_avg_wrvu - p_avg_wrvu) * c_em_total

    drivers.append(_make_driver("E&M Level Shift", "Coding", coding_effect, total_change))

    # ── 5. Denials Effect ─────────────────────────────────────────────────────
    if denial_df is not None and not denial_df.empty:
        c_denials = denial_df[
            (denial_df["provider_id"] == provider_id) &
            (denial_df["service_month"] == current_period)
        ]["denial_count"].sum()
        p_denials = denial_df[
            (denial_df["provider_id"] == provider_id) &
            (denial_df["service_month"] == prior_period)
        ]["denial_count"].sum()

        # Rough wRVU impact: assume average denied claim = 1.5 wRVU
        avg_denied_wrvu = 1.5
        denial_effect = -(c_denials - p_denials) * avg_denied_wrvu

        drivers.append(_make_driver("Denial Rate Change", "Denials", denial_effect, total_change,
                                     available=True))
    else:
        drivers.append(_make_driver("Denial Rate Change", "Denials", 0.0, total_change, available=False))

    # Sort by absolute contribution, descending; unavailable drivers go last
    drivers.sort(key=lambda d: (not d["available"], -abs(d["contribution_wrvu"])))
    return drivers


def _make_driver(name: str, category: str, contribution: float, total_change: float, available: bool = True) -> dict:
    pct = (abs(contribution) / abs(total_change) * 100) if total_change != 0 else 0.0
    return {
        "driver_name":       name,
        "driver_category":   category,
        "contribution_wrvu": round(contribution, 2),
        "contribution_pct":  round(pct, 1),
        "direction":         "increase" if contribution > 0.5 else ("decrease" if contribution < -0.5 else "neutral"),
        "available":         available,
    }
```

---

## Step 6: Create `analytics/confidence_score.py`

```python
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
    Compute a 0–100 confidence score.

    Scoring:
    - Cohort size >= 5: +40 pts | 3–4: +20 pts | <3: +0 pts
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
        caveats.append(f"Small peer cohort (n={cohort_n}) — comparison may have limited statistical power.")
    else:
        caveats.append(f"Very small peer cohort (n={cohort_n}) — adequacy signal suppressed.")

    # Volume sufficiency
    if current_encounters >= 20 and prior_encounters >= 20:
        score += CONFIDENCE_WEIGHTS["volume_adequate"]
    elif current_encounters >= 10 or prior_encounters >= 10:
        score += CONFIDENCE_WEIGHTS["volume_adequate"] // 2
        caveats.append("Low encounter volume in one or both periods — results may be less stable.")
    else:
        caveats.append("Very low encounter volume — results should be interpreted with caution.")

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
        caveats.append("Current period is not yet complete — metrics may change as more data arrives.")

    level = "High" if score >= 75 else ("Moderate" if score >= 45 else "Low")
    return {"score": score, "level": level, "caveats": caveats}
```

---

## Step 7: Create `analytics/adequacy_signal.py`

```python
"""Adequacy signal computation (Green / Yellow / Red)."""
from app.config import ADEQUACY_THRESHOLDS, ADEQUACY_CONFIG


def compute_adequacy(percentile: float, confidence_score: int, cohort_n: int) -> dict:
    """
    Compute adequacy signal based on percentile rank and confidence.

    Rules:
    - > 75th percentile → Green ("On Track")
    - 25th–75th percentile → Yellow ("Watch")
    - < 25th percentile → Red ("Below Target")
    - If confidence < 40 OR cohort_n < 3: downgrade one level and note it
    - If cohort_n < 3: signal is 'unavailable'

    Returns signal, color_hex, label, bg_color, rationale.
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
        rationale = f"Performance ranks above the 75th percentile of peers ({percentile:.0f}th percentile)."
    elif percentile >= ADEQUACY_THRESHOLDS["yellow"]:
        raw_signal = "yellow"
        rationale = f"Performance is within the middle range of peers ({percentile:.0f}th percentile)."
    else:
        raw_signal = "red"
        rationale = f"Performance ranks below the 25th percentile of peers ({percentile:.0f}th percentile)."

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
```

---

## Step 8: Create `analytics/narrative_engine.py`

```python
"""
Narrative generation engine.
Primary: template-based (always works, no API required).
Enrichment: Claude API (if ANTHROPIC_API_KEY set and use_api=True).
"""
import os
from typing import List
from app.config import COLORS


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
    Generate a 2–6 sentence narrative explanation of provider performance.
    Tries Claude API enrichment first; falls back to template on any failure.
    """
    template = _build_template(
        provider_name, period, current_wrvu, prior_wrvu, baseline_wrvu,
        cohort_stats, percentile, drivers, adequacy, confidence
    )

    if use_api and (api_key or os.getenv("ANTHROPIC_API_KEY")):
        enriched = _call_claude_api(template, api_key or os.getenv("ANTHROPIC_API_KEY"))
        return enriched if enriched else template
    return template


def _build_template(
    provider_name, period, current_wrvu, prior_wrvu, baseline_wrvu,
    cohort_stats, percentile, drivers, adequacy, confidence
) -> str:
    """Build a structured 5-part narrative from computed values."""
    from analytics.cohort_engine import _format_period  # local import to avoid circular

    pct_change = ((current_wrvu - prior_wrvu) / prior_wrvu * 100) if prior_wrvu > 0 else 0
    direction = "up" if pct_change > 0 else "down"
    abs_pct = abs(pct_change)
    period_label = _format_period(period)

    # 1. Outcome statement
    part1 = (
        f"In {period_label}, {provider_name}'s wRVUs were {current_wrvu:.0f}, "
        f"{direction} {abs_pct:.1f}% from the prior month ({prior_wrvu:.0f}) "
        f"and {((current_wrvu - baseline_wrvu) / baseline_wrvu * 100):+.1f}% vs. "
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
    available_drivers = [d for d in drivers if d["available"] and abs(d["contribution_wrvu"]) > 0.5]
    top_drivers = available_drivers[:3]
    if top_drivers:
        driver_parts = [
            f"{d['driver_name']} ({d['contribution_wrvu']:+.0f} wRVU)"
            for d in top_drivers
        ]
        part3 = f"The primary contributors were: {', '.join(driver_parts)}."
    else:
        part3 = "Insufficient data to decompose performance drivers for this period."

    # 4. Confidence and caveats
    caveat_text = confidence["caveats"][0] if confidence["caveats"] else "No data quality issues detected."
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
        "Volume": "Consider reviewing scheduling capacity and referral patterns to restore encounter volume.",
        "Mix":    "Review documentation practices — ensure complexity and time are fully captured for higher-acuity visits.",
        "Setting": "Evaluate whether site-of-service shifts are clinically appropriate or operationally driven.",
        "Coding":  "A targeted documentation review may be warranted — E&M level selection appears to have shifted downward.",
        "Denials": "An authorization and coding review is recommended to address the elevated denial rate.",
    }
    return suggestions.get(top["driver_category"], "Review with your department chief to identify opportunities.")


def _call_claude_api(template_text: str, api_key: str) -> str | None:
    """Enrich the template narrative via Claude API. Returns None on any error."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        system = (
            "You are a healthcare revenue cycle analyst. You receive a draft narrative "
            "about a physician's performance and must refine it to be clearer, more natural, "
            "and more actionable — without changing the facts, numbers, or recommendations. "
            "Keep it 3–5 sentences. Never suggest upcoding; use 'appropriate documentation' language. "
            "Return only the refined narrative text, no preamble."
        )
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=350,
            system=system,
            messages=[{"role": "user", "content": f"Refine this narrative:\n\n{template_text}"}]
        )
        return response.content[0].text.strip()
    except Exception:
        return None  # Silent fallback


def get_intervention_suggestions(drivers: List[dict], specialty: str) -> List[dict]:
    """
    Generate 2–3 role-tagged intervention suggestions based on top drivers.
    Returns list of dicts: {role, title, rationale, driver_category}
    """
    suggestions = []
    seen_categories = set()

    role_map = {
        "Volume":  ("Provider", "Review scheduling capacity and referral pipeline",
                    "Encounter volume declined — check for scheduling gaps or referral routing changes."),
        "Mix":     ("Provider", "Review documentation of visit complexity",
                    "CPT family mix shifted toward lower-wRVU codes — ensure complexity, time, and MDM are fully documented."),
        "Setting": ("Ops",      "Evaluate site-of-service utilization",
                    "A shift in site-of-service was detected — confirm whether this reflects patient or operational changes."),
        "Coding":  ("Coding",   "Conduct targeted E&M documentation review",
                    "E&M level selection trended downward vs. prior period and peers — a targeted audit may identify documentation improvement opportunities."),
        "Denials": ("Coding",   "Address elevated denial rate",
                    "Denial rate increased significantly — review top denial categories for authorization and coding patterns."),
    }

    available_drivers = [d for d in drivers if d["available"] and abs(d["contribution_wrvu"]) > 0.5]
    for driver in available_drivers[:3]:
        cat = driver["driver_category"]
        if cat in seen_categories or cat not in role_map:
            continue
        role, title, rationale = role_map[cat]
        suggestions.append({
            "role":             role,
            "title":            title,
            "rationale":        rationale,
            "driver_category":  cat,
            "contribution_wrvu": driver["contribution_wrvu"],
        })
        seen_categories.add(cat)
        if len(suggestions) >= 3:
            break

    # Ensure at least 2 suggestions
    if len(suggestions) < 2:
        suggestions.append({
            "role":             "Provider",
            "title":            "Schedule a performance review",
            "rationale":        "Discuss period results with department chief to identify context and opportunities.",
            "driver_category":  "General",
            "contribution_wrvu": 0.0,
        })

    return suggestions
```

---

## Step 9: Create `tests/test_analytics.py`

Write comprehensive unit tests for all analytics modules. Tests must cover:

```python
"""Unit tests for analytics layer. Run with: python -m pytest tests/test_analytics.py -v"""
import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.driver_attribution import compute_drivers
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy
from analytics.narrative_engine import generate_narrative, get_intervention_suggestions

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_providers():
    """10 mock providers — 5 Internal Medicine, 5 Cardiology."""
    return pd.DataFrame([
        {"provider_id": f"P{i:03d}", "specialty": "Internal Medicine" if i <= 5 else "Cardiology",
         "primary_site_id": "SITE_01" if i <= 3 else "SITE_02", "active": True}
        for i in range(1, 11)
    ])

@pytest.fixture
def sample_provider_month(sample_providers):
    """Monthly wRVU data for all 10 providers, 2 months."""
    rows = []
    for _, p in sample_providers.iterrows():
        base = 350.0 if p["specialty"] == "Internal Medicine" else 450.0
        for month in ["2024-10", "2024-11"]:
            rows.append({
                "provider_id": p["provider_id"],
                "service_month": month,
                "specialty": p["specialty"],
                "primary_site_id": p["primary_site_id"],
                "total_wrvu": base + np.random.normal(0, 20),
                "encounter_count": 130,
            })
    return pd.DataFrame(rows)

# ── Cohort Engine Tests ────────────────────────────────────────────────────────

def test_cohort_returns_same_specialty(sample_providers, sample_provider_month):
    result = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    # All peers should be Internal Medicine
    peer_specialties = sample_providers[sample_providers["provider_id"].isin(result["peer_ids"])]["specialty"].unique()
    assert "Cardiology" not in peer_specialties or result["fallback_used"]

def test_cohort_excludes_target_provider(sample_providers, sample_provider_month):
    result = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    assert "P001" not in result["peer_ids"]

def test_cohort_n_matches_peer_ids(sample_providers, sample_provider_month):
    result = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    assert result["cohort_n"] == len(result["peer_ids"])

def test_cohort_stats_returns_expected_keys(sample_providers, sample_provider_month):
    cohort = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    stats = get_cohort_stats(cohort["peer_ids"], "2024-11", sample_provider_month)
    for key in ["median", "mean", "p25", "p75", "min", "max", "n"]:
        assert key in stats

def test_percentile_midpoint(sample_providers, sample_provider_month):
    """A value equal to the median should be near the 50th percentile."""
    cohort = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    stats = get_cohort_stats(cohort["peer_ids"], "2024-11", sample_provider_month)
    p = get_percentile(stats["median"], cohort["peer_ids"], "2024-11", sample_provider_month)
    assert 30 <= p <= 70, f"Median value should be near 50th percentile, got {p:.1f}"

# ── Confidence Score Tests ─────────────────────────────────────────────────────

def test_high_confidence_full_data():
    result = compute_confidence(cohort_n=8, current_encounters=50, prior_encounters=50)
    assert result["score"] == 100
    assert result["level"] == "High"

def test_low_confidence_small_cohort():
    result = compute_confidence(cohort_n=1, current_encounters=5, prior_encounters=5, is_partial_period=True)
    assert result["score"] < 40
    assert result["level"] == "Low"

def test_confidence_caveats_for_missing_fields():
    result = compute_confidence(cohort_n=6, current_encounters=30, prior_encounters=30,
                                 missing_fields=["denial_data"])
    assert any("denial" in c.lower() for c in result["caveats"])

def test_confidence_partial_period_caveat():
    result = compute_confidence(cohort_n=6, current_encounters=30, prior_encounters=30,
                                 is_partial_period=True)
    assert any("partial" in c.lower() or "complete" in c.lower() for c in result["caveats"])

# ── Adequacy Signal Tests ─────────────────────────────────────────────────────

def test_adequacy_green_high_percentile():
    result = compute_adequacy(percentile=85.0, confidence_score=90, cohort_n=8)
    assert result["signal"] == "green"

def test_adequacy_red_low_percentile():
    result = compute_adequacy(percentile=15.0, confidence_score=90, cohort_n=8)
    assert result["signal"] == "red"

def test_adequacy_yellow_middle():
    result = compute_adequacy(percentile=50.0, confidence_score=90, cohort_n=8)
    assert result["signal"] == "yellow"

def test_adequacy_downgrade_low_confidence():
    # Green percentile but low confidence → should downgrade to yellow
    result = compute_adequacy(percentile=80.0, confidence_score=30, cohort_n=5)
    assert result["signal"] in ("yellow", "red")

def test_adequacy_unavailable_tiny_cohort():
    result = compute_adequacy(percentile=80.0, confidence_score=90, cohort_n=2)
    assert result["signal"] == "unavailable"

# ── Narrative Tests ────────────────────────────────────────────────────────────

@pytest.fixture
def sample_narrative_inputs():
    return {
        "provider_name": "Chen, Sarah",
        "period": "2024-11",
        "current_wrvu": 380.0,
        "prior_wrvu": 442.0,
        "baseline_wrvu": 430.0,
        "cohort_stats": {"median": 420.0, "mean": 418.0, "p25": 380.0, "p75": 460.0, "n": 8},
        "percentile": 34.0,
        "drivers": [
            {"driver_name": "Mix Effect (CPT Family)", "driver_category": "Mix",
             "contribution_wrvu": -38.0, "contribution_pct": 61.3, "direction": "decrease", "available": True},
            {"driver_name": "Volume Effect", "driver_category": "Volume",
             "contribution_wrvu": -24.0, "contribution_pct": 38.7, "direction": "decrease", "available": True},
        ],
        "adequacy": {"signal": "yellow", "color_hex": "#B45309", "label": "Watch",
                     "bg": "#FEF3C7", "rationale": "34th percentile vs peers."},
        "confidence": {"score": 80, "level": "High", "caveats": []},
        "use_api": False,
    }

def test_narrative_returns_string(sample_narrative_inputs):
    result = generate_narrative(**sample_narrative_inputs)
    assert isinstance(result, str)
    assert len(result) > 50

def test_narrative_contains_wrvu_value(sample_narrative_inputs):
    result = generate_narrative(**sample_narrative_inputs)
    assert "380" in result

def test_narrative_contains_provider_name(sample_narrative_inputs):
    result = generate_narrative(**sample_narrative_inputs)
    assert "Chen" in result

def test_narrative_no_api_fallback(sample_narrative_inputs):
    """Narrative should succeed with use_api=False even without a key."""
    inputs = {**sample_narrative_inputs, "use_api": False, "api_key": None}
    result = generate_narrative(**inputs)
    assert len(result) > 50

def test_intervention_suggestions_count(sample_narrative_inputs):
    suggestions = get_intervention_suggestions(sample_narrative_inputs["drivers"], "Cardiology")
    assert 2 <= len(suggestions) <= 3

def test_intervention_suggestions_have_required_keys(sample_narrative_inputs):
    suggestions = get_intervention_suggestions(sample_narrative_inputs["drivers"], "Cardiology")
    for s in suggestions:
        assert "role" in s
        assert "title" in s
        assert "rationale" in s
```

---

## Step 10: Run All Tests

```powershell
python -m pytest tests/test_analytics.py -v
```

All tests must pass before closing this session. If any test fails, fix the implementation — do not skip or modify the test.

---

## Acceptance Tests

1. All pytest tests pass (0 failures)
2. `from analytics import get_cohort, compute_drivers, generate_narrative` succeeds with no errors
3. Running the analytics pipeline manually for provider P007, period 2024-11 produces:
   - A cohort dict with `cohort_n >= 3`
   - A drivers list with at least 2 available drivers
   - A confidence dict with `score > 0`
   - A narrative string of at least 100 characters

Verify with:
```python
import duckdb
import sys
sys.path.insert(0, ".")
from analytics.data_layer import get_connection, load_all_providers, load_provider_month_summary, load_cpt_mix, load_pos_mix, load_denial_summary
from analytics import get_cohort, get_cohort_stats, get_percentile, compute_drivers, compute_confidence, compute_adequacy, generate_narrative

con = get_connection()
providers = load_all_providers(con)
pm = load_provider_month_summary(con)
charges = con.execute("SELECT * FROM fact_charge_line").df()
pos = load_pos_mix(con, "P007", ["2024-10","2024-11"])
denials = load_denial_summary(con, "P007", ["2024-10","2024-11"])

cohort = get_cohort("P007", "2024-11", providers, pm)
stats = get_cohort_stats(cohort["peer_ids"], "2024-11", pm)
pctile = get_percentile(
    pm[(pm.provider_id=="P007") & (pm.service_month=="2024-11")]["total_wrvu"].values[0],
    cohort["peer_ids"], "2024-11", pm
)
drivers = compute_drivers("P007", "2024-11", "2024-10", charges,
    con.execute("SELECT * FROM dim_cpt").df(), pos, denials)
conf = compute_confidence(cohort["cohort_n"], 130, 130)
adeq = compute_adequacy(pctile, conf["score"], cohort["cohort_n"])
narrative = generate_narrative(
    "Smith, John", "2024-11",
    pm[(pm.provider_id=="P007") & (pm.service_month=="2024-11")]["total_wrvu"].values[0],
    pm[(pm.provider_id=="P007") & (pm.service_month=="2024-10")]["total_wrvu"].values[0],
    pm[(pm.provider_id=="P007") & (pm.service_month.isin(["2024-05","2024-06","2024-07","2024-08","2024-09","2024-10"]))]["total_wrvu"].mean(),
    stats, pctile, drivers, adeq, conf, use_api=False
)

print(f"Cohort n: {cohort['cohort_n']}")
print(f"Percentile: {pctile:.1f}")
print(f"Confidence: {conf['score']}")
print(f"Adequacy: {adeq['signal']}")
print(f"Drivers: {len([d for d in drivers if d['available']])}")
print(f"Narrative ({len(narrative)} chars):\n{narrative}")
```

---

## End of Session 2

When all tests pass, report:
- Test results summary
- Output of the manual pipeline verification above
- Any implementation decisions made that deviate from this spec

Then stop. Do not proceed to Session 3.

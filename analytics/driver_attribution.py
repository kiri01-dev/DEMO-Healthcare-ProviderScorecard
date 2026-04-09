"""
Driver attribution — decomposes period-over-period wRVU change into ranked drivers.
Uses a shift-share / decomposition approach.
All functions are pure (no DB calls).
"""
from typing import List

import pandas as pd


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

    Decomposition:
    1. Volume Effect: change in encounter count * prior avg wRVU per encounter
    2. Mix Effect: CPT family share shift weighted by prior wRVU
    3. Setting Effect: POS code share shift weighted by prior wRVU
    4. Coding Effect: E&M level shift (avg wRVU per E&M visit) * current E&M volume
    5. Denials Effect: change in denial count * avg denied wRVU (if denial_df provided)

    Returns list of driver dicts sorted by abs(contribution_wrvu) descending.
    """
    current = charge_line_df[
        (charge_line_df["provider_id"] == provider_id)
        & (charge_line_df["service_month"] == current_period)
    ]
    prior = charge_line_df[
        (charge_line_df["provider_id"] == provider_id)
        & (charge_line_df["service_month"] == prior_period)
    ]

    current_wrvu = float(current["total_wrvu"].sum())
    prior_wrvu = float(prior["total_wrvu"].sum())
    total_change = current_wrvu - prior_wrvu

    drivers = []

    # ── 1. Volume Effect ──────────────────────────────────────────────────────
    current_enc = (
        current["encounter_id"].nunique()
        if "encounter_id" in current.columns
        else len(current)
    )
    prior_enc = (
        prior["encounter_id"].nunique()
        if "encounter_id" in prior.columns
        else len(prior)
    )
    prior_wrvu_per_enc = (prior_wrvu / prior_enc) if prior_enc > 0 else 0.0
    volume_effect = (current_enc - prior_enc) * prior_wrvu_per_enc

    drivers.append(_make_driver("Volume Effect", "Volume", volume_effect, total_change))

    # ── 2. Mix Effect (CPT family) ────────────────────────────────────────────
    current_fam = current.groupby("cpt_family")["total_wrvu"].sum()
    prior_fam = prior.groupby("cpt_family")["total_wrvu"].sum()
    all_families = set(current_fam.index) | set(prior_fam.index)

    mix_effect = 0.0
    for fam in all_families:
        c_wrvu = float(current_fam.get(fam, 0.0))
        p_wrvu = float(prior_fam.get(fam, 0.0))
        c_share = c_wrvu / current_wrvu if current_wrvu > 0 else 0.0
        p_share = p_wrvu / prior_wrvu if prior_wrvu > 0 else 0.0
        mix_effect += (c_share - p_share) * prior_wrvu

    drivers.append(_make_driver("Mix Effect (CPT Family)", "Mix", mix_effect, total_change))

    # ── 3. Setting Effect (POS) ───────────────────────────────────────────────
    if pos_df is not None and not pos_df.empty:
        current_pos = pos_df[
            (pos_df["service_month"] == current_period)
            & (pos_df["provider_id"] == provider_id)
        ]
        prior_pos = pos_df[
            (pos_df["service_month"] == prior_period)
            & (pos_df["provider_id"] == provider_id)
        ]

        c_pos_wrvu = current_pos.groupby("pos_code")["total_wrvu"].sum()
        p_pos_wrvu = prior_pos.groupby("pos_code")["total_wrvu"].sum()
        all_pos = set(c_pos_wrvu.index) | set(p_pos_wrvu.index)

        setting_effect = 0.0
        for pos in all_pos:
            c_share = (float(c_pos_wrvu.get(pos, 0)) / current_wrvu) if current_wrvu > 0 else 0.0
            p_share = (float(p_pos_wrvu.get(pos, 0)) / prior_wrvu) if prior_wrvu > 0 else 0.0
            setting_effect += (c_share - p_share) * prior_wrvu

        drivers.append(
            _make_driver("Setting Effect (Site-of-Service)", "Setting", setting_effect, total_change)
        )

    # ── 4. Coding Effect (E&M level shift) ───────────────────────────────────
    em_codes = ["99212", "99213", "99214", "99215"]
    em_wrvu = {"99212": 0.70, "99213": 1.30, "99214": 1.92, "99215": 2.80}

    c_em = current[current["cpt_code"].isin(em_codes)].groupby("cpt_code")["units"].sum()
    p_em = prior[prior["cpt_code"].isin(em_codes)].groupby("cpt_code")["units"].sum()
    c_em_total = float(c_em.sum())
    p_em_total = float(p_em.sum())

    coding_effect = 0.0
    if c_em_total > 0 and p_em_total > 0:
        c_avg_wrvu = sum(em_wrvu.get(k, 0) * float(c_em.get(k, 0)) for k in em_codes) / c_em_total
        p_avg_wrvu = sum(em_wrvu.get(k, 0) * float(p_em.get(k, 0)) for k in em_codes) / p_em_total
        coding_effect = (c_avg_wrvu - p_avg_wrvu) * c_em_total

    drivers.append(_make_driver("E&M Level Shift", "Coding", coding_effect, total_change))

    # ── 5. Denials Effect ─────────────────────────────────────────────────────
    if denial_df is not None and not denial_df.empty:
        c_denials = float(
            denial_df[
                (denial_df["provider_id"] == provider_id)
                & (denial_df["service_month"] == current_period)
            ]["denial_count"].sum()
        )
        p_denials = float(
            denial_df[
                (denial_df["provider_id"] == provider_id)
                & (denial_df["service_month"] == prior_period)
            ]["denial_count"].sum()
        )
        avg_denied_wrvu = 1.5
        denial_effect = -(c_denials - p_denials) * avg_denied_wrvu
        drivers.append(
            _make_driver("Denial Rate Change", "Denials", denial_effect, total_change, available=True)
        )
    else:
        drivers.append(
            _make_driver("Denial Rate Change", "Denials", 0.0, total_change, available=False)
        )

    # Sort: available drivers first, then by |contribution| descending
    drivers.sort(key=lambda d: (not d["available"], -abs(d["contribution_wrvu"])))
    return drivers


def _make_driver(
    name: str,
    category: str,
    contribution: float,
    total_change: float,
    available: bool = True,
) -> dict:
    """Build a driver dict with standardized fields."""
    pct = (abs(contribution) / abs(total_change) * 100) if total_change != 0 else 0.0
    return {
        "driver_name":       name,
        "driver_category":   category,
        "contribution_wrvu": round(contribution, 2),
        "contribution_pct":  round(pct, 1),
        "direction":         (
            "increase" if contribution > 0.5
            else ("decrease" if contribution < -0.5 else "neutral")
        ),
        "available":         available,
    }

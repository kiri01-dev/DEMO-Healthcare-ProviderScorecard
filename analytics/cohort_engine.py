"""
Cohort engine — peer selection and benchmarking.
All functions are pure (no DB calls). Receive DataFrames, return dicts.
"""
import datetime
from typing import List

import numpy as np
import pandas as pd

from app.config import MIN_COHORT_SIZE


def get_cohort(
    provider_id: str,
    period: str,
    providers_df: pd.DataFrame,
    provider_month_df: pd.DataFrame,
) -> dict:
    """
    Select the best peer cohort for a provider in a given period.
    Fallback chain: specialty + site → specialty only → all providers.

    Returns:
        peer_ids: list of provider IDs in cohort (excludes the target provider)
        cohort_definition: plain-language description
        cohort_n: number of peers
        fallback_used: bool
        fallback_level: 'none' | 'specialty_only' | 'all'
    """
    target = providers_df[providers_df["provider_id"] == provider_id].iloc[0]
    specialty = target["specialty"]
    site_id = target["primary_site_id"]

    # Providers with data in this period
    active_in_period = provider_month_df[
        provider_month_df["service_month"] == period
    ]["provider_id"].unique()
    pool = providers_df[
        (providers_df["provider_id"].isin(active_in_period))
        & (providers_df["provider_id"] != provider_id)
    ]

    # Level 1: same specialty + same site
    cohort = pool[
        (pool["specialty"] == specialty) & (pool["primary_site_id"] == site_id)
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
    kpi: str = "total_wrvu",
) -> dict:
    """Compute distribution statistics for the cohort in a given period."""
    peer_data = provider_month_df[
        (provider_month_df["provider_id"].isin(peer_ids))
        & (provider_month_df["service_month"] == period)
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


def get_percentile(
    value: float,
    peer_ids: List[str],
    period: str,
    provider_month_df: pd.DataFrame,
    kpi: str = "total_wrvu",
) -> float:
    """Return the percentile rank (0–100) of value within the peer distribution."""
    peer_values = provider_month_df[
        (provider_month_df["provider_id"].isin(peer_ids))
        & (provider_month_df["service_month"] == period)
    ][kpi].dropna().values

    if len(peer_values) == 0:
        return 50.0
    return float(np.mean(peer_values <= value) * 100)


def _format_period(period: str) -> str:
    """Convert 'YYYY-MM' to 'Mon YYYY' for display."""
    dt = datetime.datetime.strptime(period, "%Y-%m")
    return dt.strftime("%b %Y")

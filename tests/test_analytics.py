"""Unit tests for analytics layer. Run with: python -m pytest tests/test_analytics.py -v"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.cohort_engine import get_cohort, get_cohort_stats, get_percentile
from analytics.confidence_score import compute_confidence
from analytics.adequacy_signal import compute_adequacy
from analytics.driver_attribution import compute_drivers
from analytics.narrative_engine import generate_narrative, get_intervention_suggestions


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_providers():
    """10 mock providers — 5 Internal Medicine, 5 Cardiology."""
    return pd.DataFrame([
        {
            "provider_id":    f"P{i:03d}",
            "specialty":      "Internal Medicine" if i <= 5 else "Cardiology",
            "primary_site_id": "SITE_01" if i <= 3 else "SITE_02",
            "active":         True,
        }
        for i in range(1, 11)
    ])


@pytest.fixture
def sample_provider_month(sample_providers):
    """Monthly wRVU data for all 10 providers, 2 months."""
    rng = np.random.default_rng(42)
    rows = []
    for _, p in sample_providers.iterrows():
        base = 350.0 if p["specialty"] == "Internal Medicine" else 450.0
        for month in ["2024-10", "2024-11"]:
            rows.append({
                "provider_id":    p["provider_id"],
                "service_month":  month,
                "specialty":      p["specialty"],
                "primary_site_id": p["primary_site_id"],
                "total_wrvu":     base + float(rng.normal(0, 20)),
                "encounter_count": 130,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_charge_lines():
    """Minimal charge line data for 2 providers, 2 months."""
    rows = []
    cpt_mix = [
        ("99214", "E&M",  1.92, 0.40),
        ("99213", "E&M",  1.30, 0.25),
        ("99215", "E&M",  2.80, 0.15),
        ("99395", "Preventive", 1.50, 0.10),
        ("93000", "Procedures", 0.17, 0.10),
    ]
    for pid in ["P001", "P002"]:
        for month in ["2024-10", "2024-11"]:
            for i in range(50):
                cpt, fam, wrvu, _ = cpt_mix[i % len(cpt_mix)]
                rows.append({
                    "charge_id":    f"CHG_{pid}_{month}_{i:04d}",
                    "encounter_id": f"ENC_{pid}_{month}_{i:04d}",
                    "provider_id":  pid,
                    "service_month": month,
                    "cpt_code":     cpt,
                    "cpt_family":   fam,
                    "units":        1,
                    "wrvu_per_unit": wrvu,
                    "total_wrvu":   wrvu,
                    "charge_amount": wrvu * 52.0,
                    "pos_code":     "11",
                })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_pos_df():
    """POS mix data for 2 providers, 2 months."""
    rows = []
    for pid in ["P001", "P002"]:
        for month in ["2024-10", "2024-11"]:
            rows.append({
                "provider_id":    pid,
                "service_month":  month,
                "pos_code":       "11",
                "pos_label":      "Office",
                "encounter_count": 50,
                "total_wrvu":     90.0,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_narrative_inputs():
    return {
        "provider_name": "Chen, Sarah",
        "period":        "2024-11",
        "current_wrvu":  380.0,
        "prior_wrvu":    442.0,
        "baseline_wrvu": 430.0,
        "cohort_stats":  {
            "median": 420.0, "mean": 418.0, "p25": 380.0,
            "p75": 460.0, "n": 8,
        },
        "percentile": 34.0,
        "drivers": [
            {
                "driver_name": "Mix Effect (CPT Family)", "driver_category": "Mix",
                "contribution_wrvu": -38.0, "contribution_pct": 61.3,
                "direction": "decrease", "available": True,
            },
            {
                "driver_name": "Volume Effect", "driver_category": "Volume",
                "contribution_wrvu": -24.0, "contribution_pct": 38.7,
                "direction": "decrease", "available": True,
            },
        ],
        "adequacy": {
            "signal": "yellow", "color_hex": "#B45309", "label": "Watch",
            "bg": "#FEF3C7", "rationale": "34th percentile vs peers.",
        },
        "confidence": {"score": 80, "level": "High", "caveats": []},
        "use_api":    False,
    }


# ── Cohort Engine Tests ────────────────────────────────────────────────────────

def test_cohort_returns_same_specialty(sample_providers, sample_provider_month):
    result = get_cohort("P001", "2024-11", sample_providers, sample_provider_month)
    peer_specialties = sample_providers[
        sample_providers["provider_id"].isin(result["peer_ids"])
    ]["specialty"].unique()
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
    result = compute_confidence(
        cohort_n=1, current_encounters=5, prior_encounters=5, is_partial_period=True
    )
    assert result["score"] < 40
    assert result["level"] == "Low"


def test_confidence_caveats_for_missing_fields():
    result = compute_confidence(
        cohort_n=6, current_encounters=30, prior_encounters=30,
        missing_fields=["denial_data"],
    )
    assert any("denial" in c.lower() for c in result["caveats"])


def test_confidence_partial_period_caveat():
    result = compute_confidence(
        cohort_n=6, current_encounters=30, prior_encounters=30, is_partial_period=True
    )
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
    """Green percentile but low confidence -> should downgrade to yellow."""
    result = compute_adequacy(percentile=80.0, confidence_score=30, cohort_n=5)
    assert result["signal"] in ("yellow", "red")


def test_adequacy_unavailable_tiny_cohort():
    result = compute_adequacy(percentile=80.0, confidence_score=90, cohort_n=2)
    assert result["signal"] == "unavailable"


# ── Driver Attribution Tests ───────────────────────────────────────────────────

def test_drivers_returns_list(sample_charge_lines, sample_pos_df):
    result = compute_drivers(
        "P001", "2024-11", "2024-10",
        sample_charge_lines, pd.DataFrame(), sample_pos_df,
    )
    assert isinstance(result, list)
    assert len(result) >= 2


def test_drivers_have_required_keys(sample_charge_lines, sample_pos_df):
    result = compute_drivers(
        "P001", "2024-11", "2024-10",
        sample_charge_lines, pd.DataFrame(), sample_pos_df,
    )
    for d in result:
        for key in ["driver_name", "driver_category", "contribution_wrvu",
                    "contribution_pct", "direction", "available"]:
            assert key in d, f"Missing key '{key}' in driver {d}"


def test_drivers_sorted_by_abs_contribution(sample_charge_lines, sample_pos_df):
    result = compute_drivers(
        "P001", "2024-11", "2024-10",
        sample_charge_lines, pd.DataFrame(), sample_pos_df,
    )
    available = [d for d in result if d["available"]]
    for i in range(len(available) - 1):
        assert abs(available[i]["contribution_wrvu"]) >= abs(available[i + 1]["contribution_wrvu"])


# ── Narrative Tests ────────────────────────────────────────────────────────────

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

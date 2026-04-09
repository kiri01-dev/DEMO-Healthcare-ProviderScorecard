"""
Smoke test — validates all 20 providers return complete analytics outputs.
Run with: python tests/smoke_test.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from app.config import DEFAULT_PERIOD
from analytics.data_layer import (
    get_connection, load_all_providers, load_provider_month_summary,
    load_pos_mix, load_denial_summary,
)
from analytics import (
    get_cohort, get_cohort_stats, get_percentile,
    compute_drivers, compute_confidence, compute_adequacy, generate_narrative,
)

ERRORS = []
PERIOD = DEFAULT_PERIOD
PRIOR_PERIOD = "2024-10"


def check(condition: bool, message: str, provider_id: str = ""):
    """Record a failure if condition is False."""
    if not condition:
        ERRORS.append(f"  FAIL [{provider_id}] {message}")


def main():
    print(f"\n=== Smoke Test — All Providers, Period: {PERIOD} ===\n")

    con = get_connection()
    providers = load_all_providers(con)
    pm = load_provider_month_summary(con)
    charges = con.execute("SELECT * FROM fact_charge_line").df()
    cpt_ref = con.execute("SELECT * FROM dim_cpt").df()

    check(len(providers) == 20, f"Expected 20 providers, got {len(providers)}")
    check(
        len(pm[pm["service_month"] == PERIOD]) == 20,
        f"Expected 20 rows for {PERIOD}, got {len(pm[pm['service_month'] == PERIOD])}",
    )

    for _, provider in providers.iterrows():
        pid = provider["provider_id"]
        print(f"  Testing {pid} ({provider['specialty']})... ", end="", flush=True)

        p_data = pm[(pm["provider_id"] == pid) & (pm["service_month"] == PERIOD)]
        check(not p_data.empty, f"No data for period {PERIOD}", pid)
        if p_data.empty:
            print("SKIP")
            continue

        current_wrvu = float(p_data["total_wrvu"].values[0])
        enc_count = (
            int(p_data["encounter_count"].values[0])
            if "encounter_count" in p_data.columns
            else 30
        )

        # Cohort
        cohort = get_cohort(pid, PERIOD, providers, pm)
        check("peer_ids" in cohort, "Missing peer_ids", pid)
        check(cohort["cohort_n"] >= 0, "cohort_n negative", pid)
        check(pid not in cohort["peer_ids"], "Provider in own cohort", pid)

        # Stats
        stats = get_cohort_stats(cohort["peer_ids"], PERIOD, pm)
        check(stats["n"] >= 0, "Cohort stats n negative", pid)

        # Percentile
        pctile = get_percentile(current_wrvu, cohort["peer_ids"], PERIOD, pm)
        check(0 <= pctile <= 100, f"Percentile out of range: {pctile}", pid)

        # Confidence
        conf = compute_confidence(cohort["cohort_n"], enc_count, enc_count)
        check("score" in conf, "Missing confidence score", pid)
        check(0 <= conf["score"] <= 100, f"Confidence score out of range: {conf['score']}", pid)

        # Adequacy
        adeq = compute_adequacy(pctile, conf["score"], cohort["cohort_n"])
        check("signal" in adeq, "Missing adequacy signal", pid)
        check(
            adeq["signal"] in ("green", "yellow", "red", "unavailable"),
            f"Invalid signal: {adeq['signal']}",
            pid,
        )

        # Drivers
        pos = load_pos_mix(con, pid, [PERIOD, PRIOR_PERIOD])
        denial = load_denial_summary(con, pid, [PERIOD, PRIOR_PERIOD])
        pos_combined = pos if not pos.empty else pd.DataFrame()
        drivers = compute_drivers(
            pid, PERIOD, PRIOR_PERIOD, charges, cpt_ref,
            pos_combined,
            denial if not denial.empty else None,
        )
        check(len(drivers) >= 2, f"Too few drivers: {len(drivers)}", pid)

        # Narrative
        prior_row = pm[(pm["provider_id"] == pid) & (pm["service_month"] == PRIOR_PERIOD)]
        prior_wrvu = (
            float(prior_row["total_wrvu"].values[0]) if not prior_row.empty else current_wrvu
        )
        narrative = generate_narrative(
            provider["provider_name"], PERIOD, current_wrvu, prior_wrvu, current_wrvu,
            stats, pctile, drivers, adeq, conf, use_api=False,
        )
        check(
            isinstance(narrative, str) and len(narrative) > 50,
            f"Narrative too short or wrong type (len={len(narrative) if isinstance(narrative, str) else 'N/A'})",
            pid,
        )

        provider_errors = [e for e in ERRORS if f"[{pid}]" in e]
        print("OK" if not provider_errors else "FAIL")

    print(f"\n{'=' * 50}")
    if ERRORS:
        print(f"FAILED — {len(ERRORS)} error(s):")
        for err in ERRORS:
            print(err)
        con.close()
        sys.exit(1)
    else:
        print(f"ALL TESTS PASSED  ({len(providers)} providers validated)")

    con.close()


if __name__ == "__main__":
    main()

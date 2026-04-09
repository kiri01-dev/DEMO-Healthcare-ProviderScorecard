"""
test_session1.py
Session 1 acceptance tests — validates all data requirements for the Meridian PoC.
Run: python tests/test_session1.py
"""

from pathlib import Path
import sys

# Add project root to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import duckdb

DB_PATH = ROOT / "data" / "emory_poc.duckdb"


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run scripts/load_to_duckdb.py first.")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Test 1: Row counts
    n_providers = con.execute("SELECT COUNT(*) FROM dim_provider").fetchone()[0]
    assert n_providers == 20, f"Expected 20 providers, got {n_providers}"

    n_vm = con.execute("SELECT COUNT(*) FROM v_provider_month").fetchone()[0]
    assert n_vm == 240, f"Expected 240 v_provider_month rows, got {n_vm}"

    # Test 2: Specialty split
    n_im = con.execute("SELECT COUNT(*) FROM dim_provider WHERE specialty='Internal Medicine'").fetchone()[0]
    assert n_im == 10, f"Expected 10 Internal Medicine providers, got {n_im}"

    n_card = con.execute("SELECT COUNT(*) FROM dim_provider WHERE specialty='Cardiology'").fetchone()[0]
    assert n_card == 10, f"Expected 10 Cardiology providers, got {n_card}"

    # Test 3: Declining profile — P007 Q4 wRVU < Q1 wRVU
    p007_q1 = con.execute(
        "SELECT AVG(total_wrvu) FROM v_provider_month "
        "WHERE provider_id='P007' AND service_month IN ('2024-01','2024-02','2024-03')"
    ).fetchone()[0]
    p007_q4 = con.execute(
        "SELECT AVG(total_wrvu) FROM v_provider_month "
        "WHERE provider_id='P007' AND service_month IN ('2024-10','2024-11','2024-12')"
    ).fetchone()[0]
    assert p007_q4 < p007_q1, (
        f"P007 Q4 ({p007_q4:.0f}) should be < Q1 ({p007_q1:.0f}) — declining profile not detected"
    )

    p008_q1 = con.execute(
        "SELECT AVG(total_wrvu) FROM v_provider_month "
        "WHERE provider_id='P008' AND service_month IN ('2024-01','2024-02','2024-03')"
    ).fetchone()[0]
    p008_q4 = con.execute(
        "SELECT AVG(total_wrvu) FROM v_provider_month "
        "WHERE provider_id='P008' AND service_month IN ('2024-10','2024-11','2024-12')"
    ).fetchone()[0]
    assert p008_q4 < p008_q1, (
        f"P008 Q4 ({p008_q4:.0f}) should be < Q1 ({p008_q1:.0f}) — declining profile not detected"
    )

    # Test 4: Denial spike — P020 Jul-Sep vs Jan-Mar
    p020_normal = con.execute(
        "SELECT SUM(denial_count) FROM v_denial_month "
        "WHERE provider_id='P020' AND service_month IN ('2024-01','2024-02','2024-03')"
    ).fetchone()[0] or 0
    p020_spike = con.execute(
        "SELECT SUM(denial_count) FROM v_denial_month "
        "WHERE provider_id='P020' AND service_month IN ('2024-07','2024-08','2024-09')"
    ).fetchone()[0] or 0
    assert p020_spike > p020_normal * 2, (
        f"P020 denial spike not detected: spike={p020_spike}, normal={p020_normal}"
    )

    # Test 5: CPT referential integrity
    orphan_cpts = con.execute(
        "SELECT COUNT(*) FROM fact_charge_line cl "
        "WHERE NOT EXISTS (SELECT 1 FROM dim_cpt dc WHERE dc.cpt_code = cl.cpt_code)"
    ).fetchone()[0]
    assert orphan_cpts == 0, f"Found {orphan_cpts} charge lines with unknown CPT codes"

    # Test 6: No NULLs in key fields
    for col in ["provider_id", "service_month", "cpt_code", "total_wrvu", "charge_amount"]:
        n_nulls = con.execute(
            f"SELECT COUNT(*) FROM fact_charge_line WHERE {col} IS NULL"
        ).fetchone()[0]
        assert n_nulls == 0, f"NULL values found in fact_charge_line.{col}: {n_nulls}"

    # Test 7: DB file size > 1 MB
    db_size = DB_PATH.stat().st_size
    assert db_size > 1_000_000, f"Database file too small: {db_size} bytes"

    print("All Session 1 acceptance tests PASSED")
    print(f"  P007: Q1 wRVU={p007_q1:.0f}, Q4 wRVU={p007_q4:.0f} [declining OK]")
    print(f"  P008: Q1 wRVU={p008_q1:.0f}, Q4 wRVU={p008_q4:.0f} [declining OK]")
    print(f"  P020: normal denials={p020_normal}, spike denials={p020_spike} [spike OK]")

    con.close()


if __name__ == "__main__":
    main()

"""
load_to_duckdb.py
Loads all parquet files into DuckDB, creates schema (tables + views), and validates.
Run after generate_synthetic_data.py.

Validation report (last run):
  dim_provider            20 rows  [OK]
  dim_site                 4 rows  [OK]
  dim_cpt                 22 rows  [OK]
  fact_encounter      29,969 rows  [OK]
  fact_charge_line    29,969 rows  [OK]
  fact_claim          29,969 rows  [OK]
  fact_denial          1,604 rows  [OK]
  v_provider_month       240 rows  [OK]
  v_cpt_mix_month      2,410 rows  [OK]
  v_denial_month         701 rows  [OK]
  v_pos_mix_month        360 rows  [OK]
  P001 total wRVU Jan 2024: 202.21
  P011 total wRVU Jan 2024: 265.01
  P020 denial count Jul-Sep 2024: 82
  File size: 8.8 MB
  All validations passed.
"""

import sys
from pathlib import Path

import duckdb

# ── Paths ───────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "emory_poc.duckdb"

# ── DDL ──────────────────────────────────────────────────────────────────────
CREATE_TABLES = """
CREATE TABLE dim_provider (
    provider_id         VARCHAR PRIMARY KEY,
    provider_name       VARCHAR NOT NULL,
    specialty           VARCHAR NOT NULL,
    subspecialty        VARCHAR,
    provider_type       VARCHAR NOT NULL,
    employment_type     VARCHAR NOT NULL,
    primary_site_id     VARCHAR NOT NULL,
    active              BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim_site (
    site_id             VARCHAR PRIMARY KEY,
    site_name           VARCHAR NOT NULL,
    site_type           VARCHAR NOT NULL,
    specialty_focus     VARCHAR
);

CREATE TABLE dim_cpt (
    cpt_code            VARCHAR PRIMARY KEY,
    cpt_description     VARCHAR NOT NULL,
    cpt_family          VARCHAR NOT NULL,
    specialty_typical   VARCHAR,
    wrvu_value          DECIMAL(8,4) NOT NULL,
    em_level            VARCHAR
);

CREATE TABLE fact_encounter (
    encounter_id        VARCHAR PRIMARY KEY,
    provider_id         VARCHAR NOT NULL,
    service_date        DATE NOT NULL,
    service_month       VARCHAR NOT NULL,
    site_id             VARCHAR NOT NULL,
    pos_code            VARCHAR NOT NULL,
    visit_type          VARCHAR NOT NULL
);

CREATE TABLE fact_charge_line (
    charge_id           VARCHAR PRIMARY KEY,
    encounter_id        VARCHAR NOT NULL,
    provider_id         VARCHAR NOT NULL,
    service_month       VARCHAR NOT NULL,
    cpt_code            VARCHAR NOT NULL,
    cpt_family          VARCHAR NOT NULL,
    modifier_1          VARCHAR,
    units               INTEGER NOT NULL DEFAULT 1,
    wrvu_per_unit       DECIMAL(8,4) NOT NULL,
    total_wrvu          DECIMAL(10,4) NOT NULL,
    charge_amount       DECIMAL(12,2) NOT NULL,
    charge_date         DATE NOT NULL,
    pos_code            VARCHAR NOT NULL
);

CREATE TABLE fact_claim (
    claim_id            VARCHAR PRIMARY KEY,
    charge_id           VARCHAR NOT NULL,
    encounter_id        VARCHAR NOT NULL,
    provider_id         VARCHAR NOT NULL,
    service_month       VARCHAR NOT NULL,
    payer_id            VARCHAR NOT NULL,
    payer_name          VARCHAR NOT NULL,
    payer_category      VARCHAR NOT NULL,
    claim_status        VARCHAR NOT NULL,
    bill_date           DATE NOT NULL,
    adjudication_date   DATE,
    allowed_amount      DECIMAL(12,2),
    paid_amount         DECIMAL(12,2),
    contractual_adj     DECIMAL(12,2),
    patient_resp        DECIMAL(12,2)
);

CREATE TABLE fact_denial (
    denial_id           VARCHAR PRIMARY KEY,
    claim_id            VARCHAR NOT NULL,
    provider_id         VARCHAR NOT NULL,
    service_month       VARCHAR NOT NULL,
    denial_date         DATE NOT NULL,
    denial_reason_code  VARCHAR NOT NULL,
    denial_reason_desc  VARCHAR NOT NULL,
    denial_category     VARCHAR NOT NULL,
    denial_amount       DECIMAL(12,2)
);
"""

CREATE_VIEWS = """
CREATE VIEW v_provider_month AS
SELECT
    cl.provider_id,
    cl.service_month,
    p.specialty,
    p.primary_site_id,
    COUNT(DISTINCT cl.encounter_id)             AS encounter_count,
    SUM(cl.total_wrvu)                          AS total_wrvu,
    SUM(cl.charge_amount)                       AS total_charges,
    COUNT(DISTINCT cl.charge_id)                AS charge_line_count
FROM fact_charge_line cl
JOIN dim_provider p ON cl.provider_id = p.provider_id
GROUP BY cl.provider_id, cl.service_month, p.specialty, p.primary_site_id;

CREATE VIEW v_cpt_mix_month AS
SELECT
    cl.provider_id,
    cl.service_month,
    cl.cpt_code,
    cl.cpt_family,
    dc.cpt_description,
    dc.wrvu_value,
    SUM(cl.units)                               AS total_units,
    SUM(cl.total_wrvu)                          AS total_wrvu
FROM fact_charge_line cl
JOIN dim_cpt dc ON cl.cpt_code = dc.cpt_code
GROUP BY cl.provider_id, cl.service_month, cl.cpt_code, cl.cpt_family,
         dc.cpt_description, dc.wrvu_value;

CREATE VIEW v_denial_month AS
SELECT
    d.provider_id,
    d.service_month,
    d.denial_category,
    COUNT(*)                                    AS denial_count,
    SUM(d.denial_amount)                        AS denial_amount
FROM fact_denial d
GROUP BY d.provider_id, d.service_month, d.denial_category;

CREATE VIEW v_pos_mix_month AS
SELECT
    cl.provider_id,
    cl.service_month,
    cl.pos_code,
    CASE cl.pos_code
        WHEN '11' THEN 'Office'
        WHEN '22' THEN 'Hospital Outpatient'
        WHEN '24' THEN 'ASC'
        ELSE 'Other'
    END                                         AS pos_label,
    COUNT(DISTINCT cl.encounter_id)             AS encounter_count,
    SUM(cl.total_wrvu)                          AS total_wrvu
FROM fact_charge_line cl
GROUP BY cl.provider_id, cl.service_month, cl.pos_code;
"""

TABLES_ORDER = [
    "dim_provider",
    "dim_site",
    "dim_cpt",
    "fact_encounter",
    "fact_charge_line",
    "fact_claim",
    "fact_denial",
]

VIEWS_ORDER = [
    "v_provider_month",
    "v_cpt_mix_month",
    "v_denial_month",
    "v_pos_mix_month",
]

PARQUET_MAP = {
    "dim_provider":    "dim_provider.parquet",
    "dim_site":        "dim_site.parquet",
    "dim_cpt":         "dim_cpt.parquet",
    "fact_encounter":  "fact_encounter.parquet",
    "fact_charge_line":"fact_charge_line.parquet",
    "fact_claim":      "fact_claim.parquet",
    "fact_denial":     "fact_denial.parquet",
}


def main() -> None:
    """Load parquet files into DuckDB and validate."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Remove existing DB for clean load
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Removed existing {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    # Create tables
    for stmt in CREATE_TABLES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    print("Tables created.")

    # Load parquet files
    for table, parquet_file in PARQUET_MAP.items():
        parquet_path = DATA_DIR / parquet_file
        if not parquet_path.exists():
            print(f"ERROR: {parquet_path} not found. Run generate_synthetic_data.py first.")
            con.close()
            sys.exit(1)
        con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{parquet_path.as_posix()}')")

    print("Parquet files loaded.")

    # Create views
    for stmt in CREATE_VIEWS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    print("Views created.")

    # ── Validation ───────────────────────────────────────────────────────────
    errors = []
    print("\n=== DuckDB Load Validation ===")

    def check_count(table: str, expected_min: int, expected_max: int | None = None) -> int:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if expected_max is None:
            ok = n == expected_min
            expect_str = str(expected_min)
        else:
            ok = expected_min <= n <= expected_max
            expect_str = f"{expected_min:,}–{expected_max:,}"
        mark = "OK" if ok else "FAIL"
        print(f"  {table:<22} {n:>8,} rows  [{mark}]  (expect {expect_str})")
        if not ok:
            errors.append(f"{table}: got {n}, expected {expect_str}")
        return n

    check_count("dim_provider",    20)
    check_count("dim_site",         4)
    check_count("dim_cpt",         22)
    check_count("fact_encounter",  15000, 30000)
    check_count("fact_charge_line",15000, 30000)
    check_count("fact_claim",      15000, 30000)
    check_count("fact_denial",       500, 5000)

    print("\n  Views:")
    vm_count = check_count("v_provider_month", 240)
    check_count("v_cpt_mix_month",  100, 9999)
    check_count("v_denial_month",     1, 9999)
    check_count("v_pos_mix_month",    1, 9999)

    if vm_count != 240:
        errors.append(f"v_provider_month: got {vm_count}, expected 240")

    # Sample checks
    print("\n  Sample checks:")
    p001_wrvu = con.execute(
        "SELECT total_wrvu FROM v_provider_month WHERE provider_id='P001' AND service_month='2024-01'"
    ).fetchone()
    p001_val = p001_wrvu[0] if p001_wrvu else "N/A"
    print(f"  P001 total wRVU Jan 2024: {p001_val}")

    p011_wrvu = con.execute(
        "SELECT total_wrvu FROM v_provider_month WHERE provider_id='P011' AND service_month='2024-01'"
    ).fetchone()
    p011_val = p011_wrvu[0] if p011_wrvu else "N/A"
    print(f"  P011 total wRVU Jan 2024: {p011_val}")

    p020_denials = con.execute(
        "SELECT SUM(denial_count) FROM v_denial_month WHERE provider_id='P020' AND service_month IN ('2024-07','2024-08','2024-09')"
    ).fetchone()[0] or 0
    ok_denial = p020_denials > 10
    mark = "OK" if ok_denial else "FAIL"
    print(f"  P020 denial count Jul-Sep 2024: {p020_denials} [{mark}] (expect > 10)")
    if not ok_denial:
        errors.append(f"P020 denial spike: got {p020_denials}, expected > 10")

    # NULL checks on key fields
    null_checks = [
        ("fact_charge_line", "provider_id"),
        ("fact_charge_line", "service_month"),
        ("fact_charge_line", "cpt_code"),
        ("fact_charge_line", "total_wrvu"),
        ("fact_charge_line", "charge_amount"),
    ]
    print("\n  NULL checks (key fields):")
    for tbl, col in null_checks:
        n_nulls = con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL").fetchone()[0]
        ok = n_nulls == 0
        mark = "OK" if ok else "FAIL"
        print(f"  {tbl}.{col}: {n_nulls} NULLs  [{mark}]")
        if not ok:
            errors.append(f"NULL check failed: {tbl}.{col} has {n_nulls} NULLs")

    # CPT integrity
    orphan_cpts = con.execute(
        "SELECT COUNT(*) FROM fact_charge_line cl WHERE NOT EXISTS (SELECT 1 FROM dim_cpt dc WHERE dc.cpt_code = cl.cpt_code)"
    ).fetchone()[0]
    ok = orphan_cpts == 0
    mark = "OK" if ok else "FAIL"
    print(f"\n  CPT referential integrity: {orphan_cpts} orphan codes  [{mark}]")
    if not ok:
        errors.append(f"CPT integrity: {orphan_cpts} charge lines with unknown CPT codes")

    con.close()

    if errors:
        print("\n=== VALIDATION FAILED ===")
        for err in errors:
            print(f"  FAIL: {err}")
        sys.exit(1)

    db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"\n=== All validations passed ===")
    print(f"\nDatabase: {DB_PATH}")
    print(f"File size: {db_size_mb:.1f} MB")


if __name__ == "__main__":
    main()

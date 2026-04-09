"""
generate_synthetic_data.py
Generates all synthetic data for the Meridian Physician Division Provider Performance PoC.
Outputs 7 parquet files to data/ directory.
Deterministic: numpy.random.seed(42) + random.seed(42)
"""

import random
from pathlib import Path
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ── Seeds ──────────────────────────────────────────────────────────────────
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

# ── Output directory ────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Provider definitions ────────────────────────────────────────────────────
PROVIDER_PROFILES = [
    # Internal Medicine — P001 to P010
    {"provider_id": "P001", "specialty": "Internal Medicine", "profile": "steady",       "primary_site_id": "SITE_01"},
    {"provider_id": "P002", "specialty": "Internal Medicine", "profile": "steady",       "primary_site_id": "SITE_01"},
    {"provider_id": "P003", "specialty": "Internal Medicine", "profile": "steady",       "primary_site_id": "SITE_02"},
    {"provider_id": "P004", "specialty": "Internal Medicine", "profile": "high",         "primary_site_id": "SITE_01"},
    {"provider_id": "P005", "specialty": "Internal Medicine", "profile": "high",         "primary_site_id": "SITE_02"},
    {"provider_id": "P006", "specialty": "Internal Medicine", "profile": "high",         "primary_site_id": "SITE_01"},
    {"provider_id": "P007", "specialty": "Internal Medicine", "profile": "declining",    "primary_site_id": "SITE_01"},
    {"provider_id": "P008", "specialty": "Internal Medicine", "profile": "declining",    "primary_site_id": "SITE_02"},
    {"provider_id": "P009", "specialty": "Internal Medicine", "profile": "recovering",   "primary_site_id": "SITE_01"},
    {"provider_id": "P010", "specialty": "Internal Medicine", "profile": "recovering",   "primary_site_id": "SITE_02"},
    # Cardiology — P011 to P020
    {"provider_id": "P011", "specialty": "Cardiology", "profile": "steady",             "primary_site_id": "SITE_03"},
    {"provider_id": "P012", "specialty": "Cardiology", "profile": "steady",             "primary_site_id": "SITE_03"},
    {"provider_id": "P013", "specialty": "Cardiology", "profile": "steady",             "primary_site_id": "SITE_04"},
    {"provider_id": "P014", "specialty": "Cardiology", "profile": "high",               "primary_site_id": "SITE_03"},
    {"provider_id": "P015", "specialty": "Cardiology", "profile": "high",               "primary_site_id": "SITE_04"},
    {"provider_id": "P016", "specialty": "Cardiology", "profile": "declining",          "primary_site_id": "SITE_03"},
    {"provider_id": "P017", "specialty": "Cardiology", "profile": "declining",          "primary_site_id": "SITE_04"},
    {"provider_id": "P018", "specialty": "Cardiology", "profile": "coding_outlier",     "primary_site_id": "SITE_03"},
    {"provider_id": "P019", "specialty": "Cardiology", "profile": "coding_outlier",     "primary_site_id": "SITE_04"},
    {"provider_id": "P020", "specialty": "Cardiology", "profile": "denial_spike",       "primary_site_id": "SITE_04"},
]

SITES = [
    {"site_id": "SITE_01", "site_name": "Meridian Clinic North",        "site_type": "Clinic",              "specialty_focus": "Internal Medicine"},
    {"site_id": "SITE_02", "site_name": "Meridian Outpatient Center",   "site_type": "Hospital Outpatient", "specialty_focus": "Internal Medicine"},
    {"site_id": "SITE_03", "site_name": "Meridian Heart Center",        "site_type": "Clinic",              "specialty_focus": "Cardiology"},
    {"site_id": "SITE_04", "site_name": "Meridian Cardiac Services",    "site_type": "Hospital Outpatient", "specialty_focus": "Cardiology"},
]

# ── CPT reference data ──────────────────────────────────────────────────────
CPT_DATA = [
    # Internal Medicine
    {"cpt_code": "99212", "cpt_description": "Office visit, straightforward",          "cpt_family": "E&M",        "specialty_typical": "Internal Medicine", "wrvu_value": 0.70, "em_level": "99212"},
    {"cpt_code": "99213", "cpt_description": "Office visit, low complexity",           "cpt_family": "E&M",        "specialty_typical": "Internal Medicine", "wrvu_value": 1.30, "em_level": "99213"},
    {"cpt_code": "99214", "cpt_description": "Office visit, moderate complexity",      "cpt_family": "E&M",        "specialty_typical": "Internal Medicine", "wrvu_value": 1.92, "em_level": "99214"},
    {"cpt_code": "99215", "cpt_description": "Office visit, high complexity",          "cpt_family": "E&M",        "specialty_typical": "Internal Medicine", "wrvu_value": 2.80, "em_level": "99215"},
    {"cpt_code": "99395", "cpt_description": "Preventive visit, 18-39y",              "cpt_family": "Preventive", "specialty_typical": "Internal Medicine", "wrvu_value": 1.50, "em_level": None},
    {"cpt_code": "99396", "cpt_description": "Preventive visit, 40-64y",              "cpt_family": "Preventive", "specialty_typical": "Internal Medicine", "wrvu_value": 1.60, "em_level": None},
    {"cpt_code": "99397", "cpt_description": "Preventive visit, 65+y",               "cpt_family": "Preventive", "specialty_typical": "Internal Medicine", "wrvu_value": 1.50, "em_level": None},
    {"cpt_code": "G0438", "cpt_description": "Annual wellness visit (initial)",        "cpt_family": "Preventive", "specialty_typical": "Internal Medicine", "wrvu_value": 2.43, "em_level": None},
    {"cpt_code": "G0439", "cpt_description": "Annual wellness visit (subsequent)",     "cpt_family": "Preventive", "specialty_typical": "Internal Medicine", "wrvu_value": 1.50, "em_level": None},
    {"cpt_code": "93000", "cpt_description": "ECG with interpretation",                "cpt_family": "Procedures", "specialty_typical": "Internal Medicine", "wrvu_value": 0.17, "em_level": None},
    {"cpt_code": "36415", "cpt_description": "Routine venipuncture",                   "cpt_family": "Procedures", "specialty_typical": "Internal Medicine", "wrvu_value": 0.17, "em_level": None},
    {"cpt_code": "94010", "cpt_description": "Spirometry",                             "cpt_family": "Procedures", "specialty_typical": "Internal Medicine", "wrvu_value": 0.26, "em_level": None},
    # Cardiology-specific (E&M codes shared above)
    {"cpt_code": "93306", "cpt_description": "Echo TTE complete with Doppler",         "cpt_family": "Imaging",    "specialty_typical": "Cardiology",         "wrvu_value": 4.50, "em_level": None},
    {"cpt_code": "93307", "cpt_description": "Echo TTE without Doppler",               "cpt_family": "Imaging",    "specialty_typical": "Cardiology",         "wrvu_value": 3.33, "em_level": None},
    {"cpt_code": "93308", "cpt_description": "Echo TTE follow-up",                     "cpt_family": "Imaging",    "specialty_typical": "Cardiology",         "wrvu_value": 1.00, "em_level": None},
    {"cpt_code": "93015", "cpt_description": "Stress test (complete)",                 "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 4.50, "em_level": None},
    {"cpt_code": "93016", "cpt_description": "Stress test (supervision only)",         "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 0.92, "em_level": None},
    {"cpt_code": "93017", "cpt_description": "Stress test (tracing only)",             "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 1.43, "em_level": None},
    {"cpt_code": "93224", "cpt_description": "Holter monitor (up to 48h, recording)", "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 0.76, "em_level": None},
    {"cpt_code": "93226", "cpt_description": "Holter monitor (scanning analysis)",     "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 0.49, "em_level": None},
    {"cpt_code": "93454", "cpt_description": "Diagnostic coronary angiography",        "cpt_family": "Procedures", "specialty_typical": "Cardiology",         "wrvu_value": 14.00, "em_level": None},
    {"cpt_code": "78451", "cpt_description": "Nuclear stress test (SPECT, 1 study)",  "cpt_family": "Imaging",    "specialty_typical": "Cardiology",         "wrvu_value": 5.17, "em_level": None},
]

# CPT lookup dict for wRVU values
CPT_WRVU = {row["cpt_code"]: row["wrvu_value"] for row in CPT_DATA}
CPT_FAMILY = {row["cpt_code"]: row["cpt_family"] for row in CPT_DATA}

# ── Encounter volumes ────────────────────────────────────────────────────────
BASE_ENCOUNTERS = {
    ("Internal Medicine", "steady"):       (120, 160),
    ("Internal Medicine", "high"):         (160, 200),
    ("Internal Medicine", "declining"):    (130, 170),
    ("Internal Medicine", "recovering"):   (100, 180),
    ("Cardiology", "steady"):             (80, 110),
    ("Cardiology", "high"):               (110, 140),
    ("Cardiology", "declining"):          (90, 120),
    ("Cardiology", "coding_outlier"):     (85, 115),
    ("Cardiology", "denial_spike"):       (85, 115),
}

# ── CPT weights ──────────────────────────────────────────────────────────────
IM_CPT_WEIGHTS = {
    "99214": 0.40,
    "99213": 0.25,
    "99215": 0.15,
    "99212": 0.05,
    "99395": 0.05,
    "99396": 0.04,
    "G0438": 0.02,
    "G0439": 0.02,
    "93000": 0.01,
    "36415": 0.01,
}

CARD_CPT_WEIGHTS = {
    "99214": 0.25,
    "99215": 0.20,
    "99213": 0.10,
    "93306": 0.18,
    "93015": 0.10,
    "93000": 0.05,
    "93224": 0.04,
    "93307": 0.03,
    "93017": 0.02,
    "93226": 0.01,
    "93454": 0.01,
    "78451": 0.01,
}

# ── POS config ───────────────────────────────────────────────────────────────
POS_BY_SITE = {
    "SITE_01": [("11", 1.0)],
    "SITE_02": [("22", 1.0)],
    "SITE_03": [("11", 1.0)],
    "SITE_04": [("22", 0.70), ("24", 0.20), ("11", 0.10)],
}

# ── Payer weights ────────────────────────────────────────────────────────────
PAYER_CATEGORIES = ["Medicare", "Commercial", "Medicaid", "Self-Pay"]
PAYER_WEIGHTS = [0.35, 0.40, 0.15, 0.10]

PAYER_NAMES = {
    "Medicare":   ["Medicare Part B"],
    "Commercial": ["BlueCross BlueShield", "Aetna", "United Healthcare", "Cigna"],
    "Medicaid":   ["Georgia Medicaid"],
    "Self-Pay":   ["Self-Pay"],
}

# ── Denial categories ────────────────────────────────────────────────────────
DENIAL_CATEGORIES = ["Coding", "Authorization", "Eligibility", "Timely Filing"]
DENIAL_WEIGHTS_NORMAL = [0.40, 0.30, 0.20, 0.10]

DENIAL_REASON_CODES = {
    "Coding":        ("CO-4",  "The procedure code is inconsistent with the modifier"),
    "Authorization": ("CO-15", "Payment adjusted because the submitted authorization number is missing"),
    "Eligibility":   ("CO-27", "Expenses incurred after coverage terminated"),
    "Timely Filing": ("CO-29", "The time limit for filing has expired"),
}


def _random_date_in_month(year: int, month: int) -> date:
    """Returns a random date within the given year/month."""
    import calendar
    _, days_in_month = calendar.monthrange(year, month)
    return date(year, month, random.randint(1, days_in_month))


def _pick_pos(site_id: str) -> str:
    """Pick a POS code based on site POS distribution."""
    options = POS_BY_SITE[site_id]
    codes = [o[0] for o in options]
    weights = [o[1] for o in options]
    return random.choices(codes, weights=weights, k=1)[0]


def _get_cpt_weights(specialty: str, profile: str, month_num: int) -> tuple[list, list]:
    """Return (cpt_codes, weights) for a provider in a given month."""
    if specialty == "Internal Medicine":
        codes = list(IM_CPT_WEIGHTS.keys())
        weights = list(IM_CPT_WEIGHTS.values())
    else:
        codes = list(CARD_CPT_WEIGHTS.keys())
        weights = list(CARD_CPT_WEIGHTS.values())

    # coding_outlier: echo 93306 at 2x rate Jan-Jun, 0.5x rate Jul-Dec
    if profile == "coding_outlier" and specialty == "Cardiology" and "93306" in codes:
        idx = codes.index("93306")
        if month_num <= 6:
            weights[idx] *= 2.0
        else:
            weights[idx] *= 0.5

    # declining: shift E&M mix from Sep+ (increase 99213, decrease 99215)
    if profile == "declining" and month_num >= 9:
        for code, delta in [("99213", +0.15), ("99215", -0.15)]:
            if code in codes:
                idx = codes.index(code)
                weights[idx] = max(0.0, weights[idx] + delta)

    # Normalize weights
    total = sum(weights)
    weights = [w / total for w in weights]
    return codes, weights


def _get_encounter_count(specialty: str, profile: str, month_num: int) -> int:
    """Returns encounter count with profile modifier applied."""
    lo, hi = BASE_ENCOUNTERS[(specialty, profile)]
    base = random.randint(lo, hi)

    modifier = 1.0
    if profile == "declining" and month_num >= 9:
        modifier = 0.75
    elif profile == "recovering":
        if 4 <= month_num <= 6:
            modifier = 0.70
        elif month_num >= 7:
            # gradual recovery: 0.70 at Jul → 0.95 at Dec
            modifier = 0.70 + (0.25 * (month_num - 7) / 5)
    return max(1, int(base * modifier))


def generate_providers() -> pd.DataFrame:
    """Generate dim_provider DataFrame."""
    rows = []
    for p in PROVIDER_PROFILES:
        last = fake.last_name()
        first = fake.first_name()
        provider_type = random.choice(["MD", "DO"])
        rows.append({
            "provider_id":     p["provider_id"],
            "provider_name":   f"{last}, {first}",
            "specialty":       p["specialty"],
            "subspecialty":    None,
            "provider_type":   provider_type,
            "employment_type": "Employed",
            "primary_site_id": p["primary_site_id"],
            "active":          True,
        })
    return pd.DataFrame(rows)


def generate_sites() -> pd.DataFrame:
    """Generate dim_site DataFrame."""
    return pd.DataFrame(SITES)


def generate_cpt() -> pd.DataFrame:
    """Generate dim_cpt DataFrame."""
    return pd.DataFrame(CPT_DATA)


def generate_encounters_and_charges() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generate fact_encounter, fact_charge_line, fact_claim, fact_denial DataFrames.
    Returns (encounters_df, charges_df, claims_df, denials_df).
    """
    encounters = []
    charges = []
    claims = []
    denials = []

    # Build quick lookup
    profile_lookup = {p["provider_id"]: p for p in PROVIDER_PROFILES}

    for p_info in PROVIDER_PROFILES:
        pid = p_info["provider_id"]
        specialty = p_info["specialty"]
        profile = p_info["profile"]
        site_id = p_info["primary_site_id"]

        for month_num in range(1, 13):
            year = 2024
            service_month = f"{year}-{month_num:02d}"

            enc_count = _get_encounter_count(specialty, profile, month_num)
            cpt_codes, cpt_weights = _get_cpt_weights(specialty, profile, month_num)

            # Determine denial rate for this provider/month
            is_denial_spike = (profile == "denial_spike" and 7 <= month_num <= 9)
            denial_rate = 0.25 if is_denial_spike else 0.05

            for seq in range(1, enc_count + 1):
                enc_id = f"ENC_{pid}_{year}{month_num:02d}_{seq:04d}"
                service_date = _random_date_in_month(year, month_num)
                pos_code = _pick_pos(site_id)

                # Determine visit type
                visit_type = random.choices(
                    ["New", "Established", "Procedure"],
                    weights=[0.20, 0.65, 0.15],
                    k=1
                )[0]

                encounters.append({
                    "encounter_id":  enc_id,
                    "provider_id":   pid,
                    "service_date":  service_date,
                    "service_month": service_month,
                    "site_id":       site_id,
                    "pos_code":      pos_code,
                    "visit_type":    visit_type,
                })

                # Generate 1 charge line per encounter
                cpt_code = random.choices(cpt_codes, weights=cpt_weights, k=1)[0]
                wrvu_per_unit = CPT_WRVU[cpt_code]
                units = 1
                total_wrvu = wrvu_per_unit * units
                charge_amount = round(total_wrvu * 52.0, 2)
                charge_date = service_date + timedelta(days=random.randint(1, 3))
                charge_id = f"CHG_{enc_id}_{cpt_code}"

                charges.append({
                    "charge_id":      charge_id,
                    "encounter_id":   enc_id,
                    "provider_id":    pid,
                    "service_month":  service_month,
                    "cpt_code":       cpt_code,
                    "cpt_family":     CPT_FAMILY[cpt_code],
                    "modifier_1":     None,
                    "units":          units,
                    "wrvu_per_unit":  wrvu_per_unit,
                    "total_wrvu":     total_wrvu,
                    "charge_amount":  charge_amount,
                    "charge_date":    charge_date,
                    "pos_code":       pos_code,
                })

                # Generate claim
                bill_date = charge_date + timedelta(days=random.randint(2, 5))
                is_denied = random.random() < denial_rate
                claim_status = "Denied" if is_denied else "Adjudicated"
                payer_cat = random.choices(PAYER_CATEGORIES, weights=PAYER_WEIGHTS, k=1)[0]
                payer_name = random.choice(PAYER_NAMES[payer_cat])

                if is_denied:
                    adj_date = None
                    allowed_amount = None
                    paid_amount = None
                    contractual_adj = None
                    patient_resp = None
                else:
                    adj_date = bill_date + timedelta(days=random.randint(15, 45))
                    allowed_amount = round(charge_amount * random.uniform(0.55, 0.85), 2)
                    paid_amount = round(allowed_amount * random.uniform(0.75, 0.95), 2)
                    contractual_adj = round(charge_amount - allowed_amount, 2)
                    patient_resp = round(allowed_amount - paid_amount, 2)

                claim_id = f"CLM_{charge_id}"
                claims.append({
                    "claim_id":          claim_id,
                    "charge_id":         charge_id,
                    "encounter_id":      enc_id,
                    "provider_id":       pid,
                    "service_month":     service_month,
                    "payer_id":          payer_cat.upper().replace("-", "_").replace(" ", "_"),
                    "payer_name":        payer_name,
                    "payer_category":    payer_cat,
                    "claim_status":      claim_status,
                    "bill_date":         bill_date,
                    "adjudication_date": adj_date,
                    "allowed_amount":    allowed_amount,
                    "paid_amount":       paid_amount,
                    "contractual_adj":   contractual_adj,
                    "patient_resp":      patient_resp,
                })

                # Generate denial record if denied
                if is_denied:
                    denial_date = bill_date + timedelta(days=random.randint(10, 30))
                    if is_denial_spike:
                        denial_cat = "Authorization"
                    else:
                        denial_cat = random.choices(DENIAL_CATEGORIES, weights=DENIAL_WEIGHTS_NORMAL, k=1)[0]

                    reason_code, reason_desc = DENIAL_REASON_CODES[denial_cat]
                    denial_id = f"DEN_{claim_id}"
                    denials.append({
                        "denial_id":          denial_id,
                        "claim_id":           claim_id,
                        "provider_id":        pid,
                        "service_month":      service_month,
                        "denial_date":        denial_date,
                        "denial_reason_code": reason_code,
                        "denial_reason_desc": reason_desc,
                        "denial_category":    denial_cat,
                        "denial_amount":      charge_amount,
                    })

    return (
        pd.DataFrame(encounters),
        pd.DataFrame(charges),
        pd.DataFrame(claims),
        pd.DataFrame(denials),
    )


def main() -> None:
    """Generate all synthetic data and save as parquet files."""
    print("Generating synthetic data...")

    # Dimensions
    providers_df = generate_providers()
    sites_df = generate_sites()
    cpt_df = generate_cpt()

    print(f"  dim_provider:  {len(providers_df)} rows")
    print(f"  dim_site:      {len(sites_df)} rows")
    print(f"  dim_cpt:       {len(cpt_df)} rows")

    # Facts
    encounters_df, charges_df, claims_df, denials_df = generate_encounters_and_charges()

    print(f"  fact_encounter:    {len(encounters_df):,} rows")
    print(f"  fact_charge_line:  {len(charges_df):,} rows")
    print(f"  fact_claim:        {len(claims_df):,} rows")
    print(f"  fact_denial:       {len(denials_df):,} rows")

    # Save parquet files
    providers_df.to_parquet(DATA_DIR / "dim_provider.parquet", index=False)
    sites_df.to_parquet(DATA_DIR / "dim_site.parquet", index=False)
    cpt_df.to_parquet(DATA_DIR / "dim_cpt.parquet", index=False)
    encounters_df.to_parquet(DATA_DIR / "fact_encounter.parquet", index=False)
    charges_df.to_parquet(DATA_DIR / "fact_charge_line.parquet", index=False)
    claims_df.to_parquet(DATA_DIR / "fact_claim.parquet", index=False)
    denials_df.to_parquet(DATA_DIR / "fact_denial.parquet", index=False)

    print(f"\nAll parquet files saved to {DATA_DIR}/")
    print("Data generation complete.")


if __name__ == "__main__":
    main()

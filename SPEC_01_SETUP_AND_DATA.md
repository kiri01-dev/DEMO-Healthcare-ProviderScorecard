# SPEC_01 — Session 1: Environment Setup & Synthetic Data Generation
## Meridian Physician Division Provider Performance App (PoC)

> **Claude Code Session 1 of 3.**
> Read `CLAUDE.md` and `ARCHITECTURE.md` before starting.
> Working directory: the `APP_Emory PoC v2/` folder.

---

## Session Goal

By the end of this session, the following must exist and be validated:

- [ ] Python virtual environment created and all dependencies installed
- [ ] `.env.example` file created
- [ ] `scripts/generate_synthetic_data.py` written and executed successfully
- [ ] `scripts/load_to_duckdb.py` written and executed successfully
- [ ] `data/meridian_poc.duckdb` exists with all tables populated
- [ ] Validation report printed showing row counts for all tables
- [ ] All acceptance tests at the bottom of this file pass

---

## Step 1: Create Python Virtual Environment

```powershell
# Run in PowerShell from the APP_Emory PoC v2/ directory
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell execution policy blocks the activate script, run first:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## Step 2: Create `requirements.txt`

Create this file exactly:

```
streamlit==1.35.0
duckdb==0.10.3
pandas==2.2.2
numpy==1.26.4
plotly==5.22.0
anthropic==0.28.0
python-dotenv==1.0.1
faker==25.2.0
pytest==8.2.1
pyarrow==16.1.0
```

Then install:
```powershell
pip install -r requirements.txt
```

---

## Step 3: Create `.env.example`

```
# Copy this file to .env and fill in your values
ANTHROPIC_API_KEY=your_api_key_here
```

---

## Step 4: Create `scripts/generate_synthetic_data.py`

Write a complete Python script that generates all synthetic data and saves it as parquet files in `data/`.

### Script requirements

The script must be **deterministic** — use `numpy.random.seed(42)` and `random.seed(42)` at the top so the same data is always generated.

The script generates 6 parquet files:
- `data/dim_provider.parquet`
- `data/dim_site.parquet`
- `data/dim_cpt.parquet`
- `data/fact_encounter.parquet`
- `data/fact_charge_line.parquet`
- `data/fact_claim.parquet`
- `data/fact_denial.parquet`

### Providers to generate

Generate exactly these 20 providers. Use `Faker(seed=42)` for realistic names, but ensure 10 are in Internal Medicine and 10 in Cardiology.

```python
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
```

### Sites to generate

```python
SITES = [
    {"site_id": "SITE_01", "site_name": "Meridian Clinic North",         "site_type": "Clinic",              "specialty_focus": "Internal Medicine"},
    {"site_id": "SITE_02", "site_name": "Meridian Outpatient Center",   "site_type": "Hospital Outpatient", "specialty_focus": "Internal Medicine"},
    {"site_id": "SITE_03", "site_name": "Meridian Heart Center",        "site_type": "Clinic",              "specialty_focus": "Cardiology"},
    {"site_id": "SITE_04", "site_name": "Meridian Cardiac Services",    "site_type": "Hospital Outpatient", "specialty_focus": "Cardiology"},
]
```

### CPT reference data

Hardcode the CPT table exactly from `ARCHITECTURE.md` CPT Code Reference section. Include all 22 CPT codes listed there with their wRVU values and family classifications.

### Encounter and charge line generation logic

For each provider, for each month (Jan 2024 = '2024-01' through Dec 2024 = '2024-12'):

**Base monthly encounter volumes by profile and specialty:**

```python
BASE_ENCOUNTERS = {
    # (specialty, profile): (min, max) encounters per month
    ("Internal Medicine", "steady"):         (120, 160),
    ("Internal Medicine", "high"):           (160, 200),
    ("Internal Medicine", "declining"):      (130, 170),  # starts normal, drops Q4
    ("Internal Medicine", "recovering"):     (100, 180),  # dips Q2, recovers Q4
    ("Cardiology", "steady"):               (80, 110),
    ("Cardiology", "high"):                 (110, 140),
    ("Cardiology", "declining"):            (90, 120),    # starts normal, drops Q4
    ("Cardiology", "coding_outlier"):       (85, 115),
    ("Cardiology", "denial_spike"):         (85, 115),
}
```

**Profile-specific modifiers (apply on top of base):**

- `declining` providers: from September onwards, multiply encounter count by 0.75 AND shift E&M mix downward (increase 99213 share by 15%, decrease 99215 share by 15%)
- `recovering` providers: April–June multiply encounter count by 0.70; July onwards gradually recover to 0.95× by December
- `high` providers: no modifier (already high base)
- `coding_outlier` Cardiology providers: use echo (93306) at 2× the normal rate for the first 6 months; drop to 0.5× in Q3–Q4 (creates a detectable CPT mix shift)
- `denial_spike` Cardiology provider: in months July–September, set denial rate to 25% of claims (vs. normal 5%); denial category = 'Authorization'

**CPT mix by specialty (probability weights for random CPT selection):**

```python
IM_CPT_WEIGHTS = {
    "99214": 0.40,   # Most common E&M
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
    "93306": 0.18,   # Echo — high wRVU, significant in mix
    "93015": 0.10,   # Stress test
    "93000": 0.05,
    "93224": 0.04,
    "93307": 0.03,
    "93017": 0.02,
    "93226": 0.01,
    "93454": 0.01,   # Diagnostic cath — very high wRVU
    "78451": 0.01,
}
```

**POS (place of service) assignment:**
- SITE_01 and SITE_03 (Clinic): 100% POS 11 (Office)
- SITE_02 (Hospital Outpatient): 100% POS 22
- SITE_04 (Cardiac Services): 70% POS 22, 20% POS 24 (ASC), 10% POS 11

**Charge amounts:** Set `charge_amount = total_wrvu × 52.0` (roughly approximate Medicare conversion factor × 4 for gross charges).

**Claim generation:** For each charge line, generate one claim. Assign payer category with these weights: Medicare 35%, Commercial 40%, Medicaid 15%, Self-Pay 10%. Normal denial rate: 5% of claims. Set `claim_status = 'Denied'` for denied claims, `'Adjudicated'` otherwise.

**Denial generation:** For each denied claim, create one denial record. Normal denial categories: Coding 40%, Authorization 30%, Eligibility 20%, Timely Filing 10%. Apply the denial_spike profile modifier for P020 as specified above.

**Dates:**
- `service_date`: random date within the service month
- `charge_date`: service_date + 1–3 days
- `bill_date`: charge_date + 2–5 days
- `adjudication_date`: bill_date + 15–45 days (NULL for denied claims)
- `denial_date`: bill_date + 10–30 days (denials only)

### ID generation

- `encounter_id`: `ENC_{provider_id}_{YYYYMM}_{sequence:04d}`
- `charge_id`: `CHG_{encounter_id}_{cpt_code}`
- `claim_id`: `CLM_{charge_id}`
- `denial_id`: `DEN_{claim_id}`

---

## Step 5: Create `scripts/load_to_duckdb.py`

Write a script that:
1. Creates `data/` directory if it doesn't exist
2. Opens (or creates) `data/meridian_poc.duckdb`
3. Drops all tables and views if they exist (idempotent)
4. Creates all tables from the schema in `ARCHITECTURE.md`
5. Loads each parquet file into its corresponding table
6. Creates all views from the schema
7. Runs validation queries and prints a report

**Validation report must include:**

```
=== DuckDB Load Validation ===
dim_provider:       20 rows  ✓
dim_site:            4 rows  ✓
dim_cpt:            22 rows  ✓
fact_encounter:   [N] rows  ✓  (expect ~15,000–25,000)
fact_charge_line: [N] rows  ✓  (expect ~18,000–30,000)
fact_claim:       [N] rows  ✓  (same as charge_line)
fact_denial:      [N] rows  ✓  (expect ~5% of claims)

Views:
v_provider_month:  [N] rows (expect 240 = 20 providers × 12 months)
v_cpt_mix_month:   [N] rows ✓
v_denial_month:    [N] rows ✓
v_pos_mix_month:   [N] rows ✓

Sample check — P001 total wRVU Jan 2024:  [value]  ✓
Sample check — P011 total wRVU Jan 2024:  [value]  ✓
Sample check — P020 denial count Jul 2024: [value] (expect > 10)  ✓

=== All validations passed ===
```

If any check fails, print a clear error message and exit with code 1.

---

## Step 6: Execute Both Scripts

Run in order:

```powershell
python scripts/generate_synthetic_data.py
python scripts/load_to_duckdb.py
```

Both must complete without errors. Paste the validation report output into a comment at the top of `load_to_duckdb.py` for reference.

---

## Acceptance Tests

Before closing this session, verify all of the following:

1. `data/meridian_poc.duckdb` file exists and is > 1 MB
2. `dim_provider` has exactly 20 rows — 10 Internal Medicine, 10 Cardiology
3. `v_provider_month` has exactly 240 rows (20 × 12)
4. P001 through P010 have `specialty = 'Internal Medicine'`
5. P011 through P020 have `specialty = 'Cardiology'`
6. P007 and P008 show measurably lower wRVU in Oct–Dec vs Jan–Mar (declining profile)
7. P020 shows denial count in Jul–Sep at least 3× higher than Jan–Mar (denial spike)
8. All CPT codes in fact_charge_line exist in dim_cpt
9. No NULL values in: provider_id, service_month, cpt_code, total_wrvu, charge_amount

Run this quick validation query after loading:

```python
import duckdb
con = duckdb.connect("data/meridian_poc.duckdb", read_only=True)

# Test 1: row counts
assert con.execute("SELECT COUNT(*) FROM dim_provider").fetchone()[0] == 20
assert con.execute("SELECT COUNT(*) FROM v_provider_month").fetchone()[0] == 240

# Test 2: declining profile — P007 Q4 vs Q1 wRVU should be lower
p007_q1 = con.execute("SELECT AVG(total_wrvu) FROM v_provider_month WHERE provider_id='P007' AND service_month IN ('2024-01','2024-02','2024-03')").fetchone()[0]
p007_q4 = con.execute("SELECT AVG(total_wrvu) FROM v_provider_month WHERE provider_id='P007' AND service_month IN ('2024-10','2024-11','2024-12')").fetchone()[0]
assert p007_q4 < p007_q1, f"P007 Q4 ({p007_q4:.0f}) should be < Q1 ({p007_q1:.0f})"

# Test 3: denial spike — P020 Jul-Sep vs Jan-Mar
p020_normal = con.execute("SELECT SUM(denial_count) FROM v_denial_month WHERE provider_id='P020' AND service_month IN ('2024-01','2024-02','2024-03')").fetchone()[0] or 0
p020_spike  = con.execute("SELECT SUM(denial_count) FROM v_denial_month WHERE provider_id='P020' AND service_month IN ('2024-07','2024-08','2024-09')").fetchone()[0] or 0
assert p020_spike > p020_normal * 2, f"P020 denial spike not detected: spike={p020_spike}, normal={p020_normal}"

print("All Session 1 acceptance tests PASSED ✓")
con.close()
```

Save this as `tests/test_session1.py` and run it:
```powershell
python tests/test_session1.py
```

---

## End of Session 1

When all acceptance tests pass, report:
- Total rows in each fact table
- File size of `data/meridian_poc.duckdb`
- Any notable data generation decisions made

Then stop. Do not proceed to Session 2.

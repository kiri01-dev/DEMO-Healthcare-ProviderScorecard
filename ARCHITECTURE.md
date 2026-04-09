# ARCHITECTURE.md — Technical Reference
## Meridian Physician Division Provider Performance App (PoC)

> Reference document. Read this alongside SPEC files during all sessions.
> Do not deviate from the schema or module interfaces defined here without updating this file.

---

## Tech Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| UI | Streamlit 1.35+ | Fastest Python dashboard framework; no frontend build step; works on Windows |
| Database | DuckDB 0.10+ | Embedded, zero-config, fast analytical queries, no server required |
| Analytics | Python / pandas / numpy | Pure Python analytics; testable; no external service dependencies |
| Charts | Plotly Express | Native Streamlit integration; interactive by default |
| Narrative AI | Anthropic Claude API (`claude-haiku-4-5-20251001`) | Fast, cheap, high-quality; Haiku tier for low latency |
| Env management | python-dotenv | `.env` file for API key |
| Testing | pytest | Unit tests for analytics layer |
| Data generation | Faker + numpy random | Realistic synthetic names and values |

---

## Data Architecture

### Data Flow

```
scripts/generate_synthetic_data.py
    │
    ├── Generates: providers, sites, encounters, charge_lines, claims, denials
    ├── Writes: data/*.parquet (one file per table)
    │
    └── scripts/load_to_duckdb.py
            │
            └── Loads parquet → DuckDB tables → data/meridian_poc.duckdb
                        │
                        └── app/ reads via DuckDB connection (read-only)
                                │
                                ├── analytics/ modules receive DataFrames
                                └── narrative_engine.py generates text
```

### DuckDB Schema

All tables are created in the default schema. Run `scripts/load_to_duckdb.py` to create and populate.

```sql
-- ─────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ─────────────────────────────────────────────────────────

CREATE TABLE dim_provider (
    provider_id         VARCHAR PRIMARY KEY,   -- 'P001' through 'P020'
    provider_name       VARCHAR NOT NULL,       -- 'Last, First'
    specialty           VARCHAR NOT NULL,       -- 'Internal Medicine' | 'Cardiology'
    subspecialty        VARCHAR,                -- NULL for PoC
    provider_type       VARCHAR NOT NULL,       -- 'MD' | 'DO'
    employment_type     VARCHAR NOT NULL,       -- 'Employed' for all PoC providers
    primary_site_id     VARCHAR NOT NULL,       -- FK to dim_site
    active              BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim_site (
    site_id             VARCHAR PRIMARY KEY,    -- 'SITE_01' through 'SITE_04'
    site_name           VARCHAR NOT NULL,
    site_type           VARCHAR NOT NULL,       -- 'Clinic' | 'Hospital Outpatient' | 'ASC'
    specialty_focus     VARCHAR                 -- 'Internal Medicine' | 'Cardiology' | 'Mixed'
);

CREATE TABLE dim_cpt (
    cpt_code            VARCHAR PRIMARY KEY,
    cpt_description     VARCHAR NOT NULL,
    cpt_family          VARCHAR NOT NULL,       -- 'E&M' | 'Procedures' | 'Imaging' | 'Preventive'
    specialty_typical   VARCHAR,               -- specialty this code is most common for
    wrvu_value          DECIMAL(8,4) NOT NULL, -- 2024 CMS RBRVS wRVU value
    em_level            VARCHAR                -- '99213' | '99214' | '99215' | NULL
);

-- ─────────────────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────────────────

CREATE TABLE fact_encounter (
    encounter_id        VARCHAR PRIMARY KEY,
    provider_id         VARCHAR NOT NULL,       -- FK to dim_provider
    service_date        DATE NOT NULL,
    service_month       VARCHAR NOT NULL,       -- 'YYYY-MM' — pre-computed for performance
    site_id             VARCHAR NOT NULL,       -- FK to dim_site
    pos_code            VARCHAR NOT NULL,       -- '11' | '22' | '24'
    visit_type          VARCHAR NOT NULL        -- 'New' | 'Established' | 'Procedure'
);

CREATE TABLE fact_charge_line (
    charge_id           VARCHAR PRIMARY KEY,
    encounter_id        VARCHAR NOT NULL,       -- FK to fact_encounter
    provider_id         VARCHAR NOT NULL,       -- FK to dim_provider (denormalized for query perf)
    service_month       VARCHAR NOT NULL,       -- 'YYYY-MM' — denormalized for query perf
    cpt_code            VARCHAR NOT NULL,       -- FK to dim_cpt
    cpt_family          VARCHAR NOT NULL,       -- denormalized from dim_cpt
    modifier_1          VARCHAR,
    units               INTEGER NOT NULL DEFAULT 1,
    wrvu_per_unit       DECIMAL(8,4) NOT NULL,
    total_wrvu          DECIMAL(10,4) NOT NULL, -- units × wrvu_per_unit
    charge_amount       DECIMAL(12,2) NOT NULL,
    charge_date         DATE NOT NULL,
    pos_code            VARCHAR NOT NULL        -- denormalized from encounter
);

CREATE TABLE fact_claim (
    claim_id            VARCHAR PRIMARY KEY,
    charge_id           VARCHAR NOT NULL,       -- FK to fact_charge_line
    encounter_id        VARCHAR NOT NULL,
    provider_id         VARCHAR NOT NULL,
    service_month       VARCHAR NOT NULL,
    payer_id            VARCHAR NOT NULL,
    payer_name          VARCHAR NOT NULL,
    payer_category      VARCHAR NOT NULL,       -- 'Medicare' | 'Medicaid' | 'Commercial' | 'Self-Pay'
    claim_status        VARCHAR NOT NULL,       -- 'Adjudicated' | 'Denied' | 'Pending'
    bill_date           DATE NOT NULL,
    adjudication_date   DATE,
    allowed_amount      DECIMAL(12,2),
    paid_amount         DECIMAL(12,2),
    contractual_adj     DECIMAL(12,2),
    patient_resp        DECIMAL(12,2)
);

CREATE TABLE fact_denial (
    denial_id           VARCHAR PRIMARY KEY,
    claim_id            VARCHAR NOT NULL,       -- FK to fact_claim
    provider_id         VARCHAR NOT NULL,
    service_month       VARCHAR NOT NULL,
    denial_date         DATE NOT NULL,
    denial_reason_code  VARCHAR NOT NULL,       -- CARC code
    denial_reason_desc  VARCHAR NOT NULL,
    denial_category     VARCHAR NOT NULL,       -- 'Authorization' | 'Coding' | 'Eligibility' | 'Timely Filing' | 'Other'
    denial_amount       DECIMAL(12,2)
);

-- ─────────────────────────────────────────────────────────
-- PRE-AGGREGATED VIEWS (for query performance)
-- ─────────────────────────────────────────────────────────

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
```

---

## Synthetic Data Profile

### Provider Profiles

```
Internal Medicine (P001–P010):
  Sites: SITE_01 (Meridian Clinic North, Office/POS 11)
         SITE_02 (Meridian Hospital Outpatient, POS 22)
  CPT families: E&M (primary), Preventive, Minor Procedures
  Typical wRVU range: 250–500/month
  10 providers with varied performance:
    - 3 "steady" (consistent, near peer median)
    - 3 "high performers" (>75th percentile)
    - 2 "declining" (trending down Q3→Q4 2024, E&M level downshift)
    - 2 "recovering" (dip in mid-year, rebounds)

Cardiology (P011–P020):
  Sites: SITE_03 (Meridian Heart Center, Office/POS 11)
         SITE_04 (Meridian Hospital Cardiac, POS 22 + POS 24 ASC)
  CPT families: E&M, Imaging (Echo), Stress Tests, Procedures (Cath)
  Typical wRVU range: 350–700/month
  10 providers with varied performance:
    - 3 "steady"
    - 2 "high performers"
    - 2 "declining" (volume drop Q4, setting shift from hospital to office)
    - 2 "coding outliers" (E&M mix or procedure modifier anomalies)
    - 1 "denial spike" (authorization denials spike in Q3)
```

### CPT Code Reference (with 2024 CMS wRVU values)

```
Internal Medicine:
  99213  Office visit, low complexity          1.30 wRVU   E&M
  99214  Office visit, moderate complexity     1.92 wRVU   E&M
  99215  Office visit, high complexity         2.80 wRVU   E&M
  99212  Office visit, straightforward         0.70 wRVU   E&M
  99395  Preventive visit, 18-39y              1.50 wRVU   Preventive
  99396  Preventive visit, 40-64y              1.60 wRVU   Preventive
  99397  Preventive visit, 65+y                1.50 wRVU   Preventive
  G0438  Annual wellness visit (initial)       2.43 wRVU   Preventive
  G0439  Annual wellness visit (subsequent)    1.50 wRVU   Preventive
  93000  ECG with interpretation               0.17 wRVU   Procedures
  36415  Routine venipuncture                  0.17 wRVU   Procedures
  94010  Spirometry                            0.26 wRVU   Procedures

Cardiology:
  99213  Office visit, low complexity          1.30 wRVU   E&M
  99214  Office visit, moderate complexity     1.92 wRVU   E&M
  99215  Office visit, high complexity         2.80 wRVU   E&M
  93306  Echo TTE complete with Doppler        4.50 wRVU   Imaging
  93307  Echo TTE without Doppler              3.33 wRVU   Imaging
  93308  Echo TTE follow-up                    1.00 wRVU   Imaging
  93015  Stress test (complete)                4.50 wRVU   Procedures
  93016  Stress test (supervision only)        0.92 wRVU   Procedures
  93017  Stress test (tracing only)            1.43 wRVU   Procedures
  93224  Holter monitor (up to 48h, recording) 0.76 wRVU   Procedures
  93226  Holter monitor (scanning analysis)    0.49 wRVU   Procedures
  93454  Diagnostic coronary angiography       14.00 wRVU  Procedures
  93000  ECG with interpretation               0.17 wRVU   Procedures
  78451  Nuclear stress test (SPECT, 1 study)  5.17 wRVU   Imaging
```

---

## Analytics Module Interfaces

Each module is a plain Python file with pure functions. No Streamlit imports. No DuckDB imports.
All functions receive DataFrames and return DataFrames, dicts, or typed objects.

```python
# analytics/cohort_engine.py
def get_cohort(
    provider_id: str,
    period: str,                 # 'YYYY-MM'
    all_providers_df: pd.DataFrame,
    provider_month_df: pd.DataFrame
) -> dict:
    """
    Returns:
        {
            'peer_ids': List[str],
            'cohort_definition': str,        # plain-language e.g. "Internal Medicine, Office, Nov 2024"
            'cohort_n': int,
            'fallback_used': bool,
            'fallback_reason': str | None
        }
    """

def get_cohort_stats(
    peer_ids: List[str],
    period: str,
    provider_month_df: pd.DataFrame,
    kpi: str = 'total_wrvu'
) -> dict:
    """
    Returns:
        {
            'median': float,
            'mean': float,
            'p25': float,
            'p75': float,
            'min': float,
            'max': float,
            'n': int
        }
    """

def get_percentile(value: float, cohort_stats: dict) -> float:
    """Returns percentile (0–100) of value within cohort distribution."""


# analytics/driver_attribution.py
def compute_drivers(
    provider_id: str,
    current_period: str,         # 'YYYY-MM'
    prior_period: str,           # 'YYYY-MM'
    charge_line_df: pd.DataFrame,
    cpt_ref_df: pd.DataFrame,
    pos_df: pd.DataFrame
) -> List[dict]:
    """
    Returns list of driver dicts, sorted by abs(contribution_wrvu) descending:
        [
            {
                'driver_name': str,          # e.g. 'Mix Effect'
                'driver_category': str,      # 'Volume' | 'Mix' | 'Setting' | 'Coding' | 'Denials' | 'Lag'
                'contribution_wrvu': float,  # signed wRVU delta
                'contribution_pct': float,   # % of total absolute change
                'direction': str,            # 'increase' | 'decrease' | 'neutral'
                'available': bool            # False if required data missing
            },
            ...
        ]
    """


# analytics/confidence_score.py
def compute_confidence(
    cohort_n: int,
    current_period_encounters: int,
    prior_period_encounters: int,
    missing_fields: List[str],    # e.g. ['denial_data', 'lag_data']
    is_partial_period: bool
) -> dict:
    """
    Returns:
        {
            'score': int,           # 0–100
            'level': str,           # 'High' | 'Moderate' | 'Low'
            'caveats': List[str]    # plain-language caveats
        }
    Scoring weights:
        cohort_n >= 5:       +40 pts  (cohort_n 3-4: +20, <3: +0)
        encounter_count sufficient (>=20/period): +30 pts
        no missing key fields:  +20 pts
        not partial period:     +10 pts
    """


# analytics/adequacy_signal.py
def compute_adequacy(
    percentile: float,
    confidence_score: int
) -> dict:
    """
    Returns:
        {
            'signal': str,          # 'green' | 'yellow' | 'red'
            'color_hex': str,       # '#1A7A4A' | '#B45309' | '#B71C1C'
            'label': str,           # 'On Track' | 'Watch' | 'Below Target'
            'rationale': str        # 1-sentence plain-language explanation
        }
    Thresholds: >75th = green, 25th–75th = yellow, <25th = red
    If confidence < 40: downgrade signal one level and note in rationale
    """


# analytics/narrative_engine.py
def generate_narrative(
    provider_name: str,
    period: str,
    current_wrvu: float,
    prior_wrvu: float,
    baseline_wrvu: float,         # 6-month rolling average
    cohort_stats: dict,
    percentile: float,
    drivers: List[dict],
    adequacy: dict,
    confidence: dict,
    use_api: bool = True,         # False if no API key
    api_key: str | None = None
) -> str:
    """
    Returns a 2–6 sentence narrative string following the 5-part template.
    If use_api=True and api_key is set: enriches template output via Claude API.
    Falls back to template-only if API call fails.
    """
```

---

## Streamlit App Architecture

### Entry Point: `app/main.py`

```python
# Responsibilities:
# 1. st.set_page_config() — title, layout='wide', page_icon='🏥'
# 2. Load .env (ANTHROPIC_API_KEY)
# 3. Open DuckDB connection (stored in st.session_state)
# 4. Load and cache all DataFrames at startup
# 5. Render sidebar: app title, "Data as of" timestamp, role selector (David / Provider)
# 6. Route to correct page based on st.session_state.epd_current_page
#    - Default: opportunity_dashboard
#    - After row click: provider_drilldown
```

### Page: `app/pages/opportunity_dashboard.py`

```
Layout (wide):
├── Header row: "Provider Opportunity Dashboard" | "Data as of [date]"
├── Filter row: [Specialty dropdown] [Period dropdown]
├── Summary tiles (3): Total Providers | Avg Opportunity Score | Specialties Shown
└── Ranked table (st.dataframe with custom formatting):
    Columns: Rank | Provider | Specialty | Period wRVU | Peer Median | Gap | Score | Top Driver | Confidence
    On row click → set st.session_state.epd_selected_provider → navigate to drilldown
```

### Page: `app/pages/provider_drilldown.py`

```
Layout (wide):
├── Breadcrumb: "← Back to Dashboard" button
├── Provider header: Name | Specialty | Site | Period selector
├── Row 1 — KPI tiles: wRVU | Encounters | Adequacy badge | Peer percentile
├── Row 2 — Two columns:
│   ├── Left (60%): 12-month wRVU trend chart (Plotly line + 6mo avg line)
│   └── Right (40%): Cohort definition card (n, definition, fallback warning if applicable)
├── "Explain Performance" expander (auto-open):
│   ├── Narrative text block (styled box)
│   ├── Driver chart (horizontal Plotly bar)
│   └── Evidence tabs: [CPT Mix] [E&M Levels] [Site of Service]
└── Suggested Interventions (3 cards in columns): Role tag | Rationale | Evidence link
```

---

## Color Palette & Design Tokens

```python
# app/config.py

COLORS = {
    'meridian_blue':    '#012169',
    'meridian_gold':    '#F2A900',
    'green':         '#1A7A4A',
    'green_light':   '#D6F0E3',
    'yellow':        '#B45309',
    'yellow_light':  '#FEF3C7',
    'red':           '#B71C1C',
    'red_light':     '#FDECEA',
    'gray_dark':     '#444444',
    'gray_light':    '#F5F5F5',
    'white':         '#FFFFFF',
}

ADEQUACY_CONFIG = {
    'green':  {'hex': '#1A7A4A', 'label': 'On Track',      'bg': '#D6F0E3'},
    'yellow': {'hex': '#B45309', 'label': 'Watch',          'bg': '#FEF3C7'},
    'red':    {'hex': '#B71C1C', 'label': 'Below Target',   'bg': '#FDECEA'},
}

DB_PATH = "data/meridian_poc.duckdb"
DEFAULT_PERIOD = "2024-11"       # Most recent complete month in synthetic dataset
BASELINE_MONTHS = 6             # Rolling average window
MIN_COHORT_SIZE = 3             # Below this: warning shown
```

---

## API Integration Pattern

```python
# In narrative_engine.py

import anthropic
import os

SYSTEM_PROMPT = """You are a healthcare revenue cycle analyst helping physician leaders
understand provider performance. You receive structured data about a provider's performance
and must write a clear, concise, evidence-based narrative explanation (3–5 sentences).

Rules:
- Use plain language (no jargon without explanation)
- Be factual and specific — cite actual numbers
- Never suggest upcoding; frame as 'appropriate documentation'
- Follow this structure: (1) outcome, (2) peer comparison, (3) top drivers, (4) confidence note, (5) next steps
- Tone: professional, non-judgmental, actionable"""

def call_claude_api(template_text: str, api_key: str) -> str:
    """Enriches template narrative via Claude API. Falls back to template on any error."""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Refine this narrative:\n\n{template_text}"}]
        )
        return message.content[0].text
    except Exception:
        return template_text   # Silent fallback to template
```

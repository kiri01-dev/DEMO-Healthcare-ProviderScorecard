# CLAUDE.md — Meridian Physician Division Provider Performance App (PoC)

> **Read this file first in every Claude Code session before touching any code.**

---

## What This App Is

A desktop PoC for the Meridian Physician Division Provider Performance & Revenue Optimization App. It gives David Reyes (VP/CFO) a ranked view of provider performance opportunities, and lets any provider see why their RVUs changed and what to do about it — in under 2 minutes, without an analyst.

**Stack:** Python · Streamlit · DuckDB · Plotly · Anthropic Claude API
**Data:** Synthetic — Internal Medicine (10 providers) + Cardiology (10 providers) · 12 months (Jan–Dec 2024)
**Platform:** Windows desktop

---

## Folder Structure (final state after all 3 sessions)

```
APP_Emory PoC v2/
│
├── CLAUDE.md                         ← This file. Read first.
├── PRD_PoC.md                        ← PoC-scoped product requirements (reference)
├── ARCHITECTURE.md                   ← DuckDB schema, module map, data flow (reference)
├── SPEC_01_SETUP_AND_DATA.md         ← Session 1 instructions
├── SPEC_02_ANALYTICS.md              ← Session 2 instructions
├── SPEC_03_APP.md                    ← Session 3 instructions
│
├── requirements.txt                  ← Python dependencies (created in Session 1)
├── .env.example                      ← API key template (created in Session 1)
├── .env                              ← Your actual API key (never commit)
├── RUNME.md                          ← One-command launch instructions (created in Session 3)
│
├── data/
│   ├── meridian_poc.duckdb           ← All synthetic data (created in Session 1)
│   └── exports/                      ← CSV exports for debugging
│
├── scripts/
│   ├── generate_synthetic_data.py    ← Generates all synthetic data (Session 1)
│   └── load_to_duckdb.py             ← Loads parquet → DuckDB (Session 1)
│
├── analytics/
│   ├── __init__.py
│   ├── cohort_engine.py              ← Peer cohort selection + benchmarking (Session 2)
│   ├── driver_attribution.py         ← Variance decomposition (Session 2)
│   ├── confidence_score.py           ← Confidence scoring (Session 2)
│   ├── adequacy_signal.py            ← Green/Yellow/Red signal (Session 2)
│   └── narrative_engine.py           ← Template + Claude API narratives (Session 2)
│
├── app/
│   ├── main.py                       ← Streamlit entry point (Session 3)
│   ├── config.py                     ← Constants, color palette, DuckDB path
│   ├── pages/
│   │   ├── opportunity_dashboard.py  ← David's ranked opportunity view (Session 3)
│   │   └── provider_drilldown.py     ← Provider Report Card + Explanation (Session 3)
│   └── components/
│       ├── kpi_tiles.py              ← KPI metric tiles widget
│       ├── adequacy_badge.py         ← Green/Yellow/Red badge + confidence
│       ├── driver_chart.py           ← Horizontal bar chart for drivers
│       └── evidence_panels.py        ← Tabbed evidence drill-downs
│
├── tests/
│   └── smoke_test.py                 ← Validates all 20 providers return outputs (Session 3)
│
└── [PRD v2.0 .docx]                  ← Full PRD reference document
```

---

## Execution Order

Execute one spec file per Claude Code session, in order. Do not skip ahead.

| Session | Spec File | What Gets Built | Est. Time |
|---------|-----------|-----------------|-----------|
| **1** | `SPEC_01_SETUP_AND_DATA.md` | Python env, synthetic data generation, DuckDB load + validation | ~45 min |
| **2** | `SPEC_02_ANALYTICS.md` | Cohort engine, driver attribution, confidence scoring, narrative engine | ~60 min |
| **3** | `SPEC_03_APP.md` | Streamlit UI (2 pages), smoke tests, RUNME.md | ~60 min |

**Before starting Session 2:** Verify `data/meridian_poc.duckdb` exists and Session 1 validation passed.
**Before starting Session 3:** Verify Session 2 unit tests pass (`python -m pytest tests/ -v`).

---

## Key Conventions (apply in all sessions)

### Paths
- All paths are relative to the `APP_Emory PoC v2/` root directory.
- DuckDB file: `data/meridian_poc.duckdb`
- Use `pathlib.Path` for all file paths — never hardcode Windows backslashes.

### Environment Variables
- `ANTHROPIC_API_KEY` — Claude API key for narrative enrichment.
- Set in `.env` file at project root. Load with `python-dotenv`.
- App must work without this key (falls back to template-only narratives).

### Python Version
- Python 3.10+ required. Use type hints throughout.

### DuckDB
- Single connection pattern: open connection at startup, close on shutdown.
- All queries use parameterized statements (no f-string SQL).
- Database is read-only during app runtime (writes only during Session 1).

### Streamlit
- `st.set_page_config()` must be the first Streamlit call in `main.py`.
- Use `st.cache_data` for all DuckDB query results (TTL = 3600s).
- Session state key prefix: `epd_` (e.g., `st.session_state.epd_selected_provider`).

### Code Style
- All functions have docstrings.
- All analytics functions are pure (no side effects, no DB calls inside them).
- DB calls happen in a separate `data_layer.py` module; analytics functions receive DataFrames.

### Error Handling
- Never show raw Python tracebacks to the user.
- Use `st.error()` for user-facing errors with plain-language messages.
- Data quality issues show warnings (`st.warning()`), not errors.

---

## How to Run the App (after all 3 sessions are complete)

```bash
# Windows PowerShell
cd "APP_Emory PoC v2"
.\.venv\Scripts\Activate.ps1
streamlit run app/main.py
```

The app opens automatically in your default browser at `http://localhost:8501`.

---

## Reference Documents

- `PRD_PoC.md` — What the app must do and why
- `ARCHITECTURE.md` — DuckDB schema, data model, module interfaces
- `PRD v2.0 — Meridian Physician Division...docx` — Full PRD with all background context

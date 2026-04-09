# PRD — PoC Scope
## Meridian Physician Division Provider Performance & Revenue Optimization App

> **This document scopes the PoC only (Phase 0).** For the full product PRD, see `PRD v2.0 — Meridian Physician Division...docx`.
> Use this document as the authoritative requirements reference during all 3 build sessions.

---

## What the PoC Must Prove

The PoC uses synthetic data to validate four things before connecting to live Meridian systems:

1. **Cohort logic** produces fair, defensible peer benchmarks with transparent definitions.
2. **Driver attribution** correctly decomposes period-over-period performance changes into ranked, quantified drivers.
3. **Narrative generation** produces interpretable, trustworthy explanations that a CFO and a physician would both find credible.
4. **Desktop UX** meets the self-service goal — a user can answer "Why did this provider's RVUs change?" in under 2 minutes.

---

## PoC Constraints

| Parameter | Value |
|-----------|-------|
| Specialties | Internal Medicine, Cardiology |
| Providers | 20 total (10 per specialty) |
| Time period | Jan 2024 – Dec 2024 (12 months) |
| Data | Synthetic only — no live Meridian data |
| Primary KPI | wRVU (work Relative Value Units) |
| Primary demo user | David Reyes (VP/CFO) |
| Platform | Windows desktop, Streamlit app |

---

## Problem Statement (PoC Context)

Meridian physician leaders can see metrics in existing tools (Epic, Strata, Power BI) but cannot answer three questions without analyst support:

- **Is this provider's performance adequate?** (vs. fair peers)
- **Why did it change?** (ranked drivers with evidence)
- **What should be done?** (role-tagged, evidence-linked actions)

The PoC demonstrates that all three questions can be answered automatically, transparently, and self-service.

---

## PoC Objectives

| # | Objective | Demo Acceptance Test |
|---|-----------|----------------------|
| O1 | Provider self-service | David selects any provider; app returns narrative + drivers + peer comparison in <2 seconds |
| O2 | Defensible adequacy signal | Every provider-period shows Green/Yellow/Red + confidence score + cohort definition |
| O3 | Fair peer benchmarking | Cohort definition shown in plain language; cohort size n always visible |
| O4 | Actionable interventions | 2–3 role-tagged suggestions per provider, each with rationale and evidence link |

---

## Scope: PoC Boundary

### In Scope (build these)

- **Opportunity Dashboard** (David/Leadership view) — ranked provider table by opportunity score, with filters and drill-through
- **Provider Report Card** — KPI snapshot, adequacy signal, peer percentile, 12-month trend
- **Explanation View** — narrative block, top drivers chart, evidence panels (CPT mix, E&M levels, site-of-service)
- **Intervention Suggestions** — 2–3 role-tagged suggestions per provider
- **Synthetic data** for Internal Medicine and Cardiology (20 providers, 12 months)
- **Daily refresh simulation** — "Data as of" timestamp; data loaded at app startup

### Out of Scope (PoC)

- Live Meridian data connection
- Provider login / authentication (demo mode only — role selector in sidebar)
- Intervention tracking (Suggested → Reviewed → Actioned workflow)
- Denial drill-down panel (data may be included in synthetic set but UI panel is P1)
- Export to PDF/CSV
- Free-text "Ask Why" input (structured templates only)

---

## Personas (PoC Focus)

### Primary: David / Finance Leadership
**Job:** "Show me the biggest revenue opportunities ranked by provider, with confidence in the data."

Key tasks for demo:
1. Open Opportunity Dashboard → see all 20 providers ranked by opportunity score
2. Filter by specialty (Internal Medicine vs. Cardiology)
3. Click into a provider → view Report Card + narrative explanation
4. Review suggested interventions

### Secondary: Provider (drill-down only)
**Job:** "Tell me if my performance is on track and why it changed."

Key tasks for demo:
1. From Opportunity Dashboard, click a specific provider
2. View their Report Card (KPIs + adequacy signal + peer percentile)
3. Read the narrative explanation ("Why did my RVUs change?")
4. See top drivers with evidence panels

---

## Core User Journey (PoC Demo Script)

```
1. David opens app → Opportunity Dashboard loads
2. David sees 20 providers ranked by opportunity score (wRVU gap × confidence)
3. David applies specialty filter → "Cardiology"
4. David clicks provider "Dr. Sarah Chen" (highest opportunity in Cardiology)
5. App navigates to Provider Report Card
6. David sees: wRVU = 380 (Yellow — 34th percentile vs. 8 peers)
7. David clicks "Explain Performance"
8. App displays:
   - Narrative: "In Nov 2024, Dr. Chen's wRVUs were 380, down 14% from her 6-month baseline of 442..."
   - Top drivers: Mix Effect (−52 wRVU), Volume Effect (−22 wRVU), Setting Effect (+12 wRVU)
   - Evidence panels: CPT mix shift tab (echo volume down), E&M tab
   - Suggestions: [Coding] Review echo documentation completeness; [Provider] Schedule with department chief
9. David returns to dashboard, clicks an Internal Medicine provider
10. Sees different driver profile (E&M level downshift as primary driver)
```

---

## Functional Requirements (PoC — P0 Only)

### FR-1: Opportunity Dashboard

- Ranked table of all 20 providers, sorted by opportunity score (descending)
- Columns: Provider Name, Specialty, Period, wRVU (actual), wRVU (peer median), Gap, Opportunity Score, Primary Driver Category, Confidence
- Filters: Specialty (All / Internal Medicine / Cardiology), Period (month selector, default = most recent complete month)
- Click any row → navigate to Provider Report Card
- "Data as of [timestamp]" displayed in header

### FR-2: Provider Report Card

- KPI tiles: wRVU (current period), Encounters, Charges (if available), Denial Rate (if available)
- Trend chart: 12-month wRVU time series with 6-month rolling average line
- Adequacy badge: Green / Yellow / Red with confidence score (0–100) and 1-sentence rationale
- Peer comparison widget: percentile rank + delta vs. cohort median + cohort definition + n
- "Explain Performance" button → triggers Explanation View

### FR-3: Explanation View

- Narrative block: 2–6 sentences following the 5-part template (outcome → peer comparison → top drivers → confidence/caveats → actions)
- Driver chart: horizontal bars showing top 3–5 drivers ranked by contribution (wRVU delta)
- Evidence panel tabs:
  - **CPT Mix**: table of top CPT families, volume prior vs. current, wRVU delta
  - **E&M Levels**: bar chart of 99213/99214/99215 distribution, prior vs. current period
  - **Site of Service**: distribution shift (Office / Hospital Outpatient / ASC)
- Suggested interventions: 2–3 cards, each showing role tag (Provider / Coding / Ops), 1-sentence rationale, evidence link

### FR-4: Data Quality & Confidence

- Confidence score shown on every provider-period view
- If cohort n < 3: show warning "Small cohort — comparison may not be statistically reliable"
- "Data as of [date]" timestamp on all views
- Missing data fields: show "Not available" state, not blank or error

---

## Success Criteria for PoC Demo

| Metric | Target |
|--------|--------|
| All 20 providers return complete Report Cards | 100% |
| Narrative generation succeeds for all providers | 100% |
| App loads Opportunity Dashboard | <3 seconds |
| Provider Report Card loads after click | <2 seconds |
| David can complete demo journey (steps 1–10 above) | Without error |
| Cohort definition visible on every provider view | 100% |
| No raw Python errors shown to user | 0 errors |

---

## Key Terminology

| Term | Definition |
|------|------------|
| wRVU | Work Relative Value Unit — the physician work component of Medicare's RBRVS. Primary productivity metric for this PoC. |
| Opportunity Score | Composite score = (wRVU gap from peer median) × confidence score. Used to rank providers in the dashboard. |
| Cohort | Peer providers with same specialty + setting + time period used for benchmarking. |
| Driver | A quantified contributor to a period-over-period wRVU change (e.g., Volume Effect = −22 wRVU). |
| Adequacy Signal | Green (>75th percentile) / Yellow (25th–75th) / Red (<25th) badge indicating performance vs. peers. |
| Confidence Score | 0–100 composite score based on cohort size, data completeness, and statistical stability. |
| E&M | Evaluation & Management CPT codes (99202–99215) — the primary visit-based codes for office encounters. |

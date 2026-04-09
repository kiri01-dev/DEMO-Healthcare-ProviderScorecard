# How to Run the Meridian Provider Performance App

## Prerequisites
- Python 3.10+ installed
- Sessions 1-3 completed (data generated, analytics built, app built)

## Launch (Windows PowerShell)

```powershell
# 1. Navigate to the app folder
cd "APP_Emory PoC v2"

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Set your API key (first time only)
#    Copy .env.example to .env and add your ANTHROPIC_API_KEY

# 4. Launch the app
streamlit run app/main.py
```

The app opens automatically at: **http://localhost:8501**

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ExecutionPolicy` error | Run: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| `duckdb.IOException: data/meridian_poc.duckdb not found` | Run Session 1 first: `python scripts/generate_synthetic_data.py` then `python scripts/load_to_duckdb.py` |
| `ModuleNotFoundError` | Ensure venv is activated: `.\.venv\Scripts\Activate.ps1` |
| Blank narrative | Check `.env` has valid `ANTHROPIC_API_KEY`, or app works without it (template mode) |
| Port 8501 in use | `streamlit run app/main.py --server.port 8502` |

## Demo Script (for David Reyes walkthrough)
1. Open app -> **Opportunity Dashboard** loads with all 20 providers ranked by opportunity score
2. Filter by **Cardiology** -> see 10 providers
3. Select any **Below Target** provider from the dropdown -> opens Provider Report Card
4. View KPI tiles, trend chart, and adequacy signal
5. Expand **Explain Performance** -> read narrative + driver chart + evidence tabs
6. Scroll to **Suggested Interventions** -> review 2-3 role-tagged action cards
7. Click **Back to Opportunity Dashboard** -> returns to ranked view
8. Change filter to **Internal Medicine** -> explore a different provider
See latest startup instructions below
cd "APP_Emory PoC v2"
.\.venv\Scripts\Activate.ps1
streamlit run app/main.py

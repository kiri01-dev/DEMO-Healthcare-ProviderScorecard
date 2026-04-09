"""App-wide constants and configuration."""
from pathlib import Path

# Paths
ROOT_DIR = Path(__file__).parent.parent
DB_PATH = str(ROOT_DIR / "data" / "meridian_poc.duckdb")
DATA_DIR = ROOT_DIR / "data"

# Data settings
DEFAULT_PERIOD = "2024-11"
PRIOR_PERIOD = "2024-10"
BASELINE_MONTHS = 6
ALL_PERIODS = [f"2024-{m:02d}" for m in range(1, 13)]

# Cohort settings
MIN_COHORT_SIZE = 3
COHORT_WARNING_SIZE = 5

# Adequacy thresholds (percentile)
ADEQUACY_THRESHOLDS = {"green": 75.0, "yellow": 25.0}  # >75=green, 25-75=yellow, <25=red

# Confidence score weights
CONFIDENCE_WEIGHTS = {
    "cohort_adequate":    40,   # cohort_n >= 5
    "cohort_minimal":     20,   # cohort_n 3-4
    "volume_adequate":    30,   # >= 20 encounters per period
    "fields_complete":    20,   # no missing key fields
    "full_period":        10,   # not a partial month
}

# Colors
COLORS = {
    "meridian_blue":   "#012169",
    "meridian_gold":   "#F2A900",
    "green":        "#1A7A4A",
    "green_light":  "#D6F0E3",
    "yellow":       "#B45309",
    "yellow_light": "#FEF3C7",
    "red":          "#B71C1C",
    "red_light":    "#FDECEA",
    "gray_dark":    "#444444",
    "gray_light":   "#F5F5F5",
    "white":        "#FFFFFF",
}

ADEQUACY_CONFIG = {
    "green":  {"hex": "#1A7A4A", "label": "On Track",     "bg": "#D6F0E3"},
    "yellow": {"hex": "#B45309", "label": "Watch",         "bg": "#FEF3C7"},
    "red":    {"hex": "#B71C1C", "label": "Below Target",  "bg": "#FDECEA"},
}

DRIVER_COLORS = {
    "Volume":   "#012169",
    "Mix":      "#F2A900",
    "Setting":  "#1A7A4A",
    "Coding":   "#B45309",
    "Denials":  "#B71C1C",
    "Lag":      "#888888",
}

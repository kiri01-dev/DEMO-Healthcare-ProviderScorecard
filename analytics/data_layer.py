"""
Data access layer — all DuckDB queries live here.
All functions accept a duckdb.DuckDBPyConnection and return pd.DataFrame or simple values.
No other module queries the database directly.
"""
import duckdb
import pandas as pd
from app.config import DB_PATH


def get_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Open and return a DuckDB connection."""
    return duckdb.connect(DB_PATH, read_only=read_only)


def load_all_providers(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all providers from dim_provider."""
    return con.execute("SELECT * FROM dim_provider ORDER BY provider_id").df()


def load_provider_month_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all rows from v_provider_month (pre-aggregated monthly KPIs)."""
    return con.execute(
        "SELECT * FROM v_provider_month ORDER BY provider_id, service_month"
    ).df()


def load_cpt_mix(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return CPT mix data for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_cpt_mix_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months,
    ).df()


def load_pos_mix(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return place-of-service mix for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_pos_mix_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months,
    ).df()


def load_denial_summary(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return denial summary for a provider over specified months."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"SELECT * FROM v_denial_month WHERE provider_id = ? AND service_month IN ({placeholders})",
        [provider_id] + months,
    ).df()


def load_em_distribution(con: duckdb.DuckDBPyConnection, provider_id: str, months: list[str]) -> pd.DataFrame:
    """Return E&M level distribution for a provider (99212-99215 family only)."""
    placeholders = ",".join(["?" for _ in months])
    return con.execute(
        f"""
        SELECT service_month, cpt_code, SUM(total_units) AS units, SUM(total_wrvu) AS total_wrvu
        FROM v_cpt_mix_month
        WHERE provider_id = ?
          AND service_month IN ({placeholders})
          AND cpt_code IN ('99212','99213','99214','99215')
        GROUP BY service_month, cpt_code
        ORDER BY service_month, cpt_code
        """,
        [provider_id] + months,
    ).df()

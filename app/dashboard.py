import os
from datetime import date
from typing import Tuple, Optional, Dict

from dotenv import load_dotenv
load_dotenv()   # ← CHARGE AUTOMATIQUEMENT LE .env À LA RACINE DU PROJET

import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st


# ---------- DB CONNECTION ----------

def get_engine():
    """Create a SQLAlchemy engine from SUPABASE_DB_URL."""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL is not set in environment variables.")
    return create_engine(db_url)


# ---------- CACHED QUERIES ----------

@st.cache_data(show_spinner=False)
def get_symbols() -> list[str]:
    """Fetch distinct symbols from fact_prices_enriched."""
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT symbol
        FROM fact_prices_enriched
        ORDER BY symbol;
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["symbol"].tolist()


@st.cache_data(show_spinner=False)
def get_global_date_bounds() -> Tuple[date, date]:
    """Get global min/max dates available in fact_prices_enriched."""
    engine = get_engine()
    query = text(
        """
        SELECT
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM fact_prices_enriched;
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    min_d = df.loc[0, "min_date"]
    max_d = df.loc[0, "max_date"]

    # min_d / max_d should already be datetime.date
    return min_d, max_d


@st.cache_data(show_spinner=False)
def get_price_data(symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Load prices and enriched metrics for a given symbol and date range
    from fact_prices_enriched.
    """
    engine = get_engine()
    query = text(
        """
        SELECT
            date,
            close,
            daily_return_pct,
            ma_7d,
            ma_30d
        FROM fact_prices_enriched
        WHERE symbol = :symbol
          AND date BETWEEN :start_date AND :end_date
        ORDER BY date;
        """
    )
    params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
    }

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    # Ensure correct dtypes
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    return df


# ---------- RISK METRICS ----------

def compute_risk_metrics(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Compute basic risk metrics from daily_return_pct:
    - annualized volatility
    - annualized Sharpe ratio (risk-free = 0)
    - max drawdown

    Assumes daily_return_pct is expressed in percent.
    """
    if df.empty or "daily_return_pct" not in df.columns:
        return {"ann_vol": None, "sharpe": None, "max_dd": None}

    # Drop missing returns
    returns_pct = df["daily_return_pct"].dropna()
    if len(returns_pct) < 2:
        return {"ann_vol": None, "sharpe": None, "max_dd": None}

    # Convert percent to decimal returns (e.g. 1.0 -> 0.01)
    returns = returns_pct / 100.0

    # Annualized volatility (252 trading days)
    daily_vol = returns.std()
    ann_vol = daily_vol * (252 ** 0.5)

    # Sharpe ratio (risk-free rate = 0)
    daily_mean = returns.mean()
    if daily_vol > 0:
        sharpe = daily_mean / daily_vol * (252 ** 0.5)
    else:
        sharpe = None

    # Max drawdown
    wealth_index = (1 + returns).cumprod()
    previous_peaks = wealth_index.cummax()
    drawdowns = wealth_index / previous_peaks - 1
    max_dd = drawdowns.min()  # negative number

    return {"ann_vol": float(ann_vol), "sharpe": sharpe, "max_dd": float(max_dd)}


# ---------- STREAMLIT APP ----------

st.set_page_config(
    page_title="Data Finance Pipeline – Prices Dashboard",
    layout="wide",
)

st.sidebar.header("Filters")

# Load symbols and global date range
symbols = get_symbols()
global_min_date, global_max_date = get_global_date_bounds()

# Sidebar inputs
selected_symbol = st.sidebar.selectbox("Symbol", symbols)

start_date = st.sidebar.date_input(
    "Start date",
    value=global_min_date,
    min_value=global_min_date,
    max_value=global_max_date,
)

end_date = st.sidebar.date_input(
    "End date",
    value=global_max_date,
    min_value=global_min_date,
    max_value=global_max_date,
)

if start_date > end_date:
    st.sidebar.error("Start date must be before end date.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.markdown("Data source: `fact_prices_enriched`.")

# Title and intro
st.markdown(
    """
# 📈 Data Finance Pipeline – Prices Dashboard

This dashboard reads data directly from **Supabase**:

- Daily ingestion: **Airflow → raw_prices**
- Modeling: **dbt → fact tables**
- Visualization: **this Streamlit dashboard**
"""
)

# Load data for filters
df_prices = get_price_data(selected_symbol, start_date, end_date)

if df_prices.empty:
    st.warning("No data available for this period / symbol.")
    st.stop()

# ---------- PRICE KPIs ----------

start_price = float(df_prices["close"].iloc[0])
end_price = float(df_prices["close"].iloc[-1])
performance_pct = (end_price / start_price - 1.0) * 100.0
highest_close = float(df_prices["close"].max())
lowest_close = float(df_prices["close"].min())

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Symbol", selected_symbol)

with col2:
    st.metric("Start price", f"{start_price:,.2f}")

with col3:
    st.metric("End price", f"{end_price:,.2f}")

with col4:
    st.metric("Performance", f"{performance_pct:,.2f} %")


# ---------- RISK KPIs (NEW SECTION) ----------

risk = compute_risk_metrics(df_prices)
col_r1, col_r2, col_r3 = st.columns(3)

def _format_or_dash(val, fmt: str) -> str:
    if val is None:
        return "–"
    return fmt.format(val)

with col_r1:
    st.metric(
        "Annualized volatility",
        _format_or_dash(risk["ann_vol"], "{:,.2%}"),
    )

with col_r2:
    st.metric(
        "Sharpe ratio (ann.)",
        _format_or_dash(risk["sharpe"], "{:,.2f}"),
    )

with col_r3:
    st.metric(
        "Max drawdown",
        _format_or_dash(risk["max_dd"], "{:,.2%}"),
    )

st.markdown("---")

# ---------- MAIN PRICE CHART ----------

st.subheader("📊 Closing price over time")

chart_df = df_prices[["date", "close"]].set_index("date")
st.line_chart(chart_df)

# ---------- MOVING AVERAGES CHART ----------

st.subheader("📉 Moving averages (7d & 30d)")

ma_df = df_prices[["date", "ma_7d", "ma_30d"]].dropna().set_index("date")
if ma_df.empty:
    st.info("Not enough data to compute moving averages for this period.")
else:
    st.line_chart(ma_df)

# ---------- UNDERLYING DATA ----------

st.subheader("🧾 Underlying data")
st.dataframe(df_prices, use_container_width=True)

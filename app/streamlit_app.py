import os
from datetime import date

import pandas as pd
import plotly.express as px
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import streamlit as st

# -----------------------------
# 1. Load env vars and connect
# -----------------------------

load_dotenv()  # Loads variables from .env at project root

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

if not SUPABASE_DB_URL:
    st.error("SUPABASE_DB_URL is missing in your .env file.")
    st.stop()


def get_connection():
    """
    Create a new psycopg2 connection using the Supabase DB URL.
    We open/close per query for simplicity.
    """
    return psycopg2.connect(SUPABASE_DB_URL, cursor_factory=RealDictCursor)


# -----------------------------
# 2. Helper queries
# -----------------------------

@st.cache_data
def get_symbols():
    """
    Return the list of available symbols from fact_prices_enriched.
    """
    query = """
        SELECT DISTINCT symbol
        FROM fact_prices_enriched
        ORDER BY symbol;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return [r["symbol"] for r in rows]


@st.cache_data
def get_date_bounds(symbol: str):
    """
    Return min and max available dates for a given symbol.
    """
    query = """
        SELECT MIN(date) AS min_date,
               MAX(date) AS max_date
        FROM fact_prices_enriched
        WHERE symbol = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            row = cur.fetchone()

    if not row or not row["min_date"] or not row["max_date"]:
        return None, None

    return row["min_date"], row["max_date"]


@st.cache_data
def get_price_data(symbol: str, start: date, end: date):
    """
    Load enriched price data for one symbol between two dates.
    You can adapt the selected columns to your exact schema.
    """
    query = """
        SELECT
            date,
            close,
            daily_return_pct,
            ma_7d,
            ma_30d
        FROM fact_prices_enriched
        WHERE symbol = %s
          AND date BETWEEN %s AND %s
        ORDER BY date;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol, start, end))
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# -----------------------------
# 3. Streamlit layout
# -----------------------------

st.set_page_config(
    page_title="Data Finance Pipeline - Dashboard",
    layout="wide",
)

st.title("📈 Data Finance Pipeline")
st.caption("Daily prices & enriched metrics from Supabase (fact_prices_enriched).")

# Sidebar filters
st.sidebar.header("Filters")

symbols = get_symbols()
if not symbols:
    st.error("No symbols found in fact_prices_enriched.")
    st.stop()

selected_symbol = st.sidebar.selectbox("Choose an asset (symbol)", symbols)

min_date, max_date = get_date_bounds(selected_symbol)
if not min_date or not max_date:
    st.error("No date range available for this symbol.")
    st.stop()

# Date range selector
start_date, end_date = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Safety check: Streamlit returns either a single date or a tuple
if isinstance(start_date, list) or isinstance(start_date, tuple):
    # Newer streamlit: returns (start, end)
    start_date, end_date = start_date
elif not end_date:
    end_date = start_date

st.sidebar.write(f"Selected range: {start_date} → {end_date}")

# Load data
df = get_price_data(selected_symbol, start_date, end_date)

if df.empty:
    st.warning("No data found for this selection.")
    st.stop()

# -----------------------------
# 4. Main content
# -----------------------------

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"Price history - {selected_symbol}")
    fig_price = px.line(
        df,
        x="date",
        y="close",
        title=f"Close price for {selected_symbol}",
    )
    st.plotly_chart(fig_price, use_container_width=True)

with col2:
    st.subheader("Quick stats")

    latest_row = df.iloc[-1]
    st.metric("Last close", f"{latest_row['close']:.2f}")

    if "daily_return_pct" in df.columns and df["daily_return_pct"].notna().any():
        last_ret = latest_row["daily_return_pct"]
        st.metric("Last daily return (%)", f"{last_ret:.2f}")
    else:
        st.write("No daily_return_pct available.")

    st.write("Rows in selection:", len(df))

st.markdown("---")

# Moving averages chart (if present)
if "ma_7d" in df.columns and "ma_30d" in df.columns:
    st.subheader("Moving averages")
    fig_ma = px.line(
        df,
        x="date",
        y=["close", "ma_7d", "ma_30d"],
        title=f"Close & Moving Averages for {selected_symbol}",
    )
    st.plotly_chart(fig_ma, use_container_width=True)
else:
    st.info("Moving averages columns (ma_7d, ma_30d) not found in the table.")

st.subheader("Raw data")
st.dataframe(df, use_container_width=True)

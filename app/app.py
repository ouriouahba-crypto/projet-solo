import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime

st.sidebar.markdown(
    f"**Last updated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
)

# ---------- Page config ----------
st.set_page_config(
    page_title="Data Finance Pipeline",
    page_icon="📊",
    layout="wide",
)

# ---------- Styling ----------
# Small CSS tweaks for a more "premium" look
st.markdown(
    """
    <style>
    /* Global background */
    .stApp {
        background: radial-gradient(circle at top left, #101729, #050713 60%);
        color: #f5f5f5;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #050713;
        border-right: 1px solid #1f2937;
    }

    /* Metric cards */
    .kpi-card {
        padding: 1.2rem 1.4rem;
        border-radius: 1.4rem;
        background: linear-gradient(135deg, #050b18, #101729);
        border: 1px solid #1f2937;
        box-shadow: 0 18px 45px rgba(0, 0, 0, 0.45);
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #9ca3af;
        margin-bottom: 0.25rem;
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 600;
        color: #f9fafb;
    }
    .kpi-sub {
        font-size: 0.9rem;
        color: #9ca3af;
    }

    /* Hero header */
    .hero {
        padding: 1.6rem 2rem;
        border-radius: 1.8rem;
        background: radial-gradient(circle at top left, #1b2436, #050713);
        border: 1px solid #273549;
        margin-bottom: 1.5rem;
        box-shadow: 0 24px 60px rgba(0, 0, 0, 0.55);
    }
    .hero-title {
        font-size: 2.1rem;
        font-weight: 700;
        letter-spacing: 0.03em;
    }
    .hero-subtitle {
        font-size: 0.98rem;
        color: #9ca3af;
    }

    /* Small badge */
    .pill {
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        padding: 0.25rem 0.7rem;
        border-radius: 999px;
        font-size: 0.78rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #9ca3af;
        background: rgba(15, 23, 42, 0.8);
        border: 1px solid #1f2937;
        margin-bottom: 0.7rem;
    }

    /* Footer */
    .footer {
        font-size: 0.8rem;
        color: #9ca3af;
        margin-top: 1.5rem;
        text-align: center;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- Environment / DB ----------
# Local: uses .env | Streamlit Cloud: uses Secrets
load_dotenv()

def get_db_url() -> str:
    # Streamlit Cloud -> Secrets
    if "SUPABASE_DB_URL" in st.secrets:
        return st.secrets["SUPABASE_DB_URL"]
    # Local dev -> .env
    return os.getenv("SUPABASE_DB_URL", "")

db_url = get_db_url()

if not db_url:
    st.error("SUPABASE_DB_URL is missing. Set it in Streamlit Secrets (preferred) or in your local .env.")
    st.stop()

# Force SSL for Supabase + avoid stale connections
engine = create_engine(
    db_url,
    connect_args={"sslmode": "require", "connect_timeout": 10},
    pool_pre_ping=True,
)

@st.cache_data(show_spinner="Loading data from Supabase…")
def load_data() -> pd.DataFrame:
    """
    Load enriched prices from Supabase (fact_prices_enriched).
    """
    query = """
        SELECT
            date            AS dt,
            symbol,
            close,
            daily_return_pct,
            ma_7d,
            ma_30d
        FROM public.fact_prices_enriched
        ORDER BY dt
    """
    df = pd.read_sql(query, engine)
    df["dt"] = pd.to_datetime(df["dt"])
    return df


# Load once (cached)
df = load_data()

if df.empty:
    st.error("No data returned from fact_prices_enriched.")
    st.stop()

# ---------- Sidebar filters ----------
st.sidebar.markdown("### ⚙️ Filters")

symbols = sorted(df["symbol"].unique())
default_symbol = "AAPL" if "AAPL" in symbols else symbols[0]
symbol = st.sidebar.selectbox("Asset (symbol)", symbols, index=symbols.index(default_symbol))

min_date = df["dt"].min().date()
max_date = df["dt"].max().date()

date_range = st.sidebar.date_input(
    "Date range",
    (min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Streamlit returns either a single date or a tuple
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

mask_symbol = df["symbol"] == symbol
mask_dates = (df["dt"].dt.date >= start_date) & (df["dt"].dt.date <= end_date)
df_filtered = df[mask_symbol & mask_dates].copy()

if df_filtered.empty:
    st.warning("No data for this combination of symbol and date range.")
    st.stop()

# ---------- Hero + KPI section ----------
st.markdown(
    f"""
    <div class="hero">
        <div class="pill">
            <span>DATA FINANCE PIPELINE</span>
        </div>
        <div class="hero-title">📊 {symbol} – Market Analytics Dashboard</div>
        <div class="hero-subtitle">
            Daily prices & enriched metrics from Supabase, orchestrated with Airflow and modelled with dbt.<br/>
            Powered by your end-to-end data engineering pipeline.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

df_sorted = df_filtered.sort_values("dt")
last_row = df_sorted.iloc[-1]

last_close = float(last_row["close"])
last_return_pct = (
    float(last_row["daily_return_pct"])
    if pd.notnull(last_row["daily_return_pct"])
    else None
)
rows_selection = len(df_sorted)
period_days = (df_sorted["dt"].max().date() - df_sorted["dt"].min().date()).days + 1

col_kpi1, col_kpi2, col_kpi3 = st.columns(3)

with col_kpi1:
    change_str = f"{last_return_pct:+.2f}%" if last_return_pct is not None else "N/A"
    if last_return_pct is not None and last_return_pct >= 0:
        change_color = "#22c55e"  # green
    else:
        change_color = "#f97373"  # red

    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Last close</div>
            <div class="kpi-value">{last_close:,.2f}</div>
            <div class="kpi-sub" style="color:{change_color};">
                {change_str} vs previous close
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_kpi2:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Rows in selection</div>
            <div class="kpi-value">{rows_selection:,}</div>
            <div class="kpi-sub">
                From {df_sorted["dt"].min().date()} to {df_sorted["dt"].max().date()}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_kpi3:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Period length (days)</div>
            <div class="kpi-value">{period_days:,}</div>
            <div class="kpi-sub">
                Based on available trading dates
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("---")

# ---------- Charts section ----------
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader(f"Price & Moving Averages – {symbol}")

    base = alt.Chart(df_sorted).encode(
        x=alt.X("dt:T", title="Date")
    )

    price_line = base.mark_line(strokeWidth=2.2).encode(
        y=alt.Y("close:Q", title="Close"),
        tooltip=["dt:T", "close:Q"],
    )

    ma_7_line = base.mark_line(strokeDash=[4, 3]).encode(
        y="ma_7d:Q",
        color=alt.value("#f97316"),  # orange
        tooltip=["dt:T", "ma_7d:Q"],
    )

    ma_30_line = base.mark_line(strokeDash=[2, 2]).encode(
        y="ma_30d:Q",
        color=alt.value("#a855f7"),  # purple
        tooltip=["dt:T", "ma_30d:Q"],
    )

    combo_chart = alt.layer(price_line, ma_7_line, ma_30_line).interactive()

    st.altair_chart(combo_chart, use_container_width=True)

with right_col:
    st.subheader("Daily returns (%)")

    returns_chart = (
        alt.Chart(df_sorted)
        .mark_bar()
        .encode(
            x=alt.X("dt:T", title="Date"),
            y=alt.Y("daily_return_pct:Q", title="Return (%)"),
            color=alt.condition(
                "datum.daily_return_pct >= 0",
                alt.value("#22c55e"),   # green
                alt.value("#ef4444"),   # red
            ),
            tooltip=["dt:T", "daily_return_pct:Q"],
        )
        .properties(height=260)
    )

    st.altair_chart(returns_chart, use_container_width=True)

# ---------- Raw data & export ----------
st.markdown("### 🔍 Raw data")

with st.expander("See selection as a table"):
    st.dataframe(
        df_sorted[["dt", "symbol", "close", "daily_return_pct", "ma_7d", "ma_30d"]],
        use_container_width=True,
        hide_index=True,
    )

csv = df_sorted.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download CSV",
    data=csv,
    file_name=f"{symbol}_prices_enriched.csv",
    mime="text/csv",
)

# ---------- Footer ----------
st.markdown(
    """
    <div class="footer">
        Built with ❤️ using Supabase, dbt, Airflow & Streamlit – Data Finance Pipeline.
    </div>
    """,
    unsafe_allow_html=True,
)

st.sidebar.markdown("## About")
st.sidebar.markdown("""
**Data Finance Pipeline**

Educational end-to-end data engineering project:
- Data ingestion (Python)
- Storage (PostgreSQL)
- Transformation (dbt)
- Visualization (Streamlit)

**Data source:** Yahoo Finance  
**Update frequency:** Daily  
**Purpose:** Portfolio & learning project
""")

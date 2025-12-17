# Architecture

This project is organized as a simple end-to-end data pipeline:
**Ingest → Store → Model → Orchestrate → Serve**.

---

## Components

### 1) Ingestion (Python)
- Fetches daily OHLC prices per symbol (market data source).
- Uses an idempotent approach (upsert) so reruns do not duplicate data.
- Writes raw data into Supabase Postgres: `public.raw_prices`.

### 2) Storage (Supabase Postgres)
- Raw layer: `public.raw_prices`
- Operational logging: `etl_run_log` (run status, error messages, metadata)

### 3) Transformations (dbt)
dbt models transform raw data into analytics-ready tables/views:

**Staging**
- `stg_prices_raw` (view): standardizes and cleans raw schema.

**Marts**
- `dim_asset` (table): unique list of symbols + surrogate key `asset_id`.
- `dim_calendar` (table): trading dates derived from the actual dataset (prevents data loss in joins).
- `fact_prices` (table): OHLC prices linked to `asset_id` and `date`.
- `fact_prices_enriched` (view/table): returns and moving averages.

### 4) Orchestration (Airflow)
A daily DAG runs the pipeline:
1. Fetch & upsert new prices into `raw_prices`
2. Run dbt models (staging + marts)
3. Run dbt tests
4. Log status to `etl_run_log`
5. Send failure alerts (Discord webhook)

### 5) Serving (Streamlit)
The Streamlit app reads modeled data from Supabase and provides:
- Asset and date range filters
- KPIs (last close, change vs previous close, etc.)
- Price chart + moving averages
- Daily returns chart
- CSV export of filtered selection

---

## Data Model (Star Schema)

**Dimensions**
- `dim_asset(asset_id, symbol, ...)`
- `dim_calendar(date, year, month, day, day_of_week, week, ...)`

**Facts**
- `fact_prices(date, asset_id, symbol, open, high, low, close)`
- `fact_prices_enriched(...)` (derived metrics like returns and moving averages)

---

## Why `dim_calendar` is derived from trading dates
Market data contains trading days only. Building `dim_calendar` from the real dataset ensures:
- Joins do not drop rows unexpectedly
- Fact tables remain stable across refreshes
- The pipeline stays reproducible and production-like

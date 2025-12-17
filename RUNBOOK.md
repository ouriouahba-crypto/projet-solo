# Runbook

This runbook documents how to operate, validate, and troubleshoot the Data Finance Pipeline locally.

It is written as a **results-oriented checklist**, not as theory.

---

## 1) Start the stack

From the project root:

```bash
docker compose up --build
```

 Expected result:
Airflow is running
Containers start without errors

 Airflow UI:
http://localhost:8080

## 2) Run dbt models

From the dbt project directory:
```bash
cd dbt/data_finance_pipeline
dbt run
dbt test
```

 Expected result:

All models run successfully
dbt test returns PASS for all tests

## 3) Run Streamlit dashboard
From the project root:
```bash
streamlit run app/app.py
```

 Expected result:

Streamlit app opens in the browser
Historical price data is displayed
Filters, KPIs and charts are visible

## 4) Validate data in Supabase

### 4.1 Raw layer
```bash
select
  count(*) as n_raw,
  min(date) as min_date,
  max(date) as max_date
from public.raw_prices;
```

 Expected result:
n_raw is large (multi-year history)
Date range matches the ingestion period

### 4.2 Dimensions
```bash
select count(*) as n_assets from public.dim_asset;
```

Expected result:
One row per symbol

```bash
select
  count(*) as n_calendar,
  min(date),
  max(date)
from public.dim_calendar;
```
 Expected result:
One row per trading date
Date range matches raw data

### 4.3 Fact table
```bash
select
  count(*) as n_fact,
  min(date),
  max(date)
from public.fact_prices;
```
Expected result:
n_fact close to n_raw
No unexpected data loss

## 5) Recovery procedure (fact table issue)
Symptom
fact_prices contains very few rows (example: ~50)
Cause
Dimension tables out of sync
Join drops rows
Fix
```bash
cd dbt/data_finance_pipeline
dbt run --select dim_calendar --full-refresh
dbt run --select dim_asset --full-refresh
dbt run --select fact_prices --full-refresh
dbt test
```
Then validate again:
```bash
select
  (select count(*) from public.raw_prices) as n_raw,
  (select count(*) from public.fact_prices) as n_fact;
  ```

## 6) Airflow DAG failure
### Diagnostic steps
Open Airflow UI: http://localhost:8080
Open the failing DAG run
Open the failing task
Read task logs
Check .env values (Supabase credentials, webhook)
Optional run log inspection:
```bash
select *
from public.etl_run_log
order by created_at desc
limit 20;
```

## 7) Streamlit shows no data
Checklist:
.env file exists and is correct
dbt models exist in Supabase
Tables contain data
Recovery:
```bash
cd dbt/data_finance_pipeline
dbt run
```

## 8) Update documentation on GitHub
```bash
git add RUNBOOK.md
git commit -m "Add operational runbook"
git push
```
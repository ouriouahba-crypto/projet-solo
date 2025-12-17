import os
import traceback
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf
import httpx  # must be in requirements.txt


def _clean_proxies() -> None:
    """
    Remove proxy-related environment variables.
    Proxies often break HTTP calls inside containers.
    """
    proxy_vars = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
    ]
    for var in proxy_vars:
        val = os.environ.get(var)
        if val:
            print(f"[DAG] Proxy detected {var}={repr(val)} -> removing")
            os.environ.pop(var, None)


def _load_supabase_config() -> tuple[str, str]:
    """
    Load Supabase URL and service role key from environment.
    Raise an error if something is missing.
    """
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    print("SUPABASE_URL (DAG):", repr(url))
    print("SUPABASE_KEY prefix (DAG):", key[:8])

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

    return url, key


def _load_tickers() -> List[str]:
    """
    Load tickers list from environment variable TICKERS.
    Example: TICKERS=AAPL,MSFT,NVDA
    """
    raw = os.environ.get("TICKERS", "AAPL").strip()
    if not raw:
        raw = "AAPL"

    parts = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not parts:
        parts = ["AAPL"]

    print("Tickers list (from env TICKERS):", parts)
    return parts


def _normalize_target_date(logical_dt: Optional[datetime]) -> date:
    """
    Convert Airflow logical date (execution_date) into a pure date.
    If logical_dt is None (e.g. local run), use yesterday (UTC).
    """
    if logical_dt is None:
        # fallback: use yesterday in UTC
        target = (datetime.utcnow() - timedelta(days=1)).date()
        print("[WARN] No logical_date provided, fallback to yesterday:", target)
        return target

    target = logical_dt.date()
    print("Target trading date (from logical_date):", target)
    return target


def _download_one_ticker(ticker: str, target_day: date) -> pd.DataFrame:
    """
    Download data for a single ticker and a single day
    using yahoo finance start/end parameters.
    VERSION ROBUSTE : gère erreurs réseau, jours fériés, colonnes manquantes, NaN.
    """
    # yfinance: end date is exclusive, so we add +1 day
    start = target_day
    end = target_day + timedelta(days=1)

    print(f"Downloading data for {ticker} from {start} to {end}...")

    # --- try/except pour empêcher un crash sur un seul ticker ---
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            group_by="column",
            auto_adjust=False,
            progress=False,
        )
    except Exception as e:
        print(f"[ERROR] yfinance failed for {ticker} on {target_day}: {e}")
        return pd.DataFrame()

    # Aucun résultat -> jour férié / ticker inactif / bug Yahoo
    if df is None or df.empty:
        print(f"[WARN] No data downloaded for {ticker} on {target_day}.")
        return pd.DataFrame()

    # On remet l'index en colonne
    df = df.reset_index()

    # Yahoo peut parfois renvoyer un index sans colonne Date
    if "Date" not in df.columns:
        print(f"[WARN] No 'Date' column for {ticker} on {target_day}.")
        return pd.DataFrame()

    # Si Adj Close n'existe pas, on le dérive de Close
    if "Adj Close" not in df.columns:
        df["Adj Close"] = df["Close"]

    # Si MultiIndex de colonnes, on nettoie
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    # Renommage en snake_case
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    # Vérifier que les colonnes essentielles existent vraiment
    required_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    for col in required_cols:
        if col not in df.columns:
            print(f"[WARN] Missing column '{col}' for {ticker} on {target_day}.")
            return pd.DataFrame()

    # Drop des lignes avec NaN sur les prix (classique sur certains jours)
    df = df.dropna(subset=["open", "high", "low", "close", "adj_close"], how="any")
    if df.empty:
        print(f"[WARN] All rows dropped for {ticker} on {target_day} (null prices).")
        return pd.DataFrame()

    # Normalisation date + ajout du ticker
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["symbol"] = ticker

    print(f"Normalized preview for {ticker}:")
    print(df)

    return df


def _build_records(df: pd.DataFrame) -> List[Dict]:
    """
    Convert a normalized dataframe into a list of dicts for Supabase.
    """
    records: List[Dict] = []
    for _, row in df.iterrows():
        volume_val = 0
        try:
            # Si volume est NaN ou None, on met 0
            volume_val = int(row["volume"]) if pd.notna(row["volume"]) else 0
        except Exception:
            volume_val = 0

        record = {
            "symbol": str(row["symbol"]),
            "date": str(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "adj_close": float(row["adj_close"]),
            "volume": volume_val,
        }
        records.append(record)

    if records:
        print("Example record:", records[0])

    return records


def _upsert_records(url: str, key: str, records: List[Dict]) -> None:
    """
    Perform an UPSERT into Supabase via REST API on raw_prices table.
    Unique constraint is (symbol, date).
    """
    if not records:
        print("No records to upsert, skipping Supabase call.")
        return

    rest_url = url.rstrip("/") + "/rest/v1/raw_prices?on_conflict=symbol,date"

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    print("HTTP POST to:", rest_url)
    with httpx.Client(trust_env=False, timeout=10.0) as client:
        resp = client.post(rest_url, headers=headers, json=records)

        print("Supabase status code:", resp.status_code)
        print("Supabase response:", resp.text)

        # if row already exists (symbol, date), treat as success
        if resp.status_code == 409 and "already exists" in resp.text:
            print("409 conflict but row already exists -> treated as OK.")
        else:
            resp.raise_for_status()


def fetch_and_upsert(logical_dt: Optional[datetime] = None):
    """
    Main entry point for Airflow task.
    - Clean proxies
    - Load config
    - Compute target date from logical_dt
    - Load tickers list
    - Download data for each ticker for that date
    - Build records
    - Upsert into Supabase
    """
    print("=== Start fetch_and_upsert (multi-tickers, date-driven) ===")

    try:
        # 0. Remove proxies from environment
        _clean_proxies()

        # 1. Load Supabase config
        url, key = _load_supabase_config()

        # 2. Determine target trading date
        target_day = _normalize_target_date(logical_dt)

        # 3. Load tickers list
        tickers = _load_tickers()

        # 4. Download and collect data
        all_dfs: List[pd.DataFrame] = []
        for ticker in tickers:
            df_t = _download_one_ticker(ticker, target_day)
            if not df_t.empty:
                all_dfs.append(df_t)

        if not all_dfs:
            print(f"No data downloaded for any ticker on {target_day}. Exiting.")
            return

        full_df = pd.concat(all_dfs, ignore_index=True)
        print("Full normalized dataframe (all tickers):")
        print(full_df)

        # 5. Build records and upsert
        records = _build_records(full_df)
        _upsert_records(url, key, records)

        print("=== FIN OK (multi-tickers, date-driven) ===")

    except Exception as e:
        print("🔥 ERROR in fetch_and_upsert:", e)
        traceback.print_exc()
        raise

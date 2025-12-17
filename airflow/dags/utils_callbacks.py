import os
import json
import requests
import psycopg2

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")  # set in .env

def _log_to_db(dag_id, task_id, run_id, logical_date, status, details=None, error_message=None):
    if not SUPABASE_DB_URL:
        return
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.etl_run_log (dag_id, task_id, run_id, logical_date, status, ended_at, details, error_message)
                values (%s, %s, %s, %s, %s, now(), %s::jsonb, %s)
                """,
                (
                    dag_id,
                    task_id,
                    run_id,
                    logical_date,
                    status,
                    json.dumps(details or {}),
                    error_message,
                ),
            )
        conn.commit()
    finally:
        conn.close()

def _notify_discord(text):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": text}, timeout=10)
    except Exception:
        pass

def on_task_success(context):
    ti = context["ti"]
    _log_to_db(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context["run_id"],
        logical_date=str(context.get("logical_date")),
        status="success",
        details={"try_number": ti.try_number},
    )

def on_task_failure(context):
    ti = context["ti"]
    err = str(context.get("exception") or "unknown error")

    _log_to_db(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context["run_id"],
        logical_date=str(context.get("logical_date")),
        status="failed",
        details={"try_number": ti.try_number},
        error_message=err[:1800],
    )

    _notify_discord(
        f"❌ Airflow FAILED\nDAG: {ti.dag_id}\nTask: {ti.task_id}\nRun: {context.get('run_id')}\nError: {err[:500]}"
    )

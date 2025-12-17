from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from get_prices import fetch_and_upsert
from utils_callbacks import on_task_success, on_task_failure


def task_wrapper(**context):
    logical_date = context.get("logical_date") or context.get("execution_date")
    fetch_and_upsert(logical_date)


default_args = {
    "owner": "ouri",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": on_task_success,
    "on_failure_callback": on_task_failure,
    "sla": timedelta(minutes=20),
}

with DAG(
    dag_id="daily_prices",
    description="Download and upsert daily stock prices into Supabase, then run dbt.",
    default_args=default_args,
    start_date=datetime(2015, 12, 1),
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["data_finance_pipeline"],
) as dag:

    fetch_and_upsert_task = PythonOperator(
        task_id="fetch_and_upsert_prices",
        python_callable=task_wrapper,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/project/dbt && "
            "dbt run --project-dir data_finance_pipeline "
            "--profiles-dir . "
            "--select staging"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/project/dbt && "
            "dbt run --project-dir data_finance_pipeline "
            "--profiles-dir . "
            "--select marts"
        ),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "cd /opt/project/dbt && "
            "dbt test --project-dir data_finance_pipeline "
            "--profiles-dir ."
        ),
    )

    fetch_and_upsert_task >> dbt_run_staging >> dbt_run_marts >> dbt_test_all

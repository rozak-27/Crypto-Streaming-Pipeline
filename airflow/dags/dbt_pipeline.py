from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2, os

DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner":            "data-engineer",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_hourly_pipeline",
    description="Jalankan dbt transformasi tiap jam",
    schedule_interval="0 * * * *",  # tiap jam
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transform", "crypto"],
) as dag:

    # Task 1: Test koneksi database
    test_connection = BashOperator(
        task_id="test_db_connection",
        bash_command=f"{DBT_CMD} debug --profiles-dir {DBT_DIR}",
    )

    # Task 2: Run staging models
    run_staging = BashOperator(
        task_id="run_staging_models",
        bash_command=f"{DBT_CMD} run --profiles-dir {DBT_DIR} --select staging",
    )

    # Task 3: Run mart models
    run_marts = BashOperator(
        task_id="run_mart_models",
        bash_command=f"{DBT_CMD} run --profiles-dir {DBT_DIR} --select marts",
    )

    # Task 4: Run dbt tests
    run_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_DIR}",
    )

    # Task 5: Refresh continuous aggregate
    refresh_agg = BashOperator(
        task_id="refresh_continuous_aggregate",
        bash_command="""psql postgresql://pipeline_user:pipeline_secret_2024@postgres:5432/crypto_db -c "CALL refresh_continuous_aggregate('ohlcv_1hour', NOW()-INTERVAL '2 hours', NOW());" """,
    )

    # Dependency chain: test → staging → marts → tests → refresh
    test_connection >> run_staging >> run_marts >> run_tests >> refresh_agg
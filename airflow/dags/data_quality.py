from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2, logging
from datetime import timedelta

logger = logging.getLogger(__name__)

def check_data_freshness(**context):
    """Alert jika tidak ada data baru dalam 5 menit terakhir."""
    conn = psycopg2.connect(
        host="postgres", dbname="crypto_db",
        user="pipeline_user", password="pipeline_secret_2024"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol,
               MAX(time) AS last_time,
               NOW() - MAX(time) AS lag
        FROM ohlcv_1min
        GROUP BY symbol
        HAVING NOW() - MAX(time) > INTERVAL '5 minutes'
    """)
    stale = cur.fetchall()
    if stale:
        for row in stale:
            logger.warning(f"Stale data: {row[0]} last seen {row[2]} ago")
        raise ValueError(f"Stale data detected: {stale}")
    logger.info("All symbols have fresh data.")
    conn.close()

def check_price_anomaly(**context):
    """Alert jika ada harga yang berubah > 10% dalam 1 candle."""
    conn = psycopg2.connect(
        host="postgres", dbname="crypto_db",
        user="pipeline_user", password="pipeline_secret_2024"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, time, open, close,
               ABS((close - open) / NULLIF(open, 0)) AS pct_change
        FROM ohlcv_1min
        WHERE time >= NOW() - INTERVAL '1 hour'
          AND ABS((close - open) / NULLIF(open, 0)) > 0.10
        ORDER BY pct_change DESC LIMIT 10
    """)
    anomalies = cur.fetchall()
    if anomalies:
        logger.warning(f"Price anomalies found: {anomalies}")
    else:
        logger.info("No price anomalies detected.")
    conn.close()

with DAG(
    dag_id="data_quality_checks",
    schedule_interval="*/15 * * * *",  # tiap 15 menit
    start_date=days_ago(1),
    catchup=False,
    tags=["quality", "monitoring"],
) as dag:

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_data_freshness,
    )
    anomaly = PythonOperator(
        task_id="check_anomaly",
        python_callable=check_price_anomaly,
    )

    freshness >> anomaly
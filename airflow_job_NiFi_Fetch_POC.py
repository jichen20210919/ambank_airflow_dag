"""
Airflow DAG: Trigger NiFi Data Fetch Flow
Author: Ambank POC Team
Date: 2026-02-06

Corresponding curl test command:
curl -X POST http://cloudera-worker-2.internal:9080/api/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "mis006_etl",
    "run_date": "2026-02-06",
    "source_path": "/data/airflow-data/",
    "batch_id": "20260206001"
  }'
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'job_NiFi_Fetch_POC',
    default_args=default_args,
    description='Trigger NiFi to fetch data from source system',
    schedule_interval=None,  # Manual trigger, no automatic scheduling
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['nifi', 'fetch', 'mis006'],
)

# Trigger NiFi API
# Note: Configure Airflow Connection 'nifi_connection' first
# Host: cloudera-worker-2.internal
# Port: 9080
# Type: HTTP
trigger_nifi = SimpleHttpOperator(
    task_id='trigger_nifi_fetch',
    http_conn_id='nifi_connection',
    endpoint='/api/trigger',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data=json.dumps({
        "job_name": "mis006_etl",
        "run_date": "{{ ds }}",  # Airflow execution date, format: YYYY-MM-DD
        "source_path": "/data/airflow-data/",
        "batch_id": "{{ ds_nodash }}{{ execution_date.strftime('%H%M%S') }}"  # Format: YYYYMMDDHHMMSS
    }),
    response_check=lambda response: response.status_code == 200,
    log_response=True,
    dag=dag,
)


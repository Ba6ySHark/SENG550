from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'incremental_orders_processing',
    default_args=default_args,
    description='Process incremental order data every 4 seconds',
    schedule_interval=timedelta(seconds=4),
    catchup=False,
    max_active_runs=1,
)

process_orders = BashOperator(
    task_id='process_incremental_orders',
    bash_command='cd /workspace && docker compose exec -T spark python3 /workspace/processing/incremental/process_orders.py || docker exec spark python3 /workspace/processing/incremental/process_orders.py || exit 1',
    dag=dag,
)

process_orders


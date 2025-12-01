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
    'prediction_cache_generation',
    default_args=default_args,
    description='Generate and cache all predictions in Redis every 20 seconds',
    schedule_interval=timedelta(seconds=20),
    catchup=False,
    max_active_runs=1,
)

generate_predictions = BashOperator(
    task_id='generate_and_cache_predictions',
    bash_command='cd /workspace && docker compose exec -T spark python3 /workspace/processing/ml/generate_predictions.py || docker exec spark python3 /workspace/processing/ml/generate_predictions.py || exit 1',
    dag=dag,
)

generate_predictions


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
    'ml_model_training',
    default_args=default_args,
    description='Train ML model on processed order data every 20 seconds',
    schedule_interval=timedelta(seconds=20),
    catchup=False,
    max_active_runs=1,
)

train_model = BashOperator(
    task_id='train_ml_model',
    bash_command='cd /workspace && docker compose exec -T spark python3 /workspace/processing/ml/train.py || docker exec spark python3 /workspace/processing/ml/train.py || exit 1',
    dag=dag,
)

train_model


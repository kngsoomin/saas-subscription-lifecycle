from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    "ci_test",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
):
    EmptyOperator(task_id="test")
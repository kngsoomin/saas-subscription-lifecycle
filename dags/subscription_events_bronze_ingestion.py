import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.mock_event_generator import generate_mock_events
from src.bronze_writer import write_bronze_events

logger = logging.getLogger(__name__)


def generate_and_write_bronze() -> None:
    events = generate_mock_events()
    output_path = write_bronze_events(events=events)

    logger.info("Generated %s events", len(events))
    logger.info("Bronze file written to %s", output_path)

    if events:
        logger.info("Sample event: %s", events[0])

with DAG(
    dag_id="subscription_events_bronze_ingestion",
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    catchup=False,
    tags=["saas-lifecycle-airflow", "mock-events", "bronze"],
) as dag:
    generate_and_write = PythonOperator(
        task_id="generate_and_write_bronze",
        python_callable=generate_and_write_bronze,
    )
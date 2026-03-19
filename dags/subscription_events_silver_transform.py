from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.silver.transform import (
    read_bronze_incremental,
    load_history,
    update_history,
    build_current
)
from src.silver.watermark import load_watermark, save_watermark


logger = logging.getLogger(__name__)

PIPELINE_NAME = "subscription_events_silver_transform"


def transform_bronze_to_silver() -> None:
    # load watermark
    last_watermark = load_watermark(pipeline_name=PIPELINE_NAME)
    logger.info(f"Loaded watermark: {last_watermark}")

    # read bronze incremental
    new_events = read_bronze_incremental(last_watermark=last_watermark)
    logger.info(f"Read {len(new_events)} bronze events")

    if not new_events:
        logger.info(f"No new events found. Skipping silver update.")
        return

    # update affected history partitions
    updated_history_df = update_history(new_events=new_events)
    logger.info(f"Built affected history rows: {len(updated_history_df)} rows")

    # load full history for current snapshot
    full_history_df = load_history()
    current_df = build_current(full_history_df=full_history_df)
    logger.info(f"Built current snapshot with {len(current_df)} rows")

    # save watermark
    max_ingested_at = max(event["ingested_at"] for event in new_events)
    save_watermark(
        pipeline_name=PIPELINE_NAME,
        last_processed_ingested_at=max_ingested_at,
    )
    logger.info(f"Saved watermark: {max_ingested_at}")


with DAG(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2026,3,1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["saas-lifecycle-airflow", "transform", "silver"],
) as dag:
    transform_bronze_to_silver_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
    )

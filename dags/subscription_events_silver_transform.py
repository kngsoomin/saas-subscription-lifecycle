from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.common.constants import SILVER_DAG_ID, GOLD_KPI_DAILY_DAG_ID
from src.silver.transform import (
    read_bronze_incremental,
    load_history,
    update_history,
    build_current
)
from src.silver.watermark import load_watermark, save_watermark


logger = logging.getLogger(__name__)


def transform_bronze_to_silver() -> None:
    # load watermark
    last_watermark = load_watermark(pipeline_name=SILVER_DAG_ID)
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
        pipeline_name=SILVER_DAG_ID,
        last_processed_ingested_at=max_ingested_at,
    )
    logger.info(f"Saved watermark: {max_ingested_at}")


with DAG(
    dag_id=SILVER_DAG_ID,
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

    trigger_gold_kpi_daily_task = TriggerDagRunOperator(
        task_id="trigger_gold_kpi_daily",
        trigger_dag_id=GOLD_KPI_DAILY_DAG_ID
    )

    transform_bronze_to_silver_task >> trigger_gold_kpi_daily_task
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.gold.kpi_daily import (
    load_gold_inputs,
    get_affected_start_date,
    build_kpi_daily_df,
    write_kpi_daily_partitions,
    validate_latest_kpi_with_current,
    update_gold_watermark,
)
from src.silver.watermark import load_watermark


logger = logging.getLogger(__name__)

PIPELINE_NAME = "subscription_events_gold_kpi_daily"


def update_gold_kpi_daily() -> None:
    # load last watermark
    last_watermark = load_watermark(pipeline_name=PIPELINE_NAME)
    logger.info("Loaded gold watermark: %s", last_watermark)

    # load original kpi df and newly updated df
    history_df, incremental_df = load_gold_inputs(last_watermark=last_watermark)

    if history_df.empty:
        logger.info("No history data found. Skipping gold update.")
        return
    if incremental_df.empty:
        logger.info("No incremental history rows found. Skipping gold update.")
        return

    # to select partitions to update
    start_date = get_affected_start_date(incremental_df=incremental_df)
    logger.info("Affected start date: %s", start_date)

    # new kpi daily df
    kpi_df = build_kpi_daily_df(history_df=history_df, start_date=start_date)

    if kpi_df.empty:
        logger.info("No KPI rows generated. Skipping write.")
        return

    # write partitions (by event_time:dt)
    write_kpi_daily_partitions(kpi_df=kpi_df)
    logger.info("Wrote %s gold KPI rows", len(kpi_df))

    # cross-check current snapshot and kpi table
    validation_result = validate_latest_kpi_with_current(kpi_df=kpi_df)
    logger.info("Gold validation result: %s", validation_result)

    if not validation_result["is_valid"]:
        raise ValueError(f"Gold KPI daily validation failed: {validation_result}")

    # update watermark
    update_gold_watermark(
        incremental_df=incremental_df,
        pipeline_name=PIPELINE_NAME,
    )
    logger.info("Updated gold watermark.")


with DAG(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    catchup=False,
    tags=["saas-lifecycle-airflow", "gold", "kpi"],
    max_active_runs=1,
) as dag:
    update_gold_kpi_daily_task = PythonOperator(
        task_id="update_gold_kpi_daily",
        python_callable=update_gold_kpi_daily,
    )
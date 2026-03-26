from __future__ import annotations

import pandas as pd
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.gold.kpi_daily import (
    load_gold_inputs,
    load_current_snapshot,
    get_affected_start_date,
    build_kpi_daily_df,
    write_kpi_daily_partitions,
    update_gold_watermark,
)
from src.gold.validation import validate_gold_kpi_daily
from src.silver.watermark import load_watermark
from src.common.storage_factory import get_storage
from src.common.constants import GOLD_KPI_DAILY_DAG_ID


logger = logging.getLogger(__name__)


def update_gold_kpi_daily() -> None:
    storage = get_storage()

    # load last watermark
    last_watermark = load_watermark(pipeline_name=GOLD_KPI_DAILY_DAG_ID, storage=storage)
    logger.info("Loaded gold watermark: %s", last_watermark)

    # load original kpi df and newly updated df
    history_df, incremental_df = load_gold_inputs(last_watermark=last_watermark, storage=storage)

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

    affected_partitions = sorted(
        pd.to_datetime(kpi_df["date"])
        .dt.strftime("%Y-%m-%d")
        .unique()
        .tolist()
    )

    # write partitions (by event_time:dt)
    write_kpi_daily_partitions(kpi_df=kpi_df, storage=storage)
    logger.info("Wrote %s gold KPI rows", len(kpi_df))

    # validate
    current_df = load_current_snapshot(storage=storage)
    gold_validation = validate_gold_kpi_daily(
        kpi_df=kpi_df,
        current_df=current_df,
        affected_partitions=affected_partitions,
    )

    logger.info(
        "gold_kpi_validation_summary=%s",
        gold_validation.log_summary(),
    )

    if not gold_validation.passed:
        logger.error(
            "gold_kpi_validation_failed_checks=%s",
            gold_validation.failed_check_details(),
        )
        gold_validation.raise_if_failed()

    # update watermark
    update_gold_watermark(
        incremental_df=incremental_df,
        pipeline_name=GOLD_KPI_DAILY_DAG_ID,
        storage=storage,
    )
    logger.info("Updated gold watermark.")


with DAG(
    dag_id=GOLD_KPI_DAILY_DAG_ID,
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
import pandas as pd
import logging

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
from src.common.constants import TEST_ROOT, DEFAULT_GOLD_KPI_DAILY_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "test_gold_pipeline"


def print_gold_kpi_daily():
    storage = get_storage()

    all_paths = storage.list_paths(DEFAULT_GOLD_KPI_DAILY_DIR, "dt=*")

    dfs = []
    for dt_path in all_paths:
        file_path = storage.join(dt_path, "part-000.parquet")

        if storage.exists(file_path):
            df = storage.read_parquet(file_path)
            dfs.append(df)

    print("\n=== FULL KPI DAILY ===")
    if dfs:
        full_df = pd.concat(dfs, ignore_index=True).sort_values("date")
        print(full_df)
    else:
        print("No gold data found.")
    print("=== DONE ===")


def main():
    print("=== GOLD PIPELINE TEST START ===")
    storage = get_storage()

    # 1. load watermark
    last_watermark = load_watermark(
        pipeline_name=PIPELINE_NAME,
        storage=storage
    )
    print(f"Loaded watermark: {last_watermark}")

    # 2. load history + incremental slice
    history_df, incremental_df = load_gold_inputs(
        last_watermark=last_watermark,
        storage=storage
    )
    print(f"Loaded full history rows: {len(history_df)}")
    print(f"Loaded incremental rows: {len(incremental_df)}")

    if history_df.empty:
        print("No history data. Exit.")
        print_gold_kpi_daily()
        return

    if incremental_df.empty:
        print("No incremental rows. Exit.")
        print_gold_kpi_daily()
        return

    # 3. find affected start date
    start_date = get_affected_start_date(incremental_df=incremental_df)
    print(f"Affected start date: {start_date}")

    # 4. build gold kpi df
    kpi_df = build_kpi_daily_df(
        history_df=history_df,
        start_date=start_date,
    )
    print(f"Built KPI rows: {len(kpi_df)}")

    if kpi_df.empty:
        print("No KPI rows. Exit.")
        return

    affected_partitions = sorted(
        pd.to_datetime(kpi_df["date"])
        .dt.strftime("%Y-%m-%d")
        .unique()
        .tolist()
    )
    print(f"Affected partitions: {affected_partitions}")

    # 5. write partitions
    write_kpi_daily_partitions(kpi_df=kpi_df, storage=storage)
    print("Wrote gold kpi_daily partitions")

    # 6. load current snapshot for validation
    current_df = load_current_snapshot(storage=storage)
    print(f"Loaded current snapshot rows: {len(current_df)}")

    # 7. validate
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

    # 8. update watermark
    update_gold_watermark(
        incremental_df=incremental_df,
        pipeline_name=PIPELINE_NAME,
        storage=storage
    )
    print("Updated watermark")

    # sample output
    print("\n=== SAMPLE KPI DAILY ===")
    print(kpi_df.head())

    print("=== DONE ===")

    # 9. load full gold table
    print_gold_kpi_daily()

if __name__ == "__main__":
    main()
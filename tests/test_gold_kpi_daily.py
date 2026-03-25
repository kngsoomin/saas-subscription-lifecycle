from src.gold.kpi_daily import (
    load_gold_inputs,
    get_affected_start_date,
    build_kpi_daily_df,
    write_kpi_daily_partitions,
    validate_latest_kpi_with_current,
    update_gold_watermark,
)
from src.silver.watermark import load_watermark
from src.common.storage import LocalStorage
from src.common.constants import TEST_ROOT, DEFAULT_GOLD_KPI_DAILY_DIR
import pandas as pd



PIPELINE_NAME = "test_gold_pipeline"


def print_gold_kpi_daily():
    storage = LocalStorage(base_dir=TEST_ROOT)

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
    storage = LocalStorage(base_dir=TEST_ROOT)

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

    # 5. write partitions
    write_kpi_daily_partitions(kpi_df=kpi_df, storage=storage)
    print("Wrote gold kpi_daily partitions")

    # 6. validate with silver current snapshot
    validation_result = validate_latest_kpi_with_current(kpi_df=kpi_df, storage=storage)
    print(f"Validation result: {validation_result}")

    # 7. update watermark
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

    # 8. load full gold table
    print_gold_kpi_daily()

if __name__ == "__main__":
    main()
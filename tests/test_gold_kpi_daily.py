from src.gold.kpi_daily import (
    load_gold_inputs,
    get_affected_start_date,
    build_kpi_daily_df,
    write_kpi_daily_partitions,
    validate_latest_kpi_with_current,
    update_gold_watermark,
)
from src.silver.watermark import load_watermark


PIPELINE_NAME = "test_gold_pipeline"


def main():
    print("=== GOLD PIPELINE TEST START ===")

    # 1. load watermark
    last_watermark = load_watermark(
        pipeline_name=PIPELINE_NAME,
        base_dir="data/state/pipeline",
    )
    print(f"Loaded watermark: {last_watermark}")

    # 2. load history + incremental slice
    history_df, incremental_df = load_gold_inputs(
        silver_history_path="data/silver/subscription_state_history",
        last_watermark=last_watermark,
    )
    print(f"Loaded full history rows: {len(history_df)}")
    print(f"Loaded incremental rows: {len(incremental_df)}")

    if history_df.empty:
        print("No history data. Exit.")
        return

    if incremental_df.empty:
        print("No incremental rows. Exit.")
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
    write_kpi_daily_partitions(
        kpi_df=kpi_df,
        base_dir="data/gold/kpi_daily",
    )
    print("Wrote gold kpi_daily partitions")

    # 6. validate with silver current snapshot
    validation_result = validate_latest_kpi_with_current(
        kpi_df=kpi_df,
        current_snapshot_path="data/silver/subscription_state_current/current.parquet",
    )
    print(f"Validation result: {validation_result}")

    # 7. update watermark
    update_gold_watermark(
        incremental_df=incremental_df,
        pipeline_name=PIPELINE_NAME,
        state_base_dir="data/state/pipeline",
    )
    print("Updated watermark")

    # sample output
    print("\n=== SAMPLE KPI DAILY ===")
    print(kpi_df.head())

    print("=== DONE ===")


if __name__ == "__main__":
    main()
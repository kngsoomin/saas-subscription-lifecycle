from src.silver.transform import (
    read_bronze_incremental,
    update_history,
    build_current,
    load_history
)
from src.silver.watermark import load_watermark, save_watermark
from src.common.constants import TEST_ROOT
from src.common.storage_factory import get_storage

PIPELINE_NAME = "test_silver_pipeline"


def main():
    print("=== SILVER PIPELINE TEST START ===")

    storage = get_storage()

    # 1. load watermark
    last_watermark = load_watermark(
        pipeline_name=PIPELINE_NAME,
        storage=storage
    )
    print(f"Loaded watermark: {last_watermark}")

    # 2. read bronze incremental
    events = read_bronze_incremental(
        last_watermark=last_watermark,
        storage=storage
    )
    print(f"Read {len(events)} events")

    if not events:
        print("No new events. Exit.")
        return

    # 3. build history
    updated_history_df = update_history(
        new_events=events,
        storage=storage
    )
    print(f"Updated: {len(updated_history_df)} rows")

    # 4. build current
    full_history_df = load_history(storage=storage)
    current_df = build_current(
        full_history_df=full_history_df,
        storage=storage
    )
    print(f"Current snapshot: {len(current_df)} rows")

    # 5. update watermark
    max_ingested_at = max(e["ingested_at"] for e in events)
    save_watermark(
        pipeline_name=PIPELINE_NAME,
        last_processed_ingested_at=max_ingested_at,
        storage=storage
    )

    print(f"Updated watermark: {max_ingested_at}")

    # sample 출력
    print("\n=== SAMPLE CURRENT ===")
    print(current_df.head())

    print("=== DONE ===")


if __name__ == "__main__":
    main()
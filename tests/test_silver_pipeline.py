from src.silver.transform import (
    read_bronze_incremental,
    update_history,
    build_current,
    load_history
)
from src.silver.watermark import load_watermark, save_watermark


PIPELINE_NAME = "test_silver_pipeline"


def main():
    print("=== SILVER PIPELINE TEST START ===")

    # 1. load watermark
    last_watermark = load_watermark(
        pipeline_name=PIPELINE_NAME,
        base_dir="data/state/pipeline"
    )
    print(f"Loaded watermark: {last_watermark}")

    # 2. read bronze incremental
    events = read_bronze_incremental(
        last_watermark=last_watermark,
        base_dir="data/bronze/subscription_events"
    )
    print(f"Read {len(events)} events")

    if not events:
        print("No new events. Exit.")
        return

    # 3. build history
    updated_history_df = update_history(
        new_events=events,
        base_dir="data/silver/subscription_state_history"
    )
    print(f"Updated: {len(updated_history_df)} rows")

    # 4. build current
    full_history_df = load_history(
        base_dir="data/silver/subscription_state_history"
    )
    current_df = build_current(
        full_history_df=full_history_df,
        base_dir="data/silver/subscription_state_current"
    )
    print(f"Current snapshot: {len(current_df)} rows")

    # 5. update watermark
    max_ingested_at = max(e["ingested_at"] for e in events)
    save_watermark(
        pipeline_name=PIPELINE_NAME,
        last_processed_ingested_at=max_ingested_at,
        base_dir="data/state/pipeline"
    )

    print(f"Updated watermark: {max_ingested_at}")

    # sample 출력
    print("\n=== SAMPLE CURRENT ===")
    print(current_df.head())

    print("=== DONE ===")


if __name__ == "__main__":
    main()
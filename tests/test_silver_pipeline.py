import pandas as pd
import logging

from src.silver.transform import (
    read_bronze_incremental,
    update_history,
    build_current,
    load_history
)
from src.silver.validation import validate_silver_history, validate_silver_current
from src.silver.watermark import load_watermark, save_watermark
from src.common.constants import TEST_ROOT
from src.common.storage_factory import get_storage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

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

    new_events_df = pd.DataFrame(events)
    affected_partitions = sorted(
        pd.to_datetime(new_events_df["event_time"], utc=True)
        .dt.strftime("%Y-%m-%d")
        .unique()
        .tolist()
    )
    history_validation = validate_silver_history(
        df=updated_history_df,
        affected_partitions=affected_partitions
    )
    logger.info(
        "silver_history_validation_summary=%s",
        history_validation.log_summary()
    )
    if not history_validation.passed:
        logger.error(
            "silver_history_validation_failed_checks=%s",
            history_validation.failed_check_details(),
        )
        history_validation.raise_if_failed()

    # 4. build current
    full_history_df = load_history(storage=storage)
    current_df = build_current(
        full_history_df=full_history_df,
        storage=storage
    )
    print(f"Current snapshot: {len(current_df)} rows")

    current_validation = validate_silver_current(
        current_df=current_df,
        history_df=full_history_df,
    )
    logger.info(
        "silver_current_validation_summary=%s",
        current_validation.log_summary()
    )
    if not current_validation.passed:
        logger.error(
            "silver_current_validation_failed_checks=%s",
            current_validation.failed_check_details(),
        )
        current_validation.raise_if_failed()

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
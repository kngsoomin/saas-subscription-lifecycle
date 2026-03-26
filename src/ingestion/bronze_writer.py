from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
import logging

from src.common.constants import DEFAULT_BRONZE_BASE_DIR
from src.common.storage import LocalStorage, Storage
from src.ingestion.validation import validate_bronze_events

logger = logging.getLogger(__name__)


def build_bronze_output_path(
    *,
    base_dir: str = DEFAULT_BRONZE_BASE_DIR,
    runtime: datetime | None = None,
    storage: Storage | None = None,
) -> str:
    storage = storage or LocalStorage()
    runtime = runtime or datetime.now(timezone.utc)
    dt = runtime.strftime("%Y-%m-%d")
    ts = runtime.strftime("%Y%m%dT%H%M%SZ")

    return storage.join(
        base_dir,
        f"dt={dt}",
        f"subscription_events_{ts}.jsonl"
    )


def write_events_to_jsonl(
    *,
    events: List[Dict],
    output_path: str,
    storage: Storage | None = None,
) -> str | None:
    if not events:
        return None
    storage = storage or LocalStorage()
    storage.write_jsonl(path=output_path, records=events)
    return output_path


def write_bronze_events(
    *,
    events: List[Dict],
    base_dir: str = DEFAULT_BRONZE_BASE_DIR,
    runtime: datetime | None = None,
    storage: Storage | None = None,
) -> str | None:
    if not events:
        return None

    storage = storage or LocalStorage()
    runtime = runtime or datetime.now(timezone.utc)

    df = pd.DataFrame(events)
    affected_partitions = [runtime.strftime("%Y-%m-%d")] # for now (tbu for kafka when generator logic is separated from ingestion)

    validation_result = validate_bronze_events(
        df=df,
        affected_partitions=affected_partitions,
    )
    logger.info("bronze_validation_summary=%s", validation_result.log_summary())
    if not validation_result.passed:
        logger.error(
            "bronze_validation_failed_checks=%s",
            validation_result.failed_check_details(),
        )
        validation_result.raise_if_failed()

    output_path = build_bronze_output_path(
        base_dir=base_dir,
        runtime=runtime,
        storage=storage
    )
    return write_events_to_jsonl(
        events=events,
        output_path=output_path,
        storage=storage
    )

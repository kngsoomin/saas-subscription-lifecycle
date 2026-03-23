from datetime import datetime, timezone

from typing import List, Dict

from src.common.constants import DEFAULT_BRONZE_BASE_DIR
from src.common.storage import LocalStorage


def build_bronze_output_path(
    *,
    base_dir: str = DEFAULT_BRONZE_BASE_DIR,
    runtime: datetime | None = None,
    storage: LocalStorage | None = None,
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
    storage: LocalStorage | None = None,
) -> str:
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
    storage: LocalStorage | None = None,
) -> str | None:
    if not events:
        return None

    storage = storage or LocalStorage()
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

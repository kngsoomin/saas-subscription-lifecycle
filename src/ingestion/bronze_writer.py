import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

from src.common.constants import DEFAULT_BRONZE_BASE_DIR


def build_bronze_output_path(
    *,
    base_dir: str = DEFAULT_BRONZE_BASE_DIR,
    runtime: datetime | None = None,
):
    runtime = runtime or datetime.now(timezone.utc)
    dt = runtime.strftime("%Y-%m-%d")
    ts = runtime.strftime("%Y%m%dT%H%M%SZ")

    return Path(base_dir) / f"dt={dt}" / f"subscription_events_{ts}.jsonl"

def write_events_to_jsonl(
    *,
    events: List[Dict],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    return output_path

def write_bronze_events(
    *,
    events: List[Dict],
    base_dir: str = DEFAULT_BRONZE_BASE_DIR,
    runtime: datetime | None = None,
) -> Path:
    output_path = build_bronze_output_path(
        base_dir=base_dir,
        runtime=runtime,
    )
    return write_events_to_jsonl(
        events=events,
        output_path=output_path
    )

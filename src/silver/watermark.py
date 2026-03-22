import json

from src.common.constants import DEFAULT_PIPELINE_STATE_DIR
from src.common.storage import LocalStorage


def load_watermark(
    pipeline_name: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
    storage: LocalStorage | None = None,
) -> str | None:
    storage = storage or LocalStorage()
    path = storage.join(base_dir, "watermark.json")
    if not storage.exists(path):
        return None

    with storage.open_text_read(path) as f:
        payload = json.load(f)

    return payload.get(pipeline_name, {}).get("last_processed_ingested_at")

def save_watermark(
    pipeline_name: str,
    last_processed_ingested_at: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
    storage: LocalStorage | None = None,
) -> None:
    storage = storage or LocalStorage()
    path = storage.join(base_dir, "watermark.json")

    if storage.exists(path):
        with storage.open_text_read(path) as f:
            payload = json.load(f)
    else:
        payload = {}

    payload[pipeline_name] = {
        "last_processed_ingested_at": last_processed_ingested_at,
    }

    with storage.open_text_write(path) as f:
        json.dump(payload, f, indent=2)
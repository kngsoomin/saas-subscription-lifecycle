import json

from src.common.constants import DEFAULT_PIPELINE_STATE_DIR
from src.common.storage import LocalStorage, Storage


def load_watermark(
    pipeline_name: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
    storage: Storage | None = None,
) -> str | None:
    storage = storage or LocalStorage()
    path = storage.join(base_dir, "watermark.json")
    if not storage.exists(path):
        return None

    content = storage.read_text(path)
    payload = json.loads(content)

    return payload.get(pipeline_name, {}).get("last_processed_ingested_at")

def save_watermark(
    pipeline_name: str,
    last_processed_ingested_at: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
    storage: Storage | None = None,
) -> None:
    storage = storage or LocalStorage()
    path = storage.join(base_dir, "watermark.json")

    if storage.exists(path):
        content = storage.read_text(path)
        payload = json.loads(content)
    else:
        payload = {}

    payload[pipeline_name] = {
        "last_processed_ingested_at": last_processed_ingested_at,
    }
    storage.write_text(path, json.dumps(payload, indent=2))
import json
from pathlib import Path

from src.common.constants import DEFAULT_PIPELINE_STATE_DIR


def load_watermark(
    pipeline_name: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
) -> str | None:
    path = Path(base_dir) / "watermark.json"
    if not path.exists():
        return None

    payload = json.loads(path.read_text())
    return payload.get(pipeline_name, {}).get("last_processed_ingested_at")

def save_watermark(
    pipeline_name: str,
    last_processed_ingested_at: str,
    base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
) -> None:
    path = Path(base_dir) / "watermark.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        payload = json.loads(path.read_text())
    else:
        payload = {}

    payload[pipeline_name] = {
        "last_processed_ingested_at": last_processed_ingested_at,
    }

    path.write_text(json.dumps(payload, indent=2))
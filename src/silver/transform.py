import json
import pandas as pd
from pathlib import Path
from typing import Dict, List
from datetime import datetime, timezone


def read_bronze_incremental(
    last_watermark: str | None,
    base_dir: str="/opt/project/data/bronze/subscription_events",
) -> List[Dict]:
    base_path = Path(base_dir)

    events: List[Dict] = []

    watermark_dt = (
        datetime.fromisoformat(last_watermark.replace("Z", "+00:00"))
        if last_watermark else None
    )

    # for faster filtering
    watermark_date = watermark_dt.date() if watermark_dt else None

    for dt_path in base_path.glob("dt=*"):
        dt_str = dt_path.name.split("=")[1]
        dt_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        if watermark_date and dt_date < watermark_date:
            continue

        for file_path in dt_path.glob("*.jsonl"):
            with file_path.open() as f:
                for line in f:
                    event = json.loads(line)
                    ingested_at = datetime.fromisoformat(
                        event["ingested_at"].replace("Z", "+00:00")
                    )

                    if watermark_dt is None or ingested_at > watermark_dt:
                        events.append(event)

    return events


def build_history(
    new_events: List[Dict],
    base_dir: str="/opt/project/data/silver/subscription_state_history",
) -> pd.DataFrame:

    base_path = Path(base_dir)
    history_path = base_path / "history.parquet"

    base_path.mkdir(parents=True, exist_ok=True)

    # for new events
    new_df = pd.DataFrame(new_events)

    # for existing events: load history.parquet
    if history_path.exists():
        existing_df = pd.read_parquet(history_path)
    else:
        existing_df = pd.DataFrame()

    if new_df.empty and existing_df.empty:
        return pd.DataFrame()

    if existing_df.empty:
        main_df = new_df.copy()
    elif new_df.empty:
        main_df = existing_df.copy()
    else:
        main_df = pd.concat([existing_df, new_df], ignore_index=True)

    for col in ["event_time", "ingested_at"]:
        main_df[col] = pd.to_datetime(main_df[col], utc=True)

    # deduplication
    sort_cols = ["event_time", "ingested_at", "event_id"]
    missing_cols = [col for col in sort_cols if col not in main_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns found while building history dataframe: {missing_cols}")

    main_df = main_df.drop_duplicates(subset=["event_id"], keep="last")
    main_df = main_df.sort_values(by=sort_cols).reset_index(drop=True)

    # parquet overwrite
    main_df.to_parquet(history_path, index=False)

    return main_df


def build_current(
    history_df: pd.DataFrame,
    base_dir: str="/opt/project/data/silver/subscription_state_current",
    runtime: datetime | None = None,
) -> pd.DataFrame:

    required_cols = [
        "subscription_id",
        "user_id",
        "plan_id",
        "billing_cycle",
        "price",
        "currency",
        "status",
        "event_type",
        "event_time",
        "ingested_at",
        "event_id",
    ]

    missing_cols = [col for col in required_cols if col not in history_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in history dataframe: {missing_cols}")

    runtime = runtime or datetime.now(timezone.utc)

    latest_df = (
        history_df
        .sort_values(by=["event_time", "ingested_at", "event_id"])
        .groupby("subscription_id", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    current_df = latest_df[
        [
            "subscription_id",
            "user_id",
            "plan_id",
            "billing_cycle",
            "price",
            "currency",
            "status",
            "event_type",
            "event_time",
        ]
    ].rename(
        columns={
            "plan_id": "current_plan_id",
            "billing_cycle": "current_billing_cycle",
            "price": "current_price",
            "status": "current_status",
            "event_type": "last_event_type",
            "event_time": "last_event_time",
        }
    )

    current_df["snapshot_time"] = pd.Timestamp(runtime)
    output_path = Path(base_dir) / "current.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    current_df.to_parquet(output_path, index=False)

    return current_df


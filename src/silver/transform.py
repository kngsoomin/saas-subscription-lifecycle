import pandas as pd
from typing import Dict, List
from datetime import datetime, timezone

from src.common.constants import (
    DEFAULT_BRONZE_BASE_DIR,
    DEFAULT_SILVER_HISTORY_DIR,
    DEFAULT_SILVER_CURRENT_DIR
)
from src.common.storage import LocalStorage


def read_bronze_incremental(
    last_watermark: str | None,
    base_dir: str=DEFAULT_BRONZE_BASE_DIR,
    storage: LocalStorage | None=None,
) -> List[Dict]:

    storage = storage or LocalStorage()
    events: List[Dict] = []

    watermark_dt = (
        datetime.fromisoformat(last_watermark.replace("Z", "+00:00"))
        if last_watermark else None
    )

    # faster filtering
    watermark_date = watermark_dt.date() if watermark_dt else None

    for dt_path in storage.list_paths(base_dir, "dt=*"):
        dt_str = storage.basename(dt_path).split("=")[1]
        dt_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        if watermark_date and dt_date < watermark_date:
            continue

        for file_path in storage.list_paths(dt_path, "*.jsonl"):
            events_in_file = storage.read_jsonl(file_path)

            for event in events_in_file:
                ingested_at = datetime.fromisoformat(
                    event["ingested_at"].replace("Z", "+00:00")
                )
                if watermark_dt is None or ingested_at > watermark_dt:
                    events.append(event)

    return events


def load_history_partitions(
    dates: list[str],
    base_dir: str = DEFAULT_SILVER_HISTORY_DIR,
    storage: LocalStorage | None=None,
) -> pd.DataFrame:

    storage = storage or LocalStorage()
    dfs: List[pd.DataFrame] = []

    for dt_str in dates:
        file_path = storage.join(base_dir, f"dt={dt_str}", "part-000.parquet")
        if storage.exists(file_path):
            dfs.append(pd.read_parquet(file_path))

    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        return df

    for col in ["event_time", "ingested_at"]:
        df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True)

    return df


def load_history(
    base_dir: str = DEFAULT_SILVER_HISTORY_DIR,
    storage: LocalStorage | None=None,
) -> pd.DataFrame:
    storage = storage or LocalStorage()
    files = sorted(storage.list_paths(base_dir, "dt=*/part-*.parquet"))

    if not files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if df.empty:
        return df

    for col in ["event_time", "ingested_at"]:
        df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True)

    sort_cols = ["event_time", "ingested_at", "event_id"]
    missing_cols = [col for col in sort_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns found while loading history dataframe: {missing_cols}")

    df = (
        df.drop_duplicates(subset=["event_id"], keep="last")
        .sort_values(by=sort_cols)
        .reset_index(drop=True)
    )

    return df


def update_history(
    new_events: List[Dict],
    base_dir: str = DEFAULT_SILVER_HISTORY_DIR,
    storage: LocalStorage | None=None,
) -> pd.DataFrame:
    storage = storage or LocalStorage()
    storage.mkdir(base_dir)

    new_df = pd.DataFrame(new_events)
    if new_df.empty:
        return pd.DataFrame()

    for col in ["event_time", "ingested_at"]:
        new_df[col] = pd.to_datetime(new_df[col], format="ISO8601", utc=True)

    required_cols = ["event_time", "ingested_at", "event_id"]
    missing_cols = [col for col in required_cols if col not in new_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in new events dataframe: {missing_cols}")

    # partitioned by event_time
    new_df["dt"] = new_df["event_time"].dt.strftime("%Y-%m-%d")
    affected_dates = sorted(new_df["dt"].unique().tolist())

    # read affected partitions
    existing_df = load_history_partitions(
        dates=affected_dates,
        base_dir=base_dir,
        storage=storage,
    )

    if not existing_df.empty:
        existing_df["dt"] = existing_df["event_time"].dt.strftime("%Y-%m-%d")

    if existing_df.empty:
        merged_df = new_df.copy()
    else:
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)

    merged_df = (
        merged_df.drop_duplicates(subset=["event_id"], keep="last")
        .sort_values(by=["event_time", "ingested_at", "event_id"])
        .reset_index(drop=True)
    )

    for dt_value, partition_df in merged_df.groupby("dt"):
        out_dir = storage.join(base_dir, f"dt={dt_value}")
        storage.mkdir(out_dir)

        out_path = storage.join(out_dir, "part-000.parquet")
        partition_df.drop(columns=["dt"]).to_parquet(out_path, index=False)

    return merged_df.drop(columns=["dt"])


def build_current(
    full_history_df: pd.DataFrame,
    base_dir: str = DEFAULT_SILVER_CURRENT_DIR,
    runtime: datetime | None = None,
    storage: LocalStorage | None = None,
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

    missing_cols = [col for col in required_cols if col not in full_history_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in history dataframe: {missing_cols}")

    runtime = runtime or datetime.now(timezone.utc)

    latest_df = (
        full_history_df
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

    output_path = storage.join(base_dir, "current.parquet")
    storage.mkdir(storage.parent(output_path))
    current_df.to_parquet(output_path, index=False)

    return current_df


from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone

from src.silver.transform import load_history
from src.silver.watermark import save_watermark
from src.common.constants import (
    DEFAULT_SILVER_HISTORY_DIR,
    DEFAULT_SILVER_CURRENT_PATH,
    DEFAULT_GOLD_KPI_DAILY_DIR,
    DEFAULT_PIPELINE_STATE_DIR,
)
from src.common.storage import LocalStorage, Storage


KPI_DAILY_COLUMNS = [
    "date",
    "new_subscriptions",
    "new_cancellations",
    "active_subscriptions",
    "mrr",
    "currency",
    "snapshot_time",
]


def load_gold_inputs(
    last_watermark: str | None,
    silver_history_path: str = DEFAULT_SILVER_HISTORY_DIR,
    storage: Storage | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    storage = storage or LocalStorage()

    history_df = load_history(
        base_dir=silver_history_path,
        storage=storage
    )
    if history_df.empty:
        return history_df, pd.DataFrame()

    history_df = history_df.copy()
    history_df["date"] = history_df["event_time"].dt.date

    incremental_df = get_gold_incremental(
        history_df=history_df,
        last_watermark=last_watermark,
    )

    return history_df, incremental_df


def get_gold_incremental(
    history_df: pd.DataFrame,
    last_watermark: str | None,
) -> pd.DataFrame:
    if history_df.empty:
        return history_df

    if not last_watermark:
        return history_df.copy()

    watermark_dt = pd.to_datetime(last_watermark, format="ISO8601", utc=True)
    return history_df[history_df["ingested_at"] > watermark_dt].copy()


def get_affected_start_date(incremental_df: pd.DataFrame):
    if incremental_df.empty:
        return None
    return incremental_df["event_time"].min().date()


def build_kpi_daily_df(
    history_df: pd.DataFrame,
    start_date,
) -> pd.DataFrame:

    rows: list[dict] = []
    dates_to_update = sorted(
        d for d in history_df["date"].unique().tolist() if d >= start_date
    )

    if not dates_to_update:
        return pd.DataFrame(columns=KPI_DAILY_COLUMNS)

    snapshot_time = datetime.now(timezone.utc)

    for d in dates_to_update:
        as_of_df = history_df[history_df["date"] <= d].copy()
        latest_df = (
            as_of_df
            .sort_values(["event_time", "ingested_at", "event_id"])
            .groupby("subscription_id", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

        active_df = latest_df[latest_df["status"] == "active"]

        new_subscriptions = history_df[
            (history_df["date"] == d)
            & (history_df["event_type"] == "subscription_created")
        ]["subscription_id"].nunique()

        new_cancellations = history_df[
            (history_df["date"] == d)
            & (history_df["event_type"] == "subscription_cancelled")
        ]["subscription_id"].nunique()

        rows.append({
            "date": d,
            "new_subscriptions": int(new_subscriptions),
            "new_cancellations": int(new_cancellations),
            "active_subscriptions": int(active_df["subscription_id"].nunique()),
            "mrr": round(float(active_df["price"].sum()), 2),
            "currency": "USD",
            "snapshot_time": snapshot_time,
        })

    return pd.DataFrame(rows)


def write_kpi_daily_partitions(
    kpi_df: pd.DataFrame,
    base_dir: str = DEFAULT_GOLD_KPI_DAILY_DIR,
    storage: Storage | None = None,
) -> None:
    storage = storage or LocalStorage()

    if kpi_df.empty:
        return

    for _, row in kpi_df.iterrows():
        dt_value = row["date"]
        out_dir = storage.join(base_dir, f"dt={dt_value}")
        out_path = storage.join(out_dir, "part-000.parquet")

        partition_df = pd.DataFrame([row])
        storage.write_parquet(out_path, partition_df)

def validate_latest_kpi_with_current(
    kpi_df: pd.DataFrame,
    current_snapshot_path: str = DEFAULT_SILVER_CURRENT_PATH,
    storage: Storage | None = None,
) -> dict:
    storage = storage or LocalStorage()
    empty_result = {
        "is_valid": True,
        "checked": False,
        "reason": "no_data",
    }

    if kpi_df.empty or (not storage.exists(current_snapshot_path)):
        return empty_result

    current_df = storage.read_parquet(current_snapshot_path)
    if current_df.empty:
        return empty_result

    current_df["current_price"] = pd.to_numeric(
        current_df["current_price"], errors="coerce"
    ).fillna(0.0)

    latest_kpi = kpi_df.sort_values("date").iloc[-1]

    current_active_df = current_df[current_df["current_status"] == "active"]
    current_active_count = int(current_active_df["subscription_id"].nunique())
    current_mrr = round(float(current_active_df["current_price"].sum()), 2)

    actual_active_count = int(latest_kpi["active_subscriptions"])
    actual_mrr = round(float(latest_kpi["mrr"]), 2)

    active_match = actual_active_count == current_active_count
    mrr_match = actual_mrr == current_mrr

    return {
        "is_valid": active_match and mrr_match,
        "checked": True,
        "reason": None,
        "active_subscriptions_match": active_match,
        "mrr_match": mrr_match,
        "expected_active_subscriptions": current_active_count,
        "actual_active_subscriptions": actual_active_count,
        "expected_mrr": current_mrr,
        "actual_mrr": actual_mrr,
    }


def update_gold_watermark(
    incremental_df: pd.DataFrame,
    pipeline_name: str,
    state_base_dir: str = DEFAULT_PIPELINE_STATE_DIR,
    storage: Storage | None = None,
) -> None:
    storage = storage or LocalStorage()
    if incremental_df.empty:
        return

    new_watermark = incremental_df["ingested_at"].max().isoformat()

    save_watermark(
        pipeline_name=pipeline_name,
        last_processed_ingested_at=new_watermark,
        base_dir=state_base_dir,
        storage=storage,
    )


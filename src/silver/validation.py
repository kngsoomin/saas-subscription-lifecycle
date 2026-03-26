from __future__ import annotations

import pandas as pd

from src.common.schema import (
    BRONZE_ALLOWED_VALUES,
    CURRENT_HISTORY_COMPARISON_MAP,
    SILVER_ALLOWED_VALUES,
    SILVER_CURRENT_DATETIME_COLUMNS,
    SILVER_CURRENT_REQUIRED_COLUMNS,
    SILVER_CURRENT_UNIQUE_KEYS,
    SILVER_HISTORY_DATETIME_COLUMNS,
    SILVER_HISTORY_REQUIRED_COLUMNS,
    SILVER_HISTORY_UNIQUE_KEYS,
)
from src.common.validation import (
    ValidationResult,
    check_datetime_parseable,
    check_non_null,
    check_not_empty,
    check_required_columns,
    check_unique, check_allowed_values,
)


def validate_silver_history(
    df: pd.DataFrame,
    affected_partitions: list[str] | None = None,
) -> ValidationResult:
    result = ValidationResult(
        layer="silver",
        dataset="subscription_state_history",
        row_count_in=len(df),
        row_count_out=len(df),
        affected_partitions=affected_partitions or [],
    )

    result.extend(
        [
            check_not_empty(df),
            check_required_columns(df, SILVER_HISTORY_REQUIRED_COLUMNS),
            check_non_null(df, SILVER_HISTORY_REQUIRED_COLUMNS),
            check_datetime_parseable(df, SILVER_HISTORY_DATETIME_COLUMNS),
            check_allowed_values(df, "status", BRONZE_ALLOWED_VALUES["status"]),
            check_unique(df, SILVER_HISTORY_UNIQUE_KEYS, name="event_id_unique"),
        ]
    )

    _add_event_time_le_ingested_at_check(result, df)
    _add_history_sorted_within_subscription_check(result, df)

    return result


def validate_silver_current(
    current_df: pd.DataFrame,
    history_df: pd.DataFrame,
) -> ValidationResult:
    result = ValidationResult(
        layer="silver",
        dataset="subscription_state_current",
        row_count_in=len(current_df),
        row_count_out=len(current_df),
    )

    result.extend(
        [
            check_not_empty(current_df),
            check_required_columns(current_df, SILVER_CURRENT_REQUIRED_COLUMNS),
            check_non_null(current_df, SILVER_CURRENT_REQUIRED_COLUMNS),
            check_datetime_parseable(current_df, SILVER_CURRENT_DATETIME_COLUMNS),
            check_allowed_values(current_df, "current_status", SILVER_ALLOWED_VALUES["current_status"]),
            check_unique(
                current_df,
                SILVER_CURRENT_UNIQUE_KEYS,
                name="subscription_id_unique",
            ),
        ]
    )

    _add_current_matches_latest_history_check(result, current_df, history_df)

    return result


def _add_event_time_le_ingested_at_check(
    result: ValidationResult,
    df: pd.DataFrame,
) -> None:
    required_columns = {"event_time", "ingested_at"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        result.add_check(
            name="event_time_le_ingested_at",
            passed=False,
            reason="missing_columns",
            missing_columns=missing_columns,
        )
        return

    event_time = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
    ingested_at = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)

    comparable_mask = event_time.notna() & ingested_at.notna()
    invalid_mask = comparable_mask & (event_time > ingested_at)

    result.add_check(
        name="event_time_le_ingested_at",
        passed=not invalid_mask.any(),
        invalid_row_count=int(invalid_mask.sum()),
    )


def _add_history_sorted_within_subscription_check(
    result: ValidationResult,
    df: pd.DataFrame,
) -> None:
    required_columns = {"event_time", "ingested_at", "event_id"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        result.add_check(
            name="history_sorted_within_subscription",
            passed=False,
            reason="missing_columns",
            missing_columns=missing_columns,
        )
        return

    ordered_df = df.sort_values(
        by=["event_time", "ingested_at", "event_id"]
    ).reset_index(drop=True)

    actual_df = df.reset_index(drop=True)

    same_order = actual_df[
        ["event_time", "ingested_at", "event_id"]
    ].equals(
        ordered_df[
            ["event_time", "ingested_at", "event_id"]
        ]
    )

    result.add_check(
        name="history_sorted_within_subscription",
        passed=same_order,
    )


def _add_current_matches_latest_history_check(
    result: ValidationResult,
    current_df: pd.DataFrame,
    history_df: pd.DataFrame,
) -> None:
    current_required = {"subscription_id"} | set(CURRENT_HISTORY_COMPARISON_MAP.keys())
    history_required = {"subscription_id", "ingested_at", "event_id"} | set(
        CURRENT_HISTORY_COMPARISON_MAP.values()
    )

    missing_current = sorted(current_required - set(current_df.columns))
    missing_history = sorted(history_required - set(history_df.columns))

    if missing_current or missing_history:
        result.add_check(
            name="current_matches_latest_history",
            passed=False,
            reason="missing_columns",
            missing_current_columns=missing_current,
            missing_history_columns=missing_history,
        )
        return

    latest_history = (
        history_df
        .sort_values(by=["event_time", "ingested_at", "event_id"])
        .groupby("subscription_id", as_index=False)
        .tail(1)
        .copy()
    )

    rename_map = {
        history_col: f"expected_{history_col}"
        for history_col in CURRENT_HISTORY_COMPARISON_MAP.values()
    }
    latest_history = latest_history.rename(columns=rename_map)

    history_columns_for_merge = ["subscription_id"] + list(rename_map.values())

    merged = current_df.merge(
        latest_history[history_columns_for_merge],
        on="subscription_id",
        how="outer",
        indicator=True,
    )

    missing_in_current = int((merged["_merge"] == "right_only").sum())
    missing_in_history = int((merged["_merge"] == "left_only").sum())

    comparable = merged[merged["_merge"] == "both"].copy()

    if comparable.empty:
        mismatch_count = 0
        sample_mismatches: list[dict] = []
    else:
        mismatch_mask = pd.Series(False, index=comparable.index)

        for current_col, history_col in CURRENT_HISTORY_COMPARISON_MAP.items():
            expected_col = f"expected_{history_col}"

            left = comparable[current_col]
            right = comparable[expected_col]

            if current_col in SILVER_CURRENT_DATETIME_COLUMNS:
                left = pd.to_datetime(left, errors="coerce", utc=True)
                right = pd.to_datetime(right, errors="coerce", utc=True)

            mismatch_mask = mismatch_mask | (left != right)

        mismatch_count = int(mismatch_mask.sum())

        sample_columns = ["subscription_id"]
        for current_col, history_col in CURRENT_HISTORY_COMPARISON_MAP.items():
            sample_columns.extend([current_col, f"expected_{history_col}"])

        sample_mismatches = comparable.loc[
            mismatch_mask,
            sample_columns,
        ].head(10).to_dict(orient="records")

    result.add_check(
        name="current_matches_latest_history",
        passed=(missing_in_current == 0 and missing_in_history == 0 and mismatch_count == 0),
        missing_in_current_count=missing_in_current,
        missing_in_history_count=missing_in_history,
        mismatch_count=mismatch_count,
        sample_mismatches=sample_mismatches,
    )
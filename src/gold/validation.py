from __future__ import annotations

import pandas as pd

from src.common.schema import (
    GOLD_KPI_DAILY_ALLOWED_VALUES,
    GOLD_KPI_DAILY_DATETIME_COLUMNS,
    GOLD_KPI_DAILY_REQUIRED_COLUMNS,
    GOLD_KPI_DAILY_UNIQUE_KEYS,
)
from src.common.validation import (
    ValidationResult,
    check_allowed_values,
    check_datetime_parseable,
    check_non_negative,
    check_non_null,
    check_not_empty,
    check_required_columns,
    check_unique,
)


def validate_gold_kpi_daily(
    kpi_df: pd.DataFrame,
    current_df: pd.DataFrame,
    affected_partitions: list[str] | None = None,
) -> ValidationResult:
    result = ValidationResult(
        layer="gold",
        dataset="kpi_daily",
        row_count_in=len(kpi_df),
        row_count_out=len(kpi_df),
        affected_partitions=affected_partitions or [],
    )

    result.extend(
        [
            check_not_empty(kpi_df),
            check_required_columns(kpi_df, GOLD_KPI_DAILY_REQUIRED_COLUMNS),
            check_non_null(kpi_df, GOLD_KPI_DAILY_REQUIRED_COLUMNS),
            check_datetime_parseable(kpi_df, GOLD_KPI_DAILY_DATETIME_COLUMNS),
            check_unique(kpi_df, GOLD_KPI_DAILY_UNIQUE_KEYS, name="date_unique"),
            check_allowed_values(
                kpi_df,
                "currency",
                GOLD_KPI_DAILY_ALLOWED_VALUES["currency"],
            ),
            check_non_negative(kpi_df, "new_subscriptions"),
            check_non_negative(kpi_df, "new_cancellations"),
            check_non_negative(kpi_df, "active_subscriptions"),
            check_non_negative(kpi_df, "mrr"),
        ]
    )

    _add_latest_kpi_matches_current_check(
        result=result,
        kpi_df=kpi_df,
        current_df=current_df,
    )

    return result


def _add_latest_kpi_matches_current_check(
    result: ValidationResult,
    kpi_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> None:
    check = _validate_latest_kpi_with_current(
        kpi_df=kpi_df,
        current_df=current_df,
    )

    result.add_check(
        name="latest_kpi_matches_current_snapshot",
        passed=check["is_valid"],
        checked=check.get("checked"),
        reason=check.get("reason"),
        active_subscriptions_match=check.get("active_subscriptions_match"),
        mrr_match=check.get("mrr_match"),
        expected_active_subscriptions=check.get("expected_active_subscriptions"),
        actual_active_subscriptions=check.get("actual_active_subscriptions"),
        expected_mrr=check.get("expected_mrr"),
        actual_mrr=check.get("actual_mrr"),
    )


def _validate_latest_kpi_with_current(
    kpi_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> dict:
    if kpi_df.empty:
        return {
            "checked": False,
            "is_valid": True,
            "reason": "empty_kpi_df",
        }

    if current_df.empty:
        return {
            "checked": False,
            "is_valid": True,
            "reason": "empty_current_df",
        }

    latest_row = kpi_df.sort_values("date").iloc[-1]

    actual_active_subscriptions = int(latest_row["active_subscriptions"])
    actual_mrr = float(latest_row["mrr"])

    expected_active_subscriptions = int((current_df["current_status"] == "active").sum())
    expected_mrr = float(
        current_df.loc[current_df["current_status"] == "active", "current_price"].sum()
    )

    active_subscriptions_match = actual_active_subscriptions == expected_active_subscriptions
    mrr_match = actual_mrr == expected_mrr

    return {
        "checked": True,
        "is_valid": active_subscriptions_match and mrr_match,
        "active_subscriptions_match": active_subscriptions_match,
        "mrr_match": mrr_match,
        "expected_active_subscriptions": expected_active_subscriptions,
        "actual_active_subscriptions": actual_active_subscriptions,
        "expected_mrr": expected_mrr,
        "actual_mrr": actual_mrr,
    }
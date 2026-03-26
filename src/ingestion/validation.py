from __future__ import annotations

import pandas as pd

from src.common.schema import (
    BRONZE_ALLOWED_VALUES,
    BRONZE_DATETIME_COLUMNS,
    BRONZE_EVENT_SCHEMA_VERSION,
    BRONZE_REQUIRED_COLUMNS,
    EVENT_TYPE_TO_STATUS,
    PLAN_CATALOG,
)
from src.common.validation import (
    ValidationResult,
    check_allowed_values,
    check_column_le,
    check_datetime_parseable,
    check_non_null,
    check_not_empty,
    check_required_columns,
)

# input contract enforcement
def validate_bronze_events(
    df: pd.DataFrame,
    affected_partitions: list[str] | None = None,
) -> ValidationResult:
    result = ValidationResult(
        layer="bronze",
        dataset="subscription_events",
        row_count_in=len(df),
        row_count_out=len(df),
        affected_partitions=affected_partitions or [],
    )

    result.extend(
        [
            check_not_empty(df),
            check_required_columns(df, BRONZE_REQUIRED_COLUMNS),
            check_non_null(df, BRONZE_REQUIRED_COLUMNS),
            check_datetime_parseable(df, BRONZE_DATETIME_COLUMNS),
            check_allowed_values(df, "event_type", BRONZE_ALLOWED_VALUES["event_type"]),
            check_allowed_values(df, "billing_cycle", BRONZE_ALLOWED_VALUES["billing_cycle"]),
            check_allowed_values(df, "currency", BRONZE_ALLOWED_VALUES["currency"]),
            check_allowed_values(df, "status", BRONZE_ALLOWED_VALUES["status"]),
            check_allowed_values(df, "source", BRONZE_ALLOWED_VALUES["source"]),
            check_column_le(
                df,
                "event_time",
                "ingested_at",
                name="event_time_le_ingested_at",
            ),
        ]
    )

    _add_schema_version_check(result, df)
    _add_price_non_negative_check(result, df)
    _add_plan_catalog_consistency_checks(result, df)
    _add_event_type_status_consistency_check(result, df)

    return result


def _add_schema_version_check(result: ValidationResult, df: pd.DataFrame) -> None:
    if "schema_version" not in df.columns:
        result.add_check(
            name="schema_version_expected",
            passed=False,
            missing_column=True,
            expected_version=BRONZE_EVENT_SCHEMA_VERSION,
        )
        return

    actual_versions = sorted(df["schema_version"].dropna().astype(str).unique().tolist())

    result.add_check(
        name="schema_version_expected",
        passed=actual_versions == [BRONZE_EVENT_SCHEMA_VERSION],
        actual_versions=actual_versions,
        expected_version=BRONZE_EVENT_SCHEMA_VERSION,
    )


def _add_price_non_negative_check(result: ValidationResult, df: pd.DataFrame) -> None:
    if "price" not in df.columns:
        result.add_check(
            name="price_non_negative",
            passed=False,
            missing_column=True,
        )
        return

    price_numeric = pd.to_numeric(df["price"], errors="coerce")
    invalid_numeric_count = int(price_numeric.isna().sum() - df["price"].isna().sum())
    negative_price_count = int((price_numeric < 0).fillna(False).sum())

    result.add_check(
        name="price_non_negative",
        passed=(invalid_numeric_count == 0 and negative_price_count == 0),
        invalid_numeric_count=invalid_numeric_count,
        negative_price_count=negative_price_count,
    )


def _add_plan_catalog_consistency_checks(
    result: ValidationResult,
    df: pd.DataFrame,
) -> None:
    allowed_plan_ids = sorted(PLAN_CATALOG.keys())

    if "plan_id" not in df.columns:
        result.add_check(
            name="plan_id_exists_in_catalog",
            passed=False,
            reason="missing_column",
            column="plan_id",
            allowed_plan_ids=allowed_plan_ids,
        )
        return

    actual_plan_ids = set(df["plan_id"].dropna().unique().tolist())
    unknown_plan_ids = sorted(
        plan_id for plan_id in actual_plan_ids
        if plan_id not in PLAN_CATALOG
    )

    result.add_check(
        name="plan_id_exists_in_catalog",
        passed=len(unknown_plan_ids) == 0,
        allowed_plan_ids=allowed_plan_ids,
        unknown_plan_ids=unknown_plan_ids,
        invalid_row_count=int(df["plan_id"].isin(unknown_plan_ids).sum()) if unknown_plan_ids else 0,
    )

    required_columns = {"plan_id", "price", "billing_cycle"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        result.add_check(
            name="plan_attributes_match_catalog",
            passed=False,
            reason="missing_columns",
            missing_columns=missing_columns,
        )
        return

    invalid_rows: list[dict[str, object]] = []

    for row in df[["plan_id", "price", "billing_cycle"]].dropna(subset=["plan_id"]).itertuples(index=False):
        plan_id = row.plan_id
        if plan_id not in PLAN_CATALOG:
            continue

        expected = PLAN_CATALOG[plan_id]

        price_matches = row.price == expected["price"]
        billing_cycle_matches = row.billing_cycle == expected["billing_cycle"]

        if not (price_matches and billing_cycle_matches):
            invalid_rows.append(
                {
                    "plan_id": plan_id,
                    "actual_price": row.price,
                    "expected_price": expected["price"],
                    "actual_billing_cycle": row.billing_cycle,
                    "expected_billing_cycle": expected["billing_cycle"],
                }
            )

    result.add_check(
        name="plan_attributes_match_catalog",
        passed=len(invalid_rows) == 0,
        invalid_row_count=len(invalid_rows),
        sample_invalid_rows=invalid_rows[:10],
    )


def _add_event_type_status_consistency_check(
    result: ValidationResult,
    df: pd.DataFrame,
) -> None:
    required_columns = {"event_type", "status"}
    missing_columns = sorted(required_columns - set(df.columns))

    if missing_columns:
        result.add_check(
            name="event_type_status_consistency",
            passed=False,
            missing_columns=missing_columns,
        )
        return

    expected_status = df["event_type"].map(EVENT_TYPE_TO_STATUS)
    invalid_mask = expected_status.notna() & (df["status"] != expected_status)

    invalid_pairs = (
        df.loc[invalid_mask, ["event_type", "status"]]
        .drop_duplicates()
        .sort_values(["event_type", "status"])
        .to_dict(orient="records")
    )

    result.add_check(
        name="event_type_status_consistency",
        passed=not invalid_mask.any(),
        invalid_row_count=int(invalid_mask.sum()),
        invalid_pairs=invalid_pairs,
    )
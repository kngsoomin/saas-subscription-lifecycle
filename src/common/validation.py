from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class ValidationCheck:
    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "details": self.details,
        }


@dataclass
class ValidationResult:
    layer: str
    dataset: str
    checks: list[ValidationCheck] = field(default_factory=list)
    row_count_in: int | None = None
    row_count_out: int | None = None
    invalid_row_count: int | None = None
    affected_partitions: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> list[ValidationCheck]:
        return [check for check in self.checks if not check.passed]

    def failed_check_details(self) -> list[dict[str, Any]]:
        return [check.to_dict() for check in self.failed_checks]

    def add_check(
        self,
        name: str,
        passed: bool,
        **details: Any,
    ) -> None:
        self.checks.append(
            ValidationCheck(
                name=name,
                passed=passed,
                details=details,
            )
        )

    def extend(self, checks: list[ValidationCheck]) -> None:
        self.checks.extend(checks)

    def summary(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "dataset": self.dataset,
            "passed": self.passed,
            "row_count_in": self.row_count_in,
            "row_count_out": self.row_count_out,
            "invalid_row_count": self.invalid_row_count,
            "affected_partition_count": len(self.affected_partitions),
            "affected_partitions": self.affected_partitions,
            "failed_checks": [check.name for check in self.failed_checks],
            "checks": [check.to_dict() for check in self.checks],
            "metadata": self.metadata,
        }

    def log_summary(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "dataset": self.dataset,
            "passed": self.passed,
            "row_count_in": self.row_count_in,
            "row_count_out": self.row_count_out,
            "invalid_row_count": self.invalid_row_count,
            "failed_check_count": len(self.failed_checks),
            "failed_checks": [check.name for check in self.failed_checks],
            "affected_partition_count": len(self.affected_partitions),
            "affected_partitions": self.affected_partitions,
            "metadata": self.metadata,
        }

    def raise_if_failed(self) -> None:
        if self.passed:
            return

        failed_names = [check.name for check in self.failed_checks]
        raise ValueError(
            f"Validation failed for {self.layer}.{self.dataset}: {failed_names}"
        )


def check_not_empty(df: pd.DataFrame) -> ValidationCheck:
    return ValidationCheck(
        name="not_empty",
        passed=not df.empty,
        details={
            "row_count": int(len(df)),
        },
    )


def check_required_columns(
    df: pd.DataFrame,
    required_columns: list[str],
) -> ValidationCheck:
    missing_columns = [col for col in required_columns if col not in df.columns]

    return ValidationCheck(
        name="required_columns",
        passed=len(missing_columns) == 0,
        details={
            "required_columns": required_columns,
            "missing_columns": missing_columns,
        },
    )


def check_non_null(
    df: pd.DataFrame,
    columns: list[str],
) -> ValidationCheck:
    null_counts: dict[str, int] = {}

    for col in columns:
        if col not in df.columns:
            null_counts[col] = -1
        else:
            null_counts[col] = int(df[col].isna().sum())

    failed_columns = {
        col: count
        for col, count in null_counts.items()
        if count != 0
    }

    return ValidationCheck(
        name="non_null_columns",
        passed=len(failed_columns) == 0,
        details={
            "null_counts": null_counts,
            "failed_columns": failed_columns,
        },
    )


def check_unique(
    df: pd.DataFrame,
    columns: list[str],
    name: str | None = None,
) -> ValidationCheck:
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        return ValidationCheck(
            name=name or "unique_constraint",
            passed=False,
            details={
                "columns": columns,
                "missing_columns": missing_columns,
            },
        )

    duplicate_count = int(df.duplicated(subset=columns).sum())

    return ValidationCheck(
        name=name or "unique_constraint",
        passed=duplicate_count == 0,
        details={
            "columns": columns,
            "duplicate_count": duplicate_count,
        },
    )


def check_allowed_values(
    df: pd.DataFrame,
    column: str,
    allowed_values: list[Any],
) -> ValidationCheck:
    if column not in df.columns:
        return ValidationCheck(
            name=f"{column}_allowed_values",
            passed=False,
            details={
                "column": column,
                "missing_column": True,
                "allowed_values": allowed_values,
            },
        )

    actual_values = set(df[column].dropna().unique().tolist())
    invalid_values = sorted(value for value in actual_values if value not in set(allowed_values))

    return ValidationCheck(
        name=f"{column}_allowed_values",
        passed=len(invalid_values) == 0,
        details={
            "column": column,
            "allowed_values": allowed_values,
            "invalid_values": invalid_values,
        },
    )


def check_datetime_parseable(
    df: pd.DataFrame,
    columns: list[str],
) -> ValidationCheck:
    invalid_counts: dict[str, int] = {}

    for col in columns:
        if col not in df.columns:
            invalid_counts[col] = -1
            continue

        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        invalid_counts[col] = int(parsed.isna().sum())

    failed_columns = {
        col: count
        for col, count in invalid_counts.items()
        if count != 0
    }

    return ValidationCheck(
        name="datetime_parseable",
        passed=len(failed_columns) == 0,
        details={
            "invalid_counts": invalid_counts,
            "failed_columns": failed_columns,
        },
    )


def check_column_le(
    df: pd.DataFrame,
    left: str,
    right: str,
    name: str | None = None,
) -> ValidationCheck:
    missing_columns = [col for col in [left, right] if col not in df.columns]
    if missing_columns:
        return ValidationCheck(
            name=name or f"{left}_le_{right}",
            passed=False,
            details={
                "left": left,
                "right": right,
                "missing_columns": missing_columns,
            },
        )

    left_dt = pd.to_datetime(df[left], errors="coerce", utc=True)
    right_dt = pd.to_datetime(df[right], errors="coerce", utc=True)

    comparable_mask = left_dt.notna() & right_dt.notna()
    failed_count = int((comparable_mask & (left_dt > right_dt)).sum())

    return ValidationCheck(
        name=name or f"{left}_le_{right}",
        passed=failed_count == 0,
        details={
            "left": left,
            "right": right,
            "failed_count": failed_count,
        },
    )


def check_non_negative(
    df: pd.DataFrame,
    column: str,
    name: str | None = None,
) -> ValidationCheck:
    if column not in df.columns:
        return ValidationCheck(
            name=name or f"{column}_non_negative",
            passed=False,
            details={
                "reason": "missing_column",
                "column": column,
            }
        )

    numeric = pd.to_numeric(df[column], errors="coerce")
    invalid_numeric_count = int(numeric.isna().sum() - df[column].isna().sum())
    negative_count = int((numeric < 0).fillna(False).sum())

    return ValidationCheck(
        name=name or f"{column}_non_negative",
        passed=(invalid_numeric_count == 0 and negative_count == 0),
        details={
            "column": column,
            "invalid_numeric_count": invalid_numeric_count,
            "negative_count": negative_count,
        }
    )


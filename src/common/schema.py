from __future__ import annotations

# -----------------------------
# Bronze event contract
# -----------------------------

BRONZE_EVENT_SCHEMA_VERSION = "1.0"

BRONZE_REQUIRED_COLUMNS = [
    "event_id",
    "event_time",
    "event_type",
    "schema_version",
    "user_id",
    "subscription_id",
    "plan_id",
    "billing_cycle",
    "price",
    "currency",
    "status",
    "source",
    "ingested_at",
]

BRONZE_DATETIME_COLUMNS = [
    "event_time",
    "ingested_at",
]

BRONZE_ALLOWED_VALUES = {
    "event_type": [
        "subscription_created",
        "subscription_renewed",
        "subscription_upgraded",
        "subscription_downgraded",
        "subscription_cancelled",
        "payment_failed",
    ],
    "billing_cycle": ["monthly"],
    "currency": ["USD"],
    "status": ["active", "cancelled", "past_due"],
    "source": ["web", "mobile", "system"],
}

PLAN_CATALOG = {
    "basic_monthly": {"billing_cycle": "monthly", "price": 9.99},
    "pro_monthly": {"billing_cycle": "monthly", "price": 29.99},
    "enterprise_monthly": {"billing_cycle": "monthly", "price": 99.99},
}

PLAN_ORDER = [
    "basic_monthly",
    "pro_monthly",
    "enterprise_monthly",
]

EVENT_TYPE_TO_STATUS = {
    "subscription_created": "active",
    "subscription_renewed": "active",
    "subscription_upgraded": "active",
    "subscription_downgraded": "active",
    "subscription_cancelled": "cancelled",
    "payment_failed": "past_due",
}

DEFAULT_EVENT_SOURCE = "system"
DEFAULT_CURRENCY = "USD"

# -----------------------------
# Silver history contract
# -----------------------------

SILVER_HISTORY_REQUIRED_COLUMNS = [
    "event_id",
    "event_time",
    "subscription_id",
    "user_id",
    "plan_id",
    "billing_cycle",
    "price",
    "currency",
    "status",
    "event_type",
    "ingested_at",
]

SILVER_HISTORY_DATETIME_COLUMNS = [
    "event_time",
    "ingested_at",
]

SILVER_HISTORY_UNIQUE_KEYS = [
    "event_id",
]

SILVER_HISTORY_NON_NULL_COLUMNS = [
    "event_id",
    "subscription_id",
    "event_time",
    "ingested_at",
]

# -----------------------------
# Silver current snapshot contract
# -----------------------------

SILVER_CURRENT_REQUIRED_COLUMNS = [
    "subscription_id",
    "user_id",
    "current_plan_id",
    "current_billing_cycle",
    "current_price",
    "currency",
    "current_status",
    "last_event_type",
    "last_event_time",
    "snapshot_time",
]

SILVER_CURRENT_DATETIME_COLUMNS = [
    "last_event_time",
    "snapshot_time",
]

SILVER_CURRENT_UNIQUE_KEYS = [
    "subscription_id",
]

SILVER_ALLOWED_VALUES = {
    "current_status": BRONZE_ALLOWED_VALUES["status"],
}

# -----------------------------
# History -> Current consistency mapping
# -----------------------------

CURRENT_HISTORY_COMPARISON_MAP = {
    "current_plan_id": "plan_id",
    "current_billing_cycle": "billing_cycle",
    "current_price": "price",
    "current_status": "status",
    "last_event_type": "event_type",
    "last_event_time": "event_time",
}

# -----------------------------
# Gold KPI daily contract
# -----------------------------

GOLD_KPI_DAILY_REQUIRED_COLUMNS = [
    "date",
    "new_subscriptions",
    "new_cancellations",
    "active_subscriptions",
    "mrr",
    "currency",
    "snapshot_time",
]

GOLD_KPI_DAILY_UNIQUE_KEYS = [
    "date",
]

GOLD_KPI_DAILY_ALLOWED_VALUES = {
    "currency": BRONZE_ALLOWED_VALUES["currency"],
}

GOLD_KPI_DAILY_DATETIME_COLUMNS = [
    "snapshot_time",
]
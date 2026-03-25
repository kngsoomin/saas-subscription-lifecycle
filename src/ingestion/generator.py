from __future__ import annotations
import json
import random
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from typing import Dict, List
from uuid import uuid4

from src.common.constants import (
    DEFAULT_GENERATOR_SEQ_PATH,
    DEFAULT_GENERATOR_STATE_CURRENT_PATH
)
from src.common.storage import LocalStorage, Storage


PLAN_CATALOG = {
    "basic_monthly": {"billing_cycle": "monthly", "price": 9.99},
    "pro_monthly": {"billing_cycle": "monthly", "price": 29.99},
    "enterprise_monthly": {"billing_cycle": "monthly", "price": 99.99},
}

PLAN_ORDER = ["basic_monthly", "pro_monthly", "enterprise_monthly"]

SCHEMA_VERSION = "1.0"
CURRENCY = "USD"
SOURCE = "system"


@dataclass
class SubscriptionState:
    subscription_id: str
    user_id: str
    plan_id: str
    billing_cycle: str
    price: float
    currency: str
    status: str
    last_event_time: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_utc_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_last_subscription_seq(
    path: str,
    storage: Storage | None = None
) -> int:
    storage = storage or LocalStorage()
    if not storage.exists(path):
        return 0
    with storage.open_text_read(path) as f:
        return int(f.read().strip())


def save_last_subscription_seq(
    path: str,
    seq: int,
    storage: Storage | None = None
) -> None:
    storage = storage or LocalStorage()
    with storage.open_text_write(path) as f:
        f.write(str(seq))


def load_state(
    path: str,
    storage: Storage | None = None
) -> Dict[str, SubscriptionState]:
    storage = storage or LocalStorage()
    if not storage.exists(path):
        return {}
    with storage.open_text_read(path) as f:
        raw = json.load(f)

    return {k: SubscriptionState(**v) for k, v in raw.items()}


def save_state(
    path: str,
    state: Dict[str, SubscriptionState],
    storage: Storage | None = None
) -> None:
    storage = storage or LocalStorage()
    payload = {k: asdict(v) for k, v in state.items()}
    with storage.open_text_write(path) as f:
        json.dump(payload, f)


def build_event(
        *,
        event_time: datetime,
        event_type: str,
        state: SubscriptionState,
        ingested_at: datetime,
    ) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_time": format_utc_datetime(event_time),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "user_id": state.user_id,
        "subscription_id": state.subscription_id,
        "plan_id": state.plan_id,
        "billing_cycle": state.billing_cycle,
        "price": state.price,
        "currency": state.currency,
        "status": state.status,
        "source": SOURCE,
        "ingested_at": format_utc_datetime(ingested_at),
    }


def get_allowed_next_events(state: SubscriptionState) -> List[str]:
    allowed: List[str] = []

    if state.status == "active":
        allowed.extend([
            "subscription_renewed",
            "payment_failed",
            "subscription_cancelled",
        ])

        if state.plan_id != PLAN_ORDER[-1]:
            allowed.append("subscription_upgraded")
        if state.plan_id != PLAN_ORDER[0]:
            allowed.append("subscription_downgraded")

    elif state.status == "past_due":
        allowed.extend([
            "subscription_renewed",
            "subscription_cancelled",
        ])

    return allowed


def get_next_plan_for_upgrade(plan_id: str) -> str:
    idx = PLAN_ORDER.index(plan_id)
    return PLAN_ORDER[min(idx + 1, len(PLAN_ORDER) - 1)]


def get_next_plan_for_downgrade(plan_id: str) -> str:
    idx = PLAN_ORDER.index(plan_id)
    return PLAN_ORDER[max(idx - 1, 0)]


def apply_event(state: SubscriptionState, event_type: str, event_time: datetime) -> None:
    if event_type == "subscription_renewed":
        state.status = "active"

    elif event_type == "payment_failed":
        state.status = "past_due"

    elif event_type == "subscription_cancelled":
        state.status = "cancelled"

    elif event_type == "subscription_upgraded":
        new_plan = get_next_plan_for_upgrade(state.plan_id)
        state.plan_id = new_plan
        state.billing_cycle = PLAN_CATALOG[new_plan]["billing_cycle"]
        state.price = PLAN_CATALOG[new_plan]["price"]
        state.status = "active"

    elif event_type == "subscription_downgraded":
        new_plan = get_next_plan_for_downgrade(state.plan_id)
        state.plan_id = new_plan
        state.billing_cycle = PLAN_CATALOG[new_plan]["billing_cycle"]
        state.price = PLAN_CATALOG[new_plan]["price"]
        state.status = "active"

    state.last_event_time = format_utc_datetime(event_time)


def create_new_subscription(
        *,
        seq: int,
        ingested_at: datetime,
    ) -> tuple[SubscriptionState, dict]:
    start_time = utc_now() - timedelta(
        hours=random.randint(0, 5),
        minutes=random.randint(1, 59),
    )

    plan_id = random.choice(PLAN_ORDER)

    state = SubscriptionState(
        subscription_id=f"sub_{seq:05d}",
        user_id=f"user_{seq:05d}",
        plan_id=plan_id,
        billing_cycle=PLAN_CATALOG[plan_id]["billing_cycle"],
        price=PLAN_CATALOG[plan_id]["price"],
        currency=CURRENCY,
        status="active",
        last_event_time=format_utc_datetime(start_time),
    )

    event = build_event(
        event_time=start_time,
        event_type="subscription_created",
        state=state,
        ingested_at=ingested_at,
    )
    return state, event

def generate_followup_event(
        *,
        state: SubscriptionState,
        ingested_at: datetime,
    ) -> dict | None:
    allowed = get_allowed_next_events(state)

    if not allowed:
        return None

    event_type = random.choice(allowed)

    now = utc_now()
    for _ in range(3):
        next_time = parse_utc_datetime(state.last_event_time) + timedelta(
            minutes=random.randint(1, 30),
        )
        if next_time <= now:
            break
    else:
        return None

    apply_event(state, event_type, next_time)

    return build_event(
        event_time=next_time,
        event_type=event_type,
        state=state,
        ingested_at=ingested_at,
    )


def generate_mock_events(
        *,
        state_path: str = DEFAULT_GENERATOR_STATE_CURRENT_PATH,
        seq_path: str = DEFAULT_GENERATOR_SEQ_PATH,
        new_subscription_range: tuple[int, int] = (2, 3),
        max_existing_updates: int = 5,
        storage: Storage | None = None,
    ) -> List[dict]:
    ingested_at = utc_now()
    storage = storage or LocalStorage() # entry point

    state_map = load_state(state_path, storage=storage)
    last_seq = load_last_subscription_seq(seq_path, storage=storage)

    events: List[dict] = []

    existing_subscriptions = [s for s in state_map.values() if s.status in ["active", "past_due"]]
    random.shuffle(existing_subscriptions)

    for state in existing_subscriptions[:max_existing_updates]:
        event = generate_followup_event(state=state, ingested_at=ingested_at)
        if event:
            events.append(event)

    new_cnt = random.randint(*new_subscription_range)
    for _ in range(new_cnt):
        last_seq += 1
        state, event = create_new_subscription(seq=last_seq, ingested_at=ingested_at)
        state_map[state.subscription_id] = state
        events.append(event)

    save_state(state_path, state_map, storage=storage)
    save_last_subscription_seq(seq_path, last_seq, storage=storage)

    return events


if __name__ == "__main__":
    generate_mock_events()




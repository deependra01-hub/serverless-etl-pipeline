from __future__ import annotations

import hashlib
import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone


CHANNELS = ("organic", "paid_search", "social", "email", "affiliate", "direct")
DEVICE_TYPES = ("mobile", "desktop", "tablet")
BROWSERS = ("chrome", "safari", "edge", "firefox")
COUNTRIES = ("US", "IN", "GB", "DE", "AU", "SG")
EVENT_TYPES = (
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_started",
    "search",
    "ad_click",
)
PAYMENT_METHODS = ("card", "wallet", "upi", "paypal")
PRODUCT_CATEGORIES = ("fashion", "electronics", "home", "grocery", "beauty", "sports")


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _hash_ip(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


@dataclass
class SessionContext:
    session_id: str
    user_id: str
    anonymous_id: str
    channel: str
    device_type: str
    browser: str
    country_code: str
    campaign_id: str | None
    referrer: str


class TrafficGenerator:
    def __init__(self, schema_version: int = 1, seed: int = 7) -> None:
        self.schema_version = schema_version
        self.random = random.Random(seed)

    def make_session(self) -> SessionContext:
        channel = self.random.choices(CHANNELS, weights=(28, 24, 16, 14, 10, 8), k=1)[0]
        campaign_id = f"cmp_{self.random.randint(1000, 9999)}" if channel != "direct" else None
        referrer = f"https://{channel.replace('_', '')}.example.com"
        return SessionContext(
            session_id=_id("sess"),
            user_id=_id("usr"),
            anonymous_id=_id("anon"),
            channel=channel,
            device_type=self.random.choices(DEVICE_TYPES, weights=(61, 33, 6), k=1)[0],
            browser=self.random.choice(BROWSERS),
            country_code=self.random.choice(COUNTRIES),
            campaign_id=campaign_id,
            referrer=referrer,
        )

    def generate_clickstream_event(self, session: SessionContext) -> dict:
        event_type = self.random.choices(
            EVENT_TYPES, weights=(34, 24, 13, 6, 18, 5), k=1
        )[0]
        product_id = (
            f"sku_{self.random.randint(100000, 999999)}"
            if event_type in {"product_view", "add_to_cart", "checkout_started"}
            else None
        )
        category = self.random.choice(PRODUCT_CATEGORIES) if product_id else None
        page_slug = product_id or event_type
        return {
            "event_id": _id("evt"),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "session_id": session.session_id,
            "user_id": session.user_id,
            "anonymous_id": session.anonymous_id,
            "page_url": f"/{category or 'browse'}/{page_slug}",
            "product_id": product_id,
            "category": category,
            "campaign_id": session.campaign_id,
            "channel": session.channel,
            "device_type": session.device_type,
            "browser": session.browser,
            "country_code": session.country_code,
            "referrer": session.referrer,
            "ip_hash": _hash_ip(session.session_id),
            "schema_version": self.schema_version,
        }

    def maybe_generate_order(self, session: SessionContext, probability: float) -> dict | None:
        if self.random.random() > probability:
            return None
        order_value = round(max(18, self.random.lognormvariate(3.6, 0.55)), 2)
        return {
            "order_id": _id("ord"),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "session_id": session.session_id,
            "user_id": session.user_id,
            "status": self.random.choices(
                ("authorized", "captured", "declined"), weights=(18, 77, 5), k=1
            )[0],
            "payment_method": self.random.choices(
                PAYMENT_METHODS, weights=(51, 17, 22, 10), k=1
            )[0],
            "currency": "USD",
            "order_value": order_value,
            "items_count": math.ceil(self.random.uniform(1, 5)),
            "fraud_score": round(self.random.random(), 4),
            "shipping_country": session.country_code,
            "schema_version": self.schema_version,
        }

    def rate_sleep(self, events_per_minute: int, burst_factor: float, events_emitted: int) -> None:
        effective_rate_per_second = (events_per_minute * burst_factor) / 60
        if effective_rate_per_second <= 0:
            return
        delay = max((events_emitted / effective_rate_per_second) - 1, 0)
        if delay > 0:
            time.sleep(min(delay, 0.25))

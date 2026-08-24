"""Single source of truth for dataset shape, volumes, eras and dirt rates.

Everything here is deterministic. Assignment docs quote these numbers; if you
change anything, regenerate the data and re-run datagen/selftest.py.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, timedelta

GENERATOR_VERSION = "1.0"

# ---------------------------------------------------------------- calendar ---
EPOCH = date(2026, 1, 1)          # order_id numbering epoch
MONTH1_START = date(2026, 6, 1)
MONTH1_END = date(2026, 7, 15)    # era v1: 45 files
MONTH2_START = date(2026, 7, 16)
MONTH2_END = date(2026, 8, 8)     # 24 files
V2_START = date(2026, 7, 16)      # era v2: adds currency/discount_amount/channel
V3_START = date(2026, 8, 1)       # era v3: breaking changes
CORRUPT_DATE = date(2026, 7, 22)  # structurally broken lines live only here
SPIKE_DATES = {date(2026, 6, 19), date(2026, 7, 11)}  # flash-sale days, x3 volume
LATE_WINDOW_DAYS = 7              # corrections reach at most 7 days back

# ------------------------------------------------------------------ volume ---
BASE_ROWS = {"full": 1_200_000, "small": 60_000}
N_CUSTOMERS = {"full": 1_200_000, "small": 60_000}
N_PRODUCTS = {"full": 40_000, "small": 2_000}
N_STORES = 400                    # same at both scales
CORRUPT_LINES = {"full": 3_000, "small": 150}   # + exactly 1 unclosed-quote line

WEEKEND_MULT = 0.75
SPIKE_MULT = 3.0

# ------------------------------------------------------------- dirt rates ----
# Orders feed (fractions of rows unless stated). See SPEC.md §3.5.
LATE_FRAC = 0.015
DUP_FRAC = 0.0005
TS_DDMM = 0.004          # DD/MM/YYYY HH:MM:SS
TS_ISOT = 0.003          # YYYY-MM-DDTHH:MM:SS
TS_IMPOSSIBLE = 0.0002   # day '32' -> unparseable
STATUS_JUNK = 0.003
PAY_CASING = 0.003
TOTAL_NA = 0.002         # 'N/A' or empty
TOTAL_NEG = 0.001
TOTAL_COMMA = 0.003      # '1,234.56'
TOTAL_DOLLAR = 0.002     # '$79.98'
TOTAL_MISMATCH = 0.005   # total deliberately != items sum
CUST_EMPTY = 0.0015
CUST_ORPHAN = 0.0005
CUST_FLOAT = 0.002       # '123456.0'
SKU_ORPHAN_FRAC = 0.002  # per item line
ITEMS_STRTYPE = 0.003    # per row: qty/unit_price as JSON strings

# Customers dim
CUST_SIGNUP_DDMM = 0.10
CUST_SIGNUP_EPOCH = 0.08
CUST_SIGNUP_EMPTY = 0.02
CUST_NAME_SHOUT = 0.02
CUST_NAME_LASTFIRST = 0.03
CUST_NAME_PAD = 0.02
CUST_EMAIL_DUP = 0.003
CUST_EMAIL_INVALID = 0.005
CUST_COUNTRY_EMPTY = 0.01

# Products dim
PROD_PRICE_COMMA = 0.005

# Era v2 extras
CURRENCIES = ["USD", "EUR", "GBP", "VND"]
CURRENCY_P = [0.94, 0.03, 0.02, 0.01]
VND_FACTOR = 25_000
DISCOUNT_NONZERO = 0.20  # of rows; up to 30% of items total

DIM_SEED = 424_242


# ----------------------------------------------------------------- helpers ---
def seed_for(d: date) -> int:
    """Stable per-date seed: same date always regenerates the same file."""
    return int(hashlib.md5(d.isoformat().encode()).hexdigest()[:8], 16)


def era_for(d: date) -> str:
    if d >= V3_START:
        return "v3"
    if d >= V2_START:
        return "v2"
    return "v1"


def id_base(d: date) -> int:
    """First order_id of day d (numeric part)."""
    return (d - EPOCH).days * 10_000_000


def rows_for(d: date, scale: str) -> int:
    """Total data rows in the file for day d (base + late + dup)."""
    mult = SPIKE_MULT if d in SPIKE_DATES else (WEEKEND_MULT if d.weekday() >= 5 else 1.0)
    jitter = 0.95 + 0.10 * random.Random(seed_for(d) + 1).random()
    return int(BASE_ROWS[scale] * mult * jitter)


def late_for(d: date, scale: str) -> int:
    if d <= MONTH1_START:
        return 0
    return int(rows_for(d, scale) * LATE_FRAC)


def dup_for(d: date, scale: str) -> int:
    return int(rows_for(d, scale) * DUP_FRAC)


def base_rows_for(d: date, scale: str) -> int:
    """Rows with fresh order_ids on day d; defines the id range other days may
    reference in late-correction rows: [id_base(d), id_base(d) + base_rows_for(d))."""
    return rows_for(d, scale) - late_for(d, scale) - dup_for(d, scale)


def all_dates(month: str = "all"):
    def _range(a: date, b: date):
        cur = a
        while cur <= b:
            yield cur
            cur += timedelta(days=1)

    if month in ("1", "all"):
        yield from _range(MONTH1_START, MONTH1_END)
    if month in ("2", "all"):
        yield from _range(MONTH2_START, MONTH2_END)


# Column order per era (SPEC §3.3) — position changes between eras on purpose.
COLUMNS_V1 = ["order_id", "customer_id", "store_id", "order_ts", "updated_at",
              "status", "payment_method", "order_total", "items", "meta"]
COLUMNS_V2 = COLUMNS_V1 + ["currency", "discount_amount", "channel"]
COLUMNS_V3 = ["order_id", "customer_id", "store_id", "order_ts", "updated_at",
              "status", "payment_type", "order_total", "currency",
              "discount_amount", "channel", "loyalty_tier", "items", "meta"]
COLUMNS = {"v1": COLUMNS_V1, "v2": COLUMNS_V2, "v3": COLUMNS_V3}

STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "refunded"]
STATUS_P = [0.05, 0.25, 0.20, 0.40, 0.06, 0.04]
PAYMENTS = ["card", "wallet", "bank_transfer", "cod", "paypal"]
PAYMENT_P = [0.45, 0.20, 0.12, 0.13, 0.10]
CHANNELS = ["web", "app", "marketplace", "phone"]
CHANNEL_P = [0.40, 0.35, 0.20, 0.05]
DEVICES = ["ios", "android", "web", "desktop"]
DEVICE_P = [0.30, 0.35, 0.25, 0.10]
LOYALTY = ["", "bronze", "silver", "gold", "platinum"]
LOYALTY_P = [0.70, 0.12, 0.10, 0.06, 0.02]
N_ITEMS_P = [0.30, 0.28, 0.18, 0.12, 0.07, 0.05]     # for 1..6 items
QTY_P = [0.55, 0.25, 0.10, 0.06, 0.04]               # for qty 1..5

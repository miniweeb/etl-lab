"""Every pathology the lab cleans (SPEC 3.5), pinned as a test forever."""
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from work.a03_transformation.a03_cleaners import (
    PAYMENT_VALUES, STATUS_VALUES, clean_bool, clean_enum, clean_id,
    clean_money, clean_order_ts, clean_signup_ts)


@pytest.fixture
def con():
    c = duckdb.connect()                # in-memory: born empty, gone after the test
    c.execute("SET TimeZone='UTC';")    # module assumption: epochs decode as UTC
    yield c
    c.close()


def check(con, expr, cases):
    """Run one cleaner over a VALUES table of (dirty, expected); compare row by row."""
    placeholders = ", ".join("(?)" for _ in cases)
    got = [row[0] for row in con.execute(
        f"SELECT {expr} FROM (VALUES {placeholders}) t(v)",
        [dirty for dirty, _ in cases]).fetchall()]
    assert got == [expected for _, expected in cases]


def test_money(con):
    check(con, clean_money("v"), [
        ("79.98",    Decimal("79.98")),     # clean passes through
        ("93,32",    Decimal("93.32")),     # European decimal comma
        ("1.234,56", Decimal("1234.56")),   # comma + thousands dot
        ("$79.98",   Decimal("79.98")),     # currency prefix
        ("N/A",      None),
        (None,       None),                 # empty CSV field
        ("-42.10",   Decimal("-42.10")),    # negatives survive: A05's call, not ours
    ])


def test_status(con):
    check(con, clean_enum("v", STATUS_VALUES), [
        ("delivered", "delivered"),
        ("PAID ",     "paid"),              # casing + padding
        ("Paid",      "paid"),
        ("unknown",   None),                # junk -> NULL, never a guess
        (None,        None),
    ])

def test_order_ts(con):
    check(con, clean_order_ts("v"), [
        ("2026-06-02 14:30:00",  datetime(2026, 6, 2, 14, 30, 0)),   # ISO
        ("19/06/2026 08:15:00",  datetime(2026, 6, 19, 8, 15, 0)),   # day-first
        ("2026-06-02T14:30:00",  datetime(2026, 6, 2, 14, 30, 0)),   # ISO-T
        ("2026-06-31 12:00:00",  None),   # ngày bất khả thi -> NULL
        (None,                   None),
    ])


def test_customer_id(con):
    check(con, clean_id("v"), [
        ("123456.0", 123456),    # Excel-float
        ("789",      789),
        ("",         None),
        ("abc",      None),
        (None,       None),
    ])


def test_payment(con):
    check(con, clean_enum("v", PAYMENT_VALUES), [
        ("card",      "card"),
        ("CARD",      "card"),
        (" cod ",     "cod"),          # padding + casing
        ("bitcoin",   None),           # ngoài whitelist
        (None,        None),
    ])


def test_signup_ts(con):
    check(con, clean_signup_ts("v"), [
        ("2024-04-05 18:14:38", datetime(2024, 4, 5, 18, 14, 38)),
        ("05/04/2024",          datetime(2024, 4, 5, 0, 0, 0)),
        ("1712345678",          datetime(2024, 4, 5, 19, 34, 38)),  # epoch, UTC
        (None,                  None),
    ])


def test_is_active(con):
    check(con, clean_bool("v"), [
        ("true",  True),   ("1", True),  ("yes", True),  ("Y", True),
        ("false", False),  ("0", False), ("no", False),  ("N", False),
        ("maybe", None),
        (None,    None),
    ])
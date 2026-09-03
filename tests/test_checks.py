"""A05 stretch 4 — ghim hai biểu thức check, mãi mãi.

Cùng khuôn với tests/test_cleaners.py của A03: chạy MỘT luật trên bảng VALUES
gồm các dòng chọn tay, trong DuckDB in-memory, so từng dòng.

Khác biệt so với A03: cleaner nhận MỘT giá trị, còn check ở đây nhận NHIỀU cột
(total vs items_total; order_ts vs business date), nên bảng VALUES có nhiều cột.

    python -m pytest tests/ -q
"""
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest


@pytest.fixture
def con():
    c = duckdb.connect()                # in-memory: sinh ra rỗng, chết sau test
    c.execute("SET TimeZone='UTC';")
    yield c
    c.close()


# Biểu thức DƯỚI TEST — chép nguyên văn từ a05_validate_day.flag_rows.
# (Ghim ở đây nghĩa là: sửa bên kia mà quên bên này thì test đỏ, đúng ý đồ.)
TOTAL_MISMATCH = """
    CASE WHEN total >= 0 AND items_total IS NOT NULL
          AND abs(total - items_total) > 0.01
         THEN 'total_mismatch' END
"""

TS_OUT_OF_RANGE = """
    CASE WHEN ts IS NOT NULL AND NOT (
           ts >= GREATEST(DATE '2026-06-01', DATE '2026-06-10' - INTERVAL 7 DAY)
       AND ts <  DATE '2026-06-10' + INTERVAL 1 DAY)
         THEN 'ts_out_of_range' END
"""


def run(con, expr, cols, cases):
    """cases = [((giá trị cột...), kỳ vọng), ...]"""
    values = ", ".join(f"({', '.join('?' for _ in cols)})" for _ in cases)
    params = [v for inputs, _ in cases for v in inputs]
    got = [r[0] for r in con.execute(
        f"SELECT {expr} FROM (VALUES {values}) t({', '.join(cols)})",
        params).fetchall()]
    assert got == [expected for _, expected in cases]


def test_total_mismatch(con):
    # TODO(you): viết các case. Biên là điểm quan trọng nhất —
    #   bằng nhau, lệch ĐÚNG 0.01 (biên PHẢI PASS), lệch 0.02 (fail),
    #   items_total NULL (không phán xét), và total âm (đã bị
    #   negative_total bắt rồi, không được bắt lần hai).
    run(con, TOTAL_MISMATCH, ["total", "items_total"], [
        ((Decimal("79.98"), 79.98), None),
        # ...
    ])


def test_ts_out_of_range(con):
    # TODO(you): trong cửa sổ, ngày TRƯỚC cửa sổ, ngày SAU cửa sổ, và NULL
    #   (NULL đã thuộc về bad_order_ts, check này phải im lặng).
    run(con, TS_OUT_OF_RANGE, ["ts"], [
        ((datetime(2026, 6, 10, 12, 0, 0),), None),
        # ...
    ])
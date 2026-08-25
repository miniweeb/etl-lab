# work/a02_partitioning/a02_build_lake.py
"""A02 — dựng lake Parquet phân vùng, mỗi lần một ngày.

Chạy:  python -m work.build_lake
Task 2 dùng bộ hằng số ĐỎ; Task 3 đổi sang bộ XANH.
"""
import os
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

BASE = Path(os.environ.get("ETL_LAB_DATA", Path(__file__).resolve().parents[1] / "data"))
SCALE = "full"                          # Task 6 đổi thành "full"
ROOT = BASE / "small" if SCALE == "small" else BASE

RAW_ORDERS = ROOT / "raw" / "orders"
LAKE_ORDERS = ROOT / "lake" / "orders"
TMP = ROOT / "tmp"

# ── hai hằng số của cả bài ───────────────────────────────────────────
FIRST_DAY = date(2026, 6, 1)
LAST_DAY = date(2026, 7, 15)              # Task 2: 2 ngày | Task 3: date(2026, 7, 15)
PARTITION_MODE = "APPEND"   # Task 2: cái này | Task 3: "APPEND"

TS_FORMATS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"
V1_COLUMNS = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
     'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
     'payment_method':'VARCHAR','order_total':'VARCHAR',
     'items':'VARCHAR','meta':'VARCHAR'}"""

con = duckdb.connect()   # in-memory: lake chỉ là file, chưa cần database
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{TMP.as_posix()}';")


def load_day(day: date) -> int:
    """Đọc 1 CSV thô, suy ra order_date, ghi vào lake phân vùng."""
    src = RAW_ORDERS / f"orders_{day}.csv"
    result = con.execute(f"""
        COPY (
            SELECT *,
                   try_strptime(order_ts, {TS_FORMATS}) AS order_ts_clean,
                   CAST(try_strptime(order_ts, {TS_FORMATS}) AS DATE) AS order_date
            FROM read_csv('{src.as_posix()}', header=true, columns={V1_COLUMNS})
        ) TO '{LAKE_ORDERS.as_posix()}'
        (FORMAT parquet, PARTITION_BY (order_date), {PARTITION_MODE},
         ROW_GROUP_SIZE 122880);
    """)
    return result.fetchone()[0]          # COPY báo lại đã ghi bao nhiêu dòng


if __name__ == "__main__":
    LAKE_ORDERS.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)

    print(f"scale={SCALE}  mode={PARTITION_MODE}  {FIRST_DAY} .. {LAST_DAY}")
    day = FIRST_DAY
    t_total = time.perf_counter()
    while day <= LAST_DAY:
        t0 = time.perf_counter()
        rows = load_day(day)
        print(f"{day}  {rows:>9,} rows  {time.perf_counter() - t0:6.2f} s")
        day += timedelta(days=1)
    print(f"TOTAL {time.perf_counter() - t_total:8.1f} s")
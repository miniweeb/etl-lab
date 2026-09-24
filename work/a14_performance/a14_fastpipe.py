"""A14 Task 4 — work/fastpipe.py: badpipe sau lần sửa 1 (vòng lặp -> 1 câu SQL).

  python -m work.a14_performance.a14_fastpipe 2026-06-05

Ghi ra tmp/perf_fixed/ chứ không phải tmp/perf/ — GIỮ CẢ HAI để so bằng EXCEPT.
Đây là bản CHỈ sửa fix 1. Các fix 2-6 chưa đụng vào, giữ y hệt badpipe.
"""
from __future__ import annotations

import sys
import time

import duckdb

# from work.a14_performance.a14_bootstrap import DIMS, LAKE, SCALE, TMP
from work.a14_performance.a14_bootstrap import DIMS, LAKE, SCALE, SIX, TMP
# lần 4: thêm SIX

DAY = sys.argv[1] if len(sys.argv) > 1 else "2026-06-05"
OUT = TMP / "perf_fixed"
OUT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{TMP.as_posix()}';")

print(f"fastpipe [{SCALE}]  ngày {DAY}  ->  {OUT}")
t_all = t0 = time.perf_counter()


def step(name: str) -> None:
    global t0
    print(f"  {name:10s} {time.perf_counter() - t0:8.2f} s", flush=True)
    t0 = time.perf_counter()


# ── bước 1: cắt một ngày ra khỏi lake ───────────────────────────────────────
# CHƯA SỬA — vẫn strftime, vẫn DISTINCT, vẫn ORDER BY, vẫn SELECT *
con.execute(f"""
    CREATE TABLE day_raw AS
    SELECT {SIX}
    FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)
    WHERE order_date = DATE '{DAY}'""")
step("slice")
# lần 1 (chỉ đổi dòng WHERE, giữ nguyên DISTINCT/ORDER BY/SELECT * — chưa tới lượt fix 3, 4)
# lần 2 xoá DISTINCT và ORDER BY (đã chứng minh vô dụng ở trên rồi, xoá được)

# ── bước 2: kiểm tra ────────────────────────────────────────────────────────
# FIX 1 — CÁI LOOP THÀNH MỘT CÂU LỆNH.
con.execute("CREATE TABLE day_ok AS "
            "SELECT * FROM day_raw WHERE order_total IS NOT NULL")
step("validate")

# ── bước 3: bản trích cho finance ───────────────────────────────────────────
# CHƯA SỬA — vẫn ROW_GROUP_SIZE 5000 -> 122880
con.execute(f"""
    COPY (SELECT * FROM day_ok)
    TO '{(OUT / f"orders_{DAY}.parquet").as_posix()}'
    (FORMAT parquet, ROW_GROUP_SIZE 122880)""") 
step("extract")

# ── bước 4: mart store_daily ────────────────────────────────────────────────
# CHƯA SỬA — vẫn LEFT JOIN customers vô ích
# con.execute(f"""
#     COPY (
#         SELECT o.store_id, s.store_name, s.region,
#                count(*) AS orders, sum(o.order_total) AS revenue
#         FROM day_ok o
#         LEFT JOIN read_csv('{(DIMS / "customers.csv").as_posix()}', header=true) c
#                ON o.customer_id = c.customer_id
#         JOIN read_csv('{(DIMS / "stores.csv").as_posix()}', header=true) s
#           ON o.store_id = s.store_id
#         GROUP BY 1, 2, 3 ORDER BY revenue DESC
#     ) TO '{(OUT / f"store_daily_{DAY}.parquet").as_posix()}' (FORMAT parquet)""")
# fix 5 bỏ left join
con.execute(f"""
    COPY (
        SELECT o.store_id, s.store_name, s.region,
               count(*) AS orders, sum(o.order_total) AS revenue
        FROM day_ok o
        JOIN read_csv('{(DIMS / "stores.csv").as_posix()}', header=true) s
          ON o.store_id = s.store_id
        GROUP BY 1, 2, 3 ORDER BY revenue DESC
    ) TO '{(OUT / f"store_daily_{DAY}.parquet").as_posix()}' (FORMAT parquet)""")
step("mart")

print(f"  {'TOTAL':10s} {time.perf_counter() - t_all:8.2f} s")
con.close()
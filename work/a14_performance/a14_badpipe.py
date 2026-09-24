"""A14 Task 3 — work/badpipe.py: job nightly store_daily bạn vừa thừa kế.

  python -m work.a14_performance.a14_badpipe 2026-06-05
  $env:ETL_LAB_SCALE="full"; python -m work.a14_performance.a14_badpipe 2026-06-05

Nó ĐÚNG. Với một ngày cho trước, nó sinh ra dưới <DATA_ROOT>/tmp/perf/:
  orders_<day>.parquet       bản trích cho đội finance (sáu cột hợp đồng:
                             order_id, customer_id, store_id, order_ts,
                             status, order_total)
  store_daily_<day>.parquet  mỗi cửa hàng một dòng: số đơn, doanh thu

Luật của trò chơi: làm cho nó NHANH mà KHÔNG đổi thứ nó sinh ra.

ĐỌC NHƯ ĐỌC HIỆN TRƯỜNG VỤ ÁN, ĐỪNG SỬA GÌ HÔM NAY. Việc của Task 3 là chạy
nó, ghi lại thời gian TỪNG BƯỚC, rồi liệt kê ít nhất bảy cái sai — mỗi cái kèm
theo nó nằm trong bước nào và bước đó tốn bao nhiêu giây.

Cảnh báo trước khi chạy ở full scale (Task 5): bước validate mất khoảng HAI
MƯƠI PHÚT. Đó là con số bạn cần có trong văn bản, nên cứ để nó chạy hết.
"""
from __future__ import annotations

import sys
import time

import duckdb

from work.a14_performance.a14_bootstrap import DIMS, LAKE, SCALE, TMP

DAY = sys.argv[1] if len(sys.argv) > 1 else "2026-06-05"
OUT = TMP / "perf"
OUT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{TMP.as_posix()}';")

print(f"badpipe [{SCALE}]  ngày {DAY}  ->  {OUT}")
t_all = t0 = time.perf_counter()


def step(name: str) -> None:
    global t0
    print(f"  {name:10s} {time.perf_counter() - t0:8.2f} s", flush=True)
    t0 = time.perf_counter()


# ── bước 1: cắt một ngày ra khỏi lake ───────────────────────────────────────
con.execute(f"""
    CREATE TABLE day_raw AS
    SELECT DISTINCT *
    FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)
    WHERE strftime(order_ts, '%Y-%m-%d') = '{DAY}'
    ORDER BY order_ts""")
step("slice")

# ── bước 2: "kiểm tra từng dòng bằng Python trước khi tin nó" ────────────────
cur = con.execute("SELECT * FROM day_raw")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
i_total = cols.index("order_total")
con.execute("CREATE TABLE day_ok AS SELECT * FROM day_raw LIMIT 0")
marks = ", ".join(["?"] * len(cols))
for r in rows:
    if r[i_total] is not None:              # toàn bộ phần "kiểm tra" là đây
        con.execute(f"INSERT INTO day_ok VALUES ({marks})", r)
step("validate")

# ── bước 3: bản trích cho đội finance ───────────────────────────────────────
con.execute(f"""
    COPY (SELECT * FROM day_ok)
    TO '{(OUT / f"orders_{DAY}.parquet").as_posix()}'
    (FORMAT parquet, ROW_GROUP_SIZE 5000)""")
step("extract")

# ── bước 4: mart store_daily ────────────────────────────────────────────────
con.execute(f"""
    COPY (
        SELECT o.store_id, s.store_name, s.region,
               count(*) AS orders, sum(o.order_total) AS revenue
        FROM day_ok o
        LEFT JOIN read_csv('{(DIMS / "customers.csv").as_posix()}', header=true) c
               ON o.customer_id = c.customer_id
        JOIN read_csv('{(DIMS / "stores.csv").as_posix()}', header=true) s
          ON o.store_id = s.store_id
        GROUP BY 1, 2, 3 ORDER BY revenue DESC
    ) TO '{(OUT / f"store_daily_{DAY}.parquet").as_posix()}' (FORMAT parquet)""")
step("mart")

print(f"  {'TOTAL':10s} {time.perf_counter() - t_all:8.2f} s")
con.close()
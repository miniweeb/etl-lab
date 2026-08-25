# %%
import os
print(os.environ.get("ETL_LAB_DATA"))

# %% work/a02_partitioning/a02_explore.py
"""A02 — khảo sát: 1 file chứa 8 ngày, khám nghiệm vụ mất dữ liệu, chứng minh pruning."""
import os
if os.path.basename(os.getcwd()) == "work":
    os.chdir("..")
print(os.getcwd())

# %%
from pathlib import Path

import duckdb

BASE = Path(os.environ["ETL_LAB_DATA"])
SCALE = "small"
ROOT = BASE / "small" if SCALE == "small" else BASE
LAKE = (ROOT / "lake" / "orders").as_posix()
GLOB = f"{LAKE}/*/*.parquet"

TS_FORMATS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"
V1_COLUMNS = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
     'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
     'payment_method':'VARCHAR','order_total':'VARCHAR',
     'items':'VARCHAR','meta':'VARCHAR'}"""


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{(ROOT / 'tmp').as_posix()}';")

# %% ── Task 1 — một file chứa mấy ngày? ──────────────────────────────
section("TASK 1 — 1 file, 8 ngày event time")

src = (ROOT / "raw" / "orders" / "orders_2026-06-10.csv").as_posix()
con.sql(f"""
    SELECT CAST(try_strptime(order_ts, {TS_FORMATS}) AS DATE) AS order_date,
           count(*) AS n
    FROM read_csv('{src}', header=true, columns={V1_COLUMNS})
    GROUP BY 1 ORDER BY 1
""").show()
# kỳ vọng: 06-03=41, ... 06-09=222, 06-10=59.857, None=14

# %% ── Task 1b — bỏ định dạng ISO-T xem sao ─────────────────────────
section("TASK 1b — thiếu 1 định dạng thì mất bao nhiêu?")

TWO_ONLY = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']"
con.sql(f"""
    SELECT count(*) FILTER (WHERE try_strptime(order_ts, {TS_FORMATS}) IS NULL) AS null_3fmt,
           count(*) FILTER (WHERE try_strptime(order_ts, {TWO_ONLY})   IS NULL) AS null_2fmt
    FROM read_csv('{src}', header=true, columns={V1_COLUMNS})
""").show()
# nhóm NULL phình ~10x: 0,3% dòng ISO-T là đơn hàng hợp lệ

# %% ── Task 2 — khám nghiệm sau khi chạy build_lake (mode ĐỎ) ───────
section("TASK 2 — hiện trường vụ mất dữ liệu")

con.sql(f"SELECT count(*) FROM read_parquet('{GLOB}', hive_partitioning=true)").show()
# kỳ vọng 59.024 thay vì 119.727 — mất trọn ngày 06-01

con.sql(f"""
    SELECT order_date, count(*) FROM read_parquet('{GLOB}', hive_partitioning=true)
    GROUP BY 1 ORDER BY 1
""").show()
# 06-01 chỉ còn 885 dòng: đó là các bản sửa trễ, đã đè lên 60.686 dòng gốc

# %% ── Task 4 — đọc cây thư mục ─────────────────────────────────────
section("TASK 4 — 3 điều bất thường")

con.sql(f"""
    SELECT order_date,
           count(DISTINCT filename)               AS files,
           count(*)                               AS rows,
           round(sum(length(items)) / 1e6, 1)     AS approx_mb
    FROM read_parquet('{GLOB}', hive_partitioning=true, filename=true)
    GROUP BY 1 ORDER BY 1
""").show()
# 46 thư mục (1 cái là __HIVE_DEFAULT_PARTITION__), phần lớn 8 file,
# 06-19 và 07-11 to gấp ~3 lần

# %% ── Task 5 — chứng minh pruning ──────────────────────────────────
section("TASK 5 — partition filter vs data-column filter")

query_a = f"""
    SELECT count(*) AS orders, round(avg(store_id), 2) AS avg_store
    FROM read_parquet('{GLOB}', hive_partitioning=true)
    WHERE order_date = DATE '2026-06-19'
"""
query_b = f"""
    SELECT count(*) AS orders, round(avg(store_id), 2) AS avg_store
    FROM read_parquet('{GLOB}', hive_partitioning=true)
    WHERE CAST(order_ts_clean AS DATE) = DATE '2026-06-19'
"""

print("-- A: lọc theo cột phân vùng --")
con.sql(query_a).show()
print("-- B: lọc theo cột dữ liệu --")
con.sql(query_b).show()
# hai kết quả PHẢI giống hệt: (180183, 267.17)

# %%
section("TASK 5 — EXPLAIN: bằng chứng")

print("═══ QUERY A ═══")
print(con.execute("EXPLAIN " + query_a).fetchall()[0][1])
print("\n═══ QUERY B ═══")
print(con.execute("EXPLAIN " + query_b).fetchall()[0][1])
# trong khối READ_PARQUET của A tìm 2 dòng:
#   File Filters: (order_date = '2026-06-19'::DATE)
#   Scanning Files: 8/377        <-- 369 file bị bỏ qua nhờ TÊN THƯ MỤC

# %%
section("TASK 5 — đo thời gian (3 lần, lấy trung vị)")

import statistics
import time

def bench(label: str, sql: str, runs: int = 3) -> None:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    print(f"{label:<22}{statistics.median(times):8.1f} ms   (runs: "
          f"{', '.join(f'{t:.0f}' for t in times)})")

bench("A partition filter", query_a)
bench("B data filter", query_b)
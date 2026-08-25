# %%
import os
if os.path.basename(os.getcwd()) == "work":
    os.chdir("..")
print(os.getcwd())      # phải in ra D:\ETL

# %% work/a01_explore.py
"""A01 — First contact: khảo sát raw feed, load 1 ngày ra Parquet.

Chạy:  python -i -m work.a01_explore
       (-i giữ phiên tương tác, `con` còn sống để gõ tiếp SQL)
"""
import json
import time

import duckdb

from lib.labpaths import raw_orders_dir, manifest_dir, tmp_dir

DAY = "2026-06-02"


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


# %% ── setup ───────────────────────────────────────────────────────────
con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")
tmp_dir("full").mkdir(parents=True, exist_ok=True)
con.execute(f"SET temp_directory='{tmp_dir('full').as_posix()}';")


# %% ── Task 2 — reconcile với manifest ─────────────────────────────────
section("TASK 2 — count vs manifest")

for scale in ("small", "full"):
    csv_path = raw_orders_dir(scale) / f"orders_{DAY}.csv"
    n = con.execute("SELECT count(*) FROM read_csv(?)", [str(csv_path)]).fetchone()[0]
    claimed = json.loads((manifest_dir(scale) / f"orders_{DAY}.json").read_text())
    print(f"{scale:5}: counted {n:,}  manifest {claimed['rows']:,}  "
          f"match={n == claimed['rows']}")


# %% ── Task 3 — DESCRIBE + SUMMARIZE ───────────────────────────────────
section("TASK 3 — schema mà auto-detect đoán ra")

small_csv = (raw_orders_dir("small") / f"orders_{DAY}.csv").as_posix()
con.sql(f"DESCRIBE SELECT * FROM read_csv('{small_csv}')").show()

print("\n-- customer_id dạng Excel-float --")
con.sql(f"""
    SELECT count(*) AS float_form
    FROM read_csv('{small_csv}', all_varchar=true)
    WHERE customer_id LIKE '%.0'
""").show()

print("\n-- SUMMARIZE --")
con.sql(f"SUMMARIZE SELECT * FROM read_csv('{small_csv}')").show()


# %% ── Task 5 — explicit schema ────────────────────────────────────────
section("TASK 5 — khai lược đồ tường minh")

V1_COLS = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
    'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
    'payment_method':'VARCHAR','order_total':'VARCHAR',
    'items':'VARCHAR','meta':'VARCHAR'}"""

for scale in ("small", "full"):
    path = (raw_orders_dir(scale) / f"orders_{DAY}.csv").as_posix()
    con.sql(f"""
        SELECT count(*)                                                    AS rows,
               count(*) FILTER (WHERE TRY_CAST(order_ts AS TIMESTAMP) IS NULL) AS dirty_ts,
               round(100.0 * count(*) FILTER (
                   WHERE TRY_CAST(order_ts AS TIMESTAMP) IS NULL) / count(*), 2) AS pct
        FROM read_csv('{path}', header=true, columns={V1_COLS})
    """).show()

print("\n-- 5 giá trị order_ts bẩn --")
full_csv = (raw_orders_dir("full") / f"orders_{DAY}.csv").as_posix()
con.sql(f"""
    SELECT order_ts FROM read_csv('{full_csv}', header=true, columns={V1_COLS})
    WHERE TRY_CAST(order_ts AS TIMESTAMP) IS NULL LIMIT 5
""").show()


# %% ── Task 6 — load ra Parquet, rồi đua ───────────────────────────────
section("TASK 6 — CSV vs Parquet")

pq = (tmp_dir("full") / f"orders_{DAY}.parquet").as_posix()
con.execute(f"""
    COPY (SELECT * FROM read_csv('{full_csv}', header=true, columns={V1_COLS}))
    TO '{pq}' (FORMAT parquet);
""")

csv_src = f"read_csv('{full_csv}', header=true, columns={V1_COLS})"
pq_src = f"read_parquet('{pq}')"


def bench(label: str, sql: str) -> None:
    """Chạy 2 lần, The first pays a one-time OS disk-cache cost — comparing a cold run to a warm one is a classic benchmarking sin."""
    for _ in range(2):
        t0 = time.perf_counter()
        result = con.execute(sql).fetchall()
        ms = (time.perf_counter() - t0) * 1000
    print(f"{label:<34}{ms:8.0f} ms   {result[0]}")


for name, src in (("CSV", csv_src), ("Parquet", pq_src)):
    bench(f"{name} count(*)",       f"SELECT count(*) FROM {src}")
    bench(f"{name} filter store 42", f"SELECT count(*) FROM {src} WHERE store_id = 42")
    bench(f"{name} group by status", f"SELECT status, count(*) FROM {src} GROUP BY 1 ORDER BY 2 DESC")


# %% ── Task 7 — nhìn vào metadata của Parquet ──────────────────────────
section("TASK 7 — row groups")

con.sql(f"""
    SELECT row_group_id, row_group_num_rows
    FROM parquet_metadata('{pq}') WHERE column_id = 0
""").show()

con.sql(f"""
    SELECT path_in_schema, stats_min_value, stats_max_value, total_compressed_size
    FROM parquet_metadata('{pq}')
    WHERE row_group_id = 0 AND path_in_schema IN ('order_id', 'status', 'items')
""").show()

print("\nXong. `con` vẫn sống — gõ tiếp SQL nếu muốn dò thêm.")
# %%

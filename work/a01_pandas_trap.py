# work/a01_pandas_trap.py
"""A01 Task 4 — đo RAM: pandas materialize vs DuckDB stream.

Chạy riêng, KHÔNG chạy trong notebook:  python -m work.a01_pandas_trap
Tiến trình phải sạch thì số đo mới sạch.
"""
import os
import time

import pandas as pd
import psutil

from lib.labpaths import raw_orders_dir

GB = 1024 ** 3
if psutil.virtual_memory().available < 6 * GB:
    raise SystemExit("Còn dưới 6 GB RAM trống — đóng bớt ứng dụng đã.")

csv_path = raw_orders_dir("full") / "orders_2026-06-02.csv"
proc = psutil.Process(os.getpid())

print(f"RSS before: {proc.memory_info().rss / GB:.2f} GB")
t0 = time.perf_counter()
df = pd.read_csv(csv_path)              # cả file -> RAM, không có đường vòng
print(f"loaded {len(df):,} rows in {time.perf_counter() - t0:.0f}s")
print(f"RSS after:  {proc.memory_info().rss / GB:.2f} GB")
print(f"DataFrame alone: {df.memory_usage(deep=True).sum() / GB:.2f} GB")

# ── đối chiếu: DuckDB streams ────────────────────────────────────────
import duckdb

con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")
t0 = time.perf_counter()
n = con.execute("SELECT count(*) FROM read_csv(?)", [str(csv_path)]).fetchone()[0]
print(f"\nDuckDB counted {n:,} rows in {time.perf_counter() - t0:.1f}s")
print(f"RSS now: {proc.memory_info().rss / GB:.2f} GB")
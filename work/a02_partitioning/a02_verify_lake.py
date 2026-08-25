# work/a02_partitioning/a02_verify_lake.py
"""Đối chiếu lake với manifest của bộ sinh dữ liệu.

Chạy:  python -m work.verify_lake
"""
import json
import os
from datetime import date, timedelta
from pathlib import Path

import duckdb

BASE = Path(os.environ.get("ETL_LAB_DATA", Path(__file__).resolve().parents[1] / "data"))
SCALE = "full"                          # Task 6 đổi thành "full"
ROOT = BASE / "small" if SCALE == "small" else BASE

manifest_total = 0
day, last = date(2026, 6, 1), date(2026, 7, 15)
while day <= last:
    m = json.loads((ROOT / "raw" / "manifest" / f"orders_{day}.json").read_text())
    manifest_total += m["rows"]
    day += timedelta(days=1)

con = duckdb.connect()
lake_glob = (ROOT / "lake" / "orders").as_posix() + "/*/*.parquet"
lake_total = con.execute(
    f"SELECT count(*) FROM read_parquet('{lake_glob}', hive_partitioning=true)"
).fetchone()[0]

print(f"manifests say: {manifest_total:,}")
print(f"lake contains: {lake_total:,}")
print("MATCH" if manifest_total == lake_total else "MISMATCH - investigate!")
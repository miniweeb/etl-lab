# work/A03_transformation/a03_bootstrap.py
"""Kết nối và đường dẫn dùng chung cho A03.

Chạy tương tác:  python -i -m work.a03_bootstrap
"""
import duckdb

from lib.labpaths import data_root, warehouse_path, ensure_dirs

SCALE = "small"                       # Task 10 đổi thành "full"
DAY = "2026-06-02"
ensure_dirs(SCALE)

ROOT = data_root(SCALE)
R = ROOT.as_posix()                   # forward slashes for SQL string literals

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute("SET memory_limit='8GB'; SET threads=8; SET TimeZone='UTC';")
con.execute(f"SET temp_directory='{R}/tmp';")


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")
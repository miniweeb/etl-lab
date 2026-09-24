"""A14 bootstrap — path, hằng số, và một connection đã đặt sẵn.

Cùng luật A12/A13: file này KHÔNG mở warehouse.duckdb. Hôm nay mọi thứ chạy
trên connection in-memory đọc lake — không có writer nào cả, nên không có gì
để tranh.

Import: from work.a14_performance.a14_bootstrap import SCALE, LAKE, con, ...
"""
from __future__ import annotations

import os
import sys

from lib.labpaths import lake_dir, manifest_dir, raw_dims_dir, tmp_dir

# ── SCALE module-level, như A12/A13 ─────────────────────────────────────────
# A14 lật scale liên tục: Task 1,3,4 ở small; Task 2,5,6,7 làm cả hai. Notebook
# đặt os.environ trước khi import file này (xem cell 1 của a14_explore).
SCALE = os.environ.get("ETL_LAB_SCALE", "small")

LAKE = (lake_dir(SCALE) / "orders_v=*").as_posix()
#   Glob trên tên thư mục có timestamp của A10, nên không phải sửa khi rebuild.
#   Hệ quả: hive_partitioning=true sẽ lòi ra thêm cột orders_v. Vô hại hôm nay,
#   nhưng nhớ nó khi đếm cột.
DIMS = raw_dims_dir(SCALE)
TMP = tmp_dir(SCALE)
MANIFEST = manifest_dir(SCALE)

NORMAL_DAY = "2026-06-05"
SPIKE_DAY = "2026-06-19"       # ~3x ngày thường, ngày A12 dùng cho straggler

SIX = "order_id, customer_id, store_id, order_ts, status, order_total"
#   Sáu cột hợp đồng của đội finance. Danh sách này xuất hiện ở Task 4 fix 4 và
#   Task 6 — để một chỗ, khỏi lệch nhau.


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def duck(memory_limit: str = "8GB", threads: int = 8):
    """Connection in-memory, ba SET của lab (SPEC §4).

    Không nhận preserve_insertion_order=false như A13: hôm nay ta ĐO, và một
    tham số bật ngầm sẽ lẫn vào mọi phép so sánh. Task 6 ghi file lớn thì bật
    tại chỗ, có ghi chú.
    """
    import duckdb
    TMP.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'; SET threads={threads}; "
                f"SET temp_directory='{TMP.as_posix()}';")
    return con


def fix_console() -> None:
    """Plan của DuckDB in bằng ký tự khung; console Windows mặc định cp1252.

    Không phải bug DuckDB. Gọi hàm này trước khi print EXPLAIN.
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def manifest_rows(day: str) -> int:
    import json
    return json.loads((MANIFEST / f"orders_{day}.json").read_text())["rows"]


if __name__ == "__main__":
    section(f"A14 bootstrap  [scale={SCALE}]")
    print("lake :", LAKE)
    print("dims :", DIMS, "->", len(list(DIMS.glob("*.csv"))), "file csv")
    print("tmp  :", TMP)
    print()
    for d in (NORMAL_DAY, SPIKE_DAY):
        print(f"manifest {d}: {manifest_rows(d):>10,} dòng")
    c = duck()
    n = c.execute(f"SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet', "
                  f"hive_partitioning=true)").fetchone()[0]
    files = c.execute(f"SELECT count(DISTINCT filename) FROM read_parquet("
                      f"'{LAKE}/*/*.parquet', filename=true)").fetchone()[0]
    c.close()
    print(f"\nlake: {n:,} dòng trong {files} file parquet")
    print("Con số file đó là chữ N trong 'Scanning Files: 1/N' của Task 2.")
    print("Nó KHÔNG phải 69 — các lần APPEND theo ngày của A10 để lại nhiều")
    print("file trong một phân vùng. Ghi nó lại, Task 2 cần đối chiếu.")
"""A12 Task 4 — committer: process DUY NHẤT được mở warehouse.duckdb.

  python -m work.a12_parallelization.a12_commit_stage

Chạy SAU khi pool xong, và chạy MỘT MÌNH. Đó là lý do nó lấy trọn ngân sách
8GB/8-thread: một job song song có nhiều PHA, và mỗi pha được ăn cả máy. Chia
4GB cho committer chỉ vì "worker cũng cần" là chia cho những process đã chết.

Dedupe: chiến lược (a) của A08 — updated_at mới nhất thắng, theo order_id.
"""
from __future__ import annotations

import argparse
import time

import duckdb

from lib.labpaths import data_root
from work.a12_parallelization.a12_bootstrap import SCALE, STAGE, TMP, section


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="core.orders_par")
    args = ap.parse_args()

    wh = data_root(SCALE) / "warehouse" / "warehouse.duckdb"
    section(f"commit [{SCALE}] -> {args.table}")
    print("Nếu dòng dưới ném IOException: còn kernel VS Code hoặc terminal nào "
          "đang giữ warehouse. Đóng nó. Đó chính là bài học Task 2.")
    con = duckdb.connect(str(wh))          # MỘT writer. Chỉ một.
    con.execute("SET memory_limit='8GB'; SET threads=8; "
                f"SET temp_directory='{TMP.as_posix()}';")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")

    src = f"read_parquet('{STAGE.as_posix()}/*.parquet')"
    t0 = time.perf_counter()
    con.execute(f"""
        CREATE OR REPLACE TABLE {args.table} AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC) AS rn
            FROM {src}
        ) WHERE rn = 1""")
    # PARTITION BY order_id — cột VARCHAR 'ORD-...' mà A10 chuẩn hoá, KHÔNG phải
    # order_id_num. Đây là chỗ A10 trả lãi: correction ở era v3 mang id dạng
    # 'ORD-0000123456' phải rơi cùng nhóm với bản gốc v1 mang id 123456. Nếu
    # dedupe ra con số CAO hơn 4.054.002, đó là dấu hiệu hai era không khớp id.
    commit_wall = time.perf_counter() - t0

    staged, final = con.execute(f"""
        SELECT (SELECT count(*) FROM {src}),
               (SELECT count(*) FROM {args.table})""").fetchone()
    deliveries, unstamped = con.execute(f"""
        SELECT count(DISTINCT _data_date),
               count(*) FILTER (_data_date IS NULL) FROM {args.table}""").fetchone()

    print(f"staged rows      : {staged:,}      (expect small 4,116,024)")
    print(f"{args.table:<17}: {final:,}      (expect small 4,053,208)")
    print(f"_data_date distinct / NULL: {deliveries} / {unstamped}   (expect 69 / 0)")
    print(f"commit wall      : {commit_wall:.1f}s   <- cộng vào stage wall cho Task 9")
    # 4.054.002 = manifest rows - late_rows - dup_rows. orders_par thấp hơn 794:
    # với 794 order, bản duy nhất của nó lại chính là dòng timestamp hỏng đã bị
    # CANON vứt. Mất hẳn order, không phải mất một bản sao. ĐẾM, đừng ép cho bằng.
    con.close()


if __name__ == "__main__":
    main()
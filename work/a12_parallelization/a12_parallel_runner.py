"""A12 Task 3 — stage nhiều ngày song song. Worker CHỈ ghi parquet.

  python -m work.a12_parallelization.a12_parallel_runner --workers 4 --threads 2 --mem 3GB
  python -m work.a12_parallelization.a12_parallel_runner --workers 8 --threads 1 --mem 1500MB --order size
  $env:ETL_LAB_SCALE="full"; python -m work.a12_parallelization.a12_parallel_runner ...

LUẬT BẤT DI BẤT DỊCH: không dòng nào trong file này được mở warehouse.duckdb.
Một file DuckDB chỉ cho MỘT process ghi — và cả read_only cũng hỏng khi có
writer đang giữ. Worker viết parquet; commit_stage.py viết DB.

Hai ràng buộc của Windows spawn (process con re-import file này):
  1. code khởi động pool nằm dưới `if __name__ == "__main__":`
  2. load_day() phải ở TOP-LEVEL (con tìm nó theo tên, không nhận closure)
"""
from __future__ import annotations

import argparse
import time
from multiprocessing import Pool

import duckdb

from work.a12_parallelization.a12_bootstrap import (
    ALL_END, ALL_START, MANIFEST, PeakRSS, RAW, SCALE, STAGE, TMP,
    biggest_first, daterange, legal, manifest, section)
from work.a12_parallelization.a12_stage_sql import era_from_manifest, stage_sql


def load_day(task):
    """CHẠY TRONG PROCESS CON. Trả tuple thuần — phải pickle được.

    duckdb.connect() không tham số = DB in-memory riêng của worker này. Không
    file, không khoá, không ai giành. Crash ở đây chỉ giết một ngày.
    """
    day, cfg = task
    try:
        era = era_from_manifest(day, manifest(day))
        con = duckdb.connect()
        con.execute(f"SET threads={cfg['threads']}; "
                    f"SET memory_limit='{cfg['mem']}'; "
                    f"SET enable_progress_bar=false; "
                    f"SET temp_directory='{TMP.as_posix()}';")
        # enable_progress_bar=false: DuckDB tự vẽ thanh tiến trình khi query
        # chạy >2s và stdout là terminal. Với N worker, N thanh cùng ghi vào
        # MỘT stdout — dòng chèn lẫn nhau, và con số elapsed cạnh một ngày lại
        # thuộc về worker khác. Log song song phải do MỘT process in.
        # memory_limit BẮT BUỘC phải đặt: mặc định là ~80% RAM MÁY, cho MỖI
        # process. 8 worker mặc định = tự hứa ~100GB trên máy 16GB.
        t0 = time.perf_counter()
        con.execute(stage_sql(day, RAW / f"orders_{day}.csv",
                              STAGE / f"orders_{day}.parquet", era))
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet("
            f"'{(STAGE / f'orders_{day}.parquet').as_posix()}')").fetchone()[0]
        con.close()
        return day, era, "OK", time.perf_counter() - t0, rows, ""
    except Exception as e:
        # continue-on-error như A11: một ngày hỏng không được giết cả run.
        return day, "?", "FAIL", 0.0, 0, f"{type(e).__name__}: {e}"[:200]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=ALL_START)
    ap.add_argument("--end", default=ALL_END)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--threads", type=int, default=2, help="mỗi worker")
    ap.add_argument("--mem", default="3GB", help="mỗi worker")
    ap.add_argument("--order", choices=("date", "size"), default="date")
    ap.add_argument("--clean", action="store_true", help="xoá stage cũ trước")
    args = ap.parse_args()

    mem_gb = (float(args.mem[:-2]) if args.mem.upper().endswith("GB")
              else float(args.mem[:-2]) / 1024)
    ok, why = legal(args.workers, args.threads, mem_gb)
    section(f"stage [{SCALE}]  {args.workers}x{args.threads}  mem/worker={args.mem}"
            f"  order={args.order}")
    print(f"budget: {'LEGAL' if ok else 'ILLEGAL — ' + why}")
    # In ra chứ KHÔNG chặn: Task 6 cố tình chạy cấu hình bất hợp lệ để nhìn máy
    # cư xử thế nào. Một cái gate ở đây sẽ giết mất bài học.

    STAGE.mkdir(parents=True, exist_ok=True)
    if args.clean:
        for f in STAGE.glob("orders_*.parquet"):
            f.unlink()

    days = list(daterange(args.start, args.end))
    if args.order == "size":
        days = biggest_first(days)
    tasks = [(d, {"threads": args.threads, "mem": args.mem}) for d in days]

    failed, staged = [], 0
    with PeakRSS() as rss:
        t0 = time.perf_counter()
        with Pool(processes=args.workers) as pool:
            for day, era, status, secs, rows, err in pool.imap_unordered(load_day, tasks):
                # imap_unordered: kết quả về theo thứ tự HOÀN THÀNH, không theo
                # thứ tự ngày. Đó là dấu hiệu pool đang thật sự chạy song song.
                print(f"  {day}  {era:3}  {status:4}  {secs:6.1f}s  {rows:>9,}  {err}")
                staged += rows
                if status == "FAIL":
                    failed.append(day)
        wall = time.perf_counter() - t0

    print(f"\nwall {wall:.1f}s | peak RSS {rss.peak_gb:.1f} GB | "
          f"staged {staged:,} rows | failed: {failed or 'none'}")
    # Chỉ cộng manifest của ĐÚNG những ngày vừa chạy. Cộng cả 69 trong khi mới
    # stage 45 thì "dropped" hoá ra là phần chưa chạy — một phép trừ trông như
    # chẩn đoán nhưng thật ra là so hai tập khác nhau.
    claimed = duckdb.connect().execute(
        f"SELECT sum(rows) FROM read_json_auto("
        f"'{(MANIFEST / 'orders_*.json').as_posix()}') "
        f"WHERE file BETWEEN 'orders_{args.start}.csv' AND 'orders_{args.end}.csv'"
    ).fetchone()[0]
    print(f"manifest ({len(days)} ngày) {claimed:,}  ->  dropped {claimed - staged:,}")
    # Expect small, cả 69 ngày: staged 4,116,024 | manifest 4,116,840 | dropped 816
    # Expect small, month 1   : staged 2,773,013 | dropped 553 (khớp A10)
    print("Ghi journal: workers/threads/mem/order + wall + peak RSS. "
          "Chạy lần 2 và ghi cả hai — lần 1 trả giá cold disk cache.")


if __name__ == "__main__":     # BẮT BUỘC trên Windows. Bỏ đi = bom fork.
    main()
"""work/runner.py — backfill runner: phát lại pipeline một-ngày trên một dải ngày.

    python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08 --dry-run
    python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08
    python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08 --only-failed
    python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08 --force

Backfill KHÔNG phải một script riêng. Nó là pipeline hằng ngày, chạy lại trên
nhiều ngày quá khứ. Viết một "bản backfill" riêng của cùng logic là tự tạo ra
hai pipeline rồi để chúng trôi xa nhau. A07 đã làm phần khó — mỗi ngày nạp lại
được bao nhiêu lần cũng ra một kết quả. Hôm nay chỉ thêm một vòng lặp mỏng.

Bốn việc của một runner tử tế:
  1. Lập kế hoạch trước khi động tay  -> --dry-run
  2. Nhớ đã làm gì                    -> đọc ops.etl_runs, crash ở ngày 40 không
                                         bắt chạy lại từ ngày 1
  3. Gặp lỗi thì đi tiếp              -> 69 ngày là 69 việc độc lập, nhưng phải
                                         ồn ào ở bản tổng kết cuối
  4. Ước lượng chi phí                -> rows/giây x số dòng còn lại
"""
from __future__ import annotations

import argparse
import datetime as dt
import time

import duckdb

from lib.labpaths import tmp_dir, warehouse_path
from work.pipeline import load_day
from work.preflight import run_checks

STEP = "day"        # tên step ở tầng runner trong ops.etl_runs


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]


def latest_status(con, d: dt.date) -> str | None:
    row = con.execute("""
        SELECT status FROM ops.etl_runs
        WHERE run_date = ? AND step = ?
        ORDER BY started_at DESC LIMIT 1
    """, [d, STEP]).fetchone()
    return row[0] if row else None


def build_plan(con, dates, only_failed: bool, force: bool):
    """Ba chế độ, và chúng khác nhau thật sự:

      mặc định      bỏ qua ngày đã xanh, chạy mọi thứ còn lại  -> resume
      --only-failed chỉ chạy lại ngày có lần cuối 'failed'
      --force       chạy lại tất cả, kể cả ngày xanh           -> restatement

    Một trạng thái cần biết trước khi gặp: Ctrl-C giữa chừng để lại dòng ledger
    ở 'running'. Resume mặc định nhặt nó lên (mọi thứ không xanh đều chạy lại),
    nhưng --only-failed thì KHÔNG, vì nó lọc đúng cái tên nó nói.
    """
    plan = []                             # (date, "run"|"skip", reason)
    for d in dates:
        st = latest_status(con, d)
        if force:
            plan.append((d, "run", f"--force (trạng thái cuối: {st})"))
        elif only_failed:
            if st == "failed":
                plan.append((d, "run", "lần cuối failed"))
            else:
                plan.append((d, "skip", f"trạng thái cuối là {st}, không phải failed"))
        elif st == "success":
            plan.append((d, "skip", "đã xanh"))
        else:
            plan.append((d, "run", "chưa chạy bao giờ" if st is None
                         else f"trạng thái cuối: {st}"))
    return plan


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill một dải ngày, mỗi lần một ngày.")
    p.add_argument("--scale", choices=["small", "full"], required=True)
    p.add_argument("--start", required=True, type=dt.date.fromisoformat)
    p.add_argument("--end", required=True, type=dt.date.fromisoformat)
    p.add_argument("--only-failed", action="store_true",
                   help="chỉ chạy lại ngày có lần cuối failed")
    p.add_argument("--force", action="store_true",
                   help="chạy lại mọi ngày, kể cả ngày xanh (restatement)")
    p.add_argument("--dry-run", action="store_true",
                   help="in kế hoạch, không đụng gì")
    args = p.parse_args()

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute(f"""SET memory_limit='8GB'; SET threads=8;
                    SET temp_directory='{tmp_dir(args.scale).as_posix()}';""")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ops.etl_runs (
            run_id BIGINT PRIMARY KEY, run_date DATE, step VARCHAR, status VARCHAR,
            started_at TIMESTAMP, finished_at TIMESTAMP,
            rows BIGINT, source_file VARCHAR, error VARCHAR)""")

    plan = build_plan(con, date_range(args.start, args.end),
                      args.only_failed, args.force)
    for d, action, reason in plan:
        print(f"{d}  {action:4}  {reason}")
    to_run = [d for d, action, _ in plan if action == "run"]
    print(f"\n{len(to_run)} trên {len(plan)} ngày được chọn.")
    if args.dry_run or not to_run:
        con.close()
        return

    t0 = time.time()
    for d in to_run:
        run_id = con.execute("SELECT nextval('ops.run_id_seq')").fetchone()[0]
        con.execute("""INSERT INTO ops.etl_runs
                       VALUES (?, ?, ?, 'running', now(), NULL, NULL, ?, NULL)""",
                    [run_id, d, STEP, f"orders_{d}.csv"])
        #   source_file ghi ở INSERT, không phải UPDATE: một ngày thất bại vẫn
        #   phải nói được nó nghẹn ở file nào. Và nó sống Ở ĐÂY chứ không phải
        #   trên 1,2 triệu dòng dữ liệu (§7.3).
        day_t0 = time.time()
        try:
            if not run_checks(args.scale, d):
                raise RuntimeError("preflight đỏ - sửa dòng FAIL ở trên "
                                   "rồi chạy --only-failed")
            #   Preflight chạy trước MỖI ngày, không phải một lần lúc khởi
            #   động: mỗi ngày mang file nguồn và manifest riêng, còn ổ đĩa đầy
            #   và khoá file thì xuất hiện giữa chừng chứ không phải trước đó.
            n = load_day(con, d, scale=args.scale, run_id=run_id)
            con.execute("""UPDATE ops.etl_runs SET status='success',
                           finished_at=now(), rows=? WHERE run_id = ?""",
                        [n, run_id])
            print(f"{d}  OK    {n:>12,} dòng   {time.time() - day_t0:6.1f} s")
        except Exception as e:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            #   load_day có thể chết bên trong BEGIN của nó. Một transaction
            #   chết sẽ giết luôn lệnh UPDATE ledger ngay dưới đây — luật
            #   đường-lỗi của A07.
            con.execute("""UPDATE ops.etl_runs SET status='failed',
                           finished_at=now(), error=? WHERE run_id = ?""",
                        [str(e)[:500], run_id])
            print(f"{d}  FAIL  {type(e).__name__}: {str(e)[:120]}")

    print(f"\nWall clock: {time.time() - t0:,.1f} s")
    print(con.execute("""
        WITH latest AS (
            SELECT run_date, status, rows,
                   row_number() OVER (PARTITION BY run_date
                                      ORDER BY started_at DESC) AS rn
            FROM ops.etl_runs
            WHERE step = ? AND run_date BETWEEN ? AND ?)
        SELECT status, count(*) AS days, sum(rows) AS rows_loaded
        FROM latest WHERE rn = 1
        GROUP BY status ORDER BY status
    """, [STEP, args.start, args.end]).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
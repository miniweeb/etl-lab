"""A10 Task 7 — dựng lại toàn bộ lake theo canonical schema.

  python -m work.a10_schema_evolution.a10_rebuild_lake --scale small
  python -m work.a10_schema_evolution.a10_rebuild_lake --scale full

Điều kiện tiên quyết: đã chạy a10_restore_0619 cho scale tương ứng, nếu không
phép cộng ở (d) sẽ lệch 889 (small) / 17.853 (full) và bạn sẽ truy nhầm chỗ.

Kế hoạch: migrate month 1 từ PARQUET lake đã có (không parse lại 45 CSV) ->
nạp month 2 từ raw CSV theo từng ngày -> tất cả vào MỘT thư mục versioned mới
-> verify row identity -> mới đổi view cho reader (pattern A09).
"""
from __future__ import annotations

import argparse
import time

from work.a10_schema_evolution.a10_bootstrap import (
    boot, section, daterange, RunLog, BAD_DAY, LAKE_VER,
    MONTH2_START, MONTH2_END)
from work.a10_schema_evolution import a10_canonical as C


def copy_aligned(con, dest: str) -> None:
    """APPEND, KHÔNG phải OVERWRITE_OR_IGNORE.

    Mỗi lần COPY đặt tên file lại từ data_0.parquet. Late row khiến ngày D ghi
    vào các partition D-7..D — đúng những thư mục ngày trước đã ghi. Với
    OVERWRITE_OR_IGNORE, data_0.parquet của ngày D ĐÈ LÊN file của ngày trước
    trong partition dùng chung, im lặng. APPEND sinh tên file duy nhất.
    Sau khi chạy xong, mở order_date=2026-07-14/ ra nhìn: nhiều file parquet,
    mỗi writer một file — đó mới là đúng.
    """
    con.execute(f"""COPY ({C.CANON}) TO '{dest}'
      (FORMAT parquet, PARTITION_BY (order_date), APPEND, ROW_GROUP_SIZE 122880)""")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="small", choices=("small", "full"))
    args = ap.parse_args()

    con, ROOT, RAW, MANIFEST = boot(args.scale)
    dest = (ROOT / "lake" / LAKE_VER).as_posix()
    log = RunLog(con)
    t0 = time.perf_counter()

    # ── (a) migrate month 1 từ lake cũ ──────────────────────────────────────
    section(f"(a) migrate month 1  [{args.scale}]")
    old_lake = ROOT / "lake" / "orders"
    rid = log.start("2026-06-01", "lake/orders/*.parquet")
    try:
        C.open_lake_v1(con, old_lake)          # lake giữ order_ts gốc -> ALIGN['v1'] chạy thẳng
        dropped = con.execute(C.DROPPED_SQL).fetchone()[0]
        kept = con.execute(f"SELECT count(*) FROM ({C.CANON})").fetchone()[0]
        copy_aligned(con, dest)
        log.ok(rid, kept, dropped)
        print(f"month 1: kept {kept:,} dropped {dropped:,} "
              f"({time.perf_counter() - t0:.0f}s)")
    except Exception as e:
        log.fail(rid, "2026-06-01", str(e))
        raise

    # ── (b)+(c) nạp month 2 từng ngày ───────────────────────────────────────
    section("(b) load month 2 day by day")
    for day in daterange(MONTH2_START, MONTH2_END):
        rid = log.start(day, f"orders_{day}.csv")
        try:
            C.open_day(con, day, RAW)
            dropped = con.execute(C.DROPPED_SQL).fetchone()[0]
            kept = con.execute(f"SELECT count(*) FROM ({C.CANON})").fetchone()[0]
            copy_aligned(con, dest)
            log.ok(rid, kept, dropped)
            print(f"{day}  success   kept {kept:>7,}  dropped {dropped:>4,}")
        except Exception as e:                       # (c) BAD_DAY rơi vào đây
            msg = str(e).splitlines()[0]
            log.fail(rid, day, msg)
            print(f"{day}  FAILED    {msg[:90]}")

    print(f"\ndropped_total = {log.dropped_total:,}   <- vế 'dropped' của (d)")
    print(f"failed        = {log.failed}")
    print(f"{BAD_DAY}: ghi failed và đi tiếp. KHÔNG chẩn đoán hôm nay — đó là A11.")

    # ── (d) verify row identity TRƯỚC khi publish ───────────────────────────
    section("(d) verify")
    claimed = con.execute(f"""
        SELECT sum(rows) FROM read_json_auto(
          '{(MANIFEST / "orders_*.json").as_posix()}')
        WHERE file != 'orders_{BAD_DAY}.csv'""").fetchone()[0]
    got, parts = con.execute(f"""
        SELECT count(*), count(DISTINCT order_date)
        FROM read_parquet('{dest}/*/*.parquet', hive_partitioning=true)""").fetchone()
    closes = got + log.dropped_total == claimed
    print(f"manifest excl. {BAD_DAY} : {claimed:,}")
    print(f"lake rows                : {got:,}")
    print(f"dropped                  : {log.dropped_total:,}")
    print(f"identity closes          : {closes}")
    print(f"distinct order_date      : {parts}")
    # Expect small: 4,058,440 | 4,057,637 | 803 | True | 69
    # 69 partition dù chỉ 68 file nạp được — giải thích vì sao vào journal.
    # (Gợi ý: ngày 07-23..07-29 giao late correction VÀO partition 07-22.)

    if not closes:
        print("\nDỪNG. Không publish view khi phép cộng chưa đóng. "
              "Kiểm: đã chạy a10_restore_0619 chưa? dropped của month 1 tính chưa?")
        con.close()
        return

    # ── (e) đổi view cho reader ─────────────────────────────────────────────
    section("(e) create core.lake_orders")
    # A09 dọn dẹp đã bỏ view này, nên đây là CREATE chứ không phải repoint —
    # nhưng vẫn là cùng một nước đi catalog của A09: atomic với reader warehouse.
    con.execute(f"""CREATE OR REPLACE VIEW core.lake_orders AS
      SELECT * EXCLUDE (orders_v)
      FROM read_parquet('{dest}/*/*.parquet', hive_partitioning=true)""")
    # EXCLUDE (orders_v): tên thư mục orders_v=... trông như một Hive partition
    # nên hive_partitioning=true bịa ra cột orders_v. Vô hại, nhưng giữ shape sạch.
    con.sql("SELECT count(*), count(DISTINCT order_date) FROM core.lake_orders").show()
    print("Xong. Giờ mới dời lake/orders cũ sang .trash (làm tay, sau khi xem lại).")
    print(f"wall-clock: {time.perf_counter() - t0:.0f}s   <- ghi journal, A12 sẽ so")
    con.close()


if __name__ == "__main__":
    main()
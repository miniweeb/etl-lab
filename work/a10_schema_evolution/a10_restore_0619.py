"""A10 Task 0 (tiền đề) — dựng lại partition order_date=2026-06-19 của lake small/full.

VÌ SAO: A09 dedupe đã lấy 889 dòng (small) / 17.853 dòng (full) ra khỏi partition
06-19. Đó là kết quả đúng CỦA A09, nhưng Task 7d của A10 đối soát lake với
manifest thô — manifest không biết gì về dedupe. Nếu migrate month 1 từ lake
hiện tại, identity ra 4.056.748 thay vì 4.057.637 đề cho, và bạn sẽ đi truy một
sai lệch do chính mình tạo ra ở bài trước.

Cách xử lý: trả partition đó về đúng trạng thái A02 để lại (có dup), rồi mới
bắt đầu A10.

  python -m work.a10_schema_evolution.a10_restore_0619 --scale small --dry-run
  python -m work.a10_schema_evolution.a10_restore_0619 --scale small

LƯU Ý: partition 06-19 nhận dòng từ TÁM file (06-19..06-26) vì late window
D+7 — dựng lại chỉ từ orders_2026-06-19.csv sẽ thiếu phần late.
"""
from __future__ import annotations

import argparse
import shutil

from work.a10_schema_evolution.a10_bootstrap import boot, section, daterange
from work.a10_schema_evolution.a10_canonical import V1_COLS

TARGET = "2026-06-19"
SRC_DAYS = list(daterange("2026-06-19", "2026-06-26"))   # D .. D+7

# ĐÃ ĐỐI CHIẾU với DESCRIBE của partition thật: 12 cột, order_ts_clean nằm SAU
# meta (không phải cạnh order_ts), order_date là cột cuối và được lưu THẬT TRONG
# FILE (không chỉ ở tên thư mục).
TS = ("try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',"
      "'%d/%m/%Y %H:%M:%S','%Y-%m-%dT%H:%M:%S'])")

PROJECTION = f"""
SELECT
    order_id, customer_id, store_id,
    order_ts, updated_at, status, payment_method, order_total, items, meta,
    {TS}                AS order_ts_clean,
    CAST({TS} AS DATE)  AS order_date
FROM src
WHERE CAST({TS} AS DATE) = DATE '{TARGET}'
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="small", choices=("small", "full"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con, ROOT, RAW, MANIFEST = boot(args.scale)
    lake = ROOT / "lake" / "orders"
    part = lake / f"order_date={TARGET}"

    # ── 1. chụp ảnh trước ───────────────────────────────────────────────────
    section(f"BEFORE  [{args.scale}]")
    before_part = con.execute(
        f"SELECT count(*) FROM read_parquet('{part.as_posix()}/*.parquet')"
    ).fetchone()[0]
    before_all = con.execute(
        f"SELECT count(*) FROM read_parquet('{lake.as_posix()}/*/*.parquet', "
        f"hive_partitioning=true)").fetchone()[0]
    print(f"partition {TARGET}: {before_part:,}")
    print(f"lake total       : {before_all:,}")

    con.sql(f"DESCRIBE SELECT * FROM read_parquet('{part.as_posix()}/*.parquet')").show()
    # ^ so cột này với PROJECTION ở trên. Phải khớp tên VÀ thứ tự.

    # ── 2. dựng bản mới trong bộ nhớ, đối chiếu TRƯỚC khi xoá gì ────────────
    section("REBUILD (chưa ghi)")
    con.execute("DROP TABLE IF EXISTS staging.a10_restore_0619")
    first = True
    for day in SRC_DAYS:
        f = (RAW / f"orders_{day}.csv").as_posix()
        con.execute(f"CREATE OR REPLACE VIEW src AS "
                    f"SELECT * FROM read_csv('{f}', header=true, columns={V1_COLS})")
        verb = ("CREATE TABLE staging.a10_restore_0619 AS" if first
                else "INSERT INTO staging.a10_restore_0619")
        con.execute(f"{verb} {PROJECTION}")
        n = con.execute("SELECT count(*) FROM staging.a10_restore_0619").fetchone()[0]
        print(f"  {day} -> tổng dồn {n:,}")
        first = False

    after_part = con.execute(
        "SELECT count(*) FROM staging.a10_restore_0619").fetchone()[0]
    delta = after_part - before_part
    print(f"\npartition mới: {after_part:,}   (chênh +{delta:,})")
    print(f"lake sau khi ghi sẽ là: {before_all + delta:,}")
    # Expect small: chênh +889  -> month 1 về đúng 2.773.566
    # Expect full : chênh +17.853

    if args.dry_run:
        print("\n--dry-run: không ghi gì. Kiểm hai con số trên rồi chạy lại không có cờ.")
        con.close()
        return

    # ── 3. ghi đè partition ─────────────────────────────────────────────────
    section("WRITE")
    trash = ROOT / ".trash" / f"a09_dedup_order_date={TARGET}"
    trash.parent.mkdir(parents=True, exist_ok=True)
    if trash.exists():
        shutil.rmtree(trash)
    shutil.move(str(part), str(trash))     # KHÔNG xoá — dời sang .trash (luật A09)
    print(f"partition cũ -> {trash}")

    part.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM staging.a10_restore_0619)
      TO '{(part / "data_0.parquet").as_posix()}'
      (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
    # GIỮ cột order_date trong file: DESCRIBE cho thấy lake A02 lưu nó THẬT
    # bên trong parquet, không chỉ ở tên thư mục. Nếu EXCLUDE, partition này
    # sẽ lệch schema với 44 partition còn lại và read_parquet('*/*') sẽ hỏng.

    # ── 4. verify ───────────────────────────────────────────────────────────
    section("AFTER")
    now_all = con.execute(
        f"SELECT count(*) FROM read_parquet('{lake.as_posix()}/*/*.parquet', "
        f"hive_partitioning=true)").fetchone()[0]
    claimed = con.execute(f"""
        SELECT sum(rows) FROM read_json_auto(
          '{(MANIFEST / "orders_*.json").as_posix()}') WHERE era = 'v1'""").fetchone()[0]
    print(f"lake total : {now_all:,}")
    print(f"manifest v1: {claimed:,}")
    print(f"khớp       : {now_all == claimed}")
    # Nếu False: phần chênh còn lại là các dòng order_ts không parse được mà A02
    # đã bỏ. Đếm chúng, đừng ép cho bằng — chúng thuộc vế "dropped" của identity.
    con.execute("DROP TABLE staging.a10_restore_0619")
    con.close()


if __name__ == "__main__":
    main()
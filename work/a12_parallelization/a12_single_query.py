"""A12 Task 7 — cách kia: MỘT process, MỘT query, cả danh sách file.

  python -m work.a12_parallelization.a12_single_query --era v1

Pool không phải dạng song song duy nhất. DuckDB tự chia một query trên nhiều
file ra 8 core. Đưa nó cả list file và tránh hoàn toàn overhead process.

Một query = một schema = MỘT ERA. Cả 69 ngày là ba lần chạy, ba danh sách —
lại là V1/V2/V3_COLS của A10. Đó không phải hạn chế của kỹ thuật này, đó là
sự thật về dữ liệu mà pool che giấu bằng cách xử từng ngày một.
"""
from __future__ import annotations

import argparse
import time

import duckdb

from work.a12_parallelization.a12_bootstrap import (
    BAD_DAY, RAW, SCALE, TMP, daterange, section)
from work.a12_parallelization.a12_stage_sql import COLS
from work.a10_schema_evolution.a10_canonical import ALIGN, CANON

ERA_RANGE = {"v1": ("2026-06-01", "2026-07-15"),
             "v2": ("2026-07-16", "2026-07-31"),
             "v3": ("2026-08-01", "2026-08-08")}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--era", choices=("v1", "v2", "v3"), default="v1")
    ap.add_argument("--relax", action="store_true",
                    help="bật store_rejects cho CẢ era để nuốt file gãy")
    args = ap.parse_args()

    start, end = ERA_RANGE[args.era]
    files = [RAW / f"orders_{d}.csv" for d in daterange(start, end)]
    file_list = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    out = TMP / f"lake_single_query_{args.era}"

    section(f"single query [{SCALE}] era={args.era}  {len(files)} files")
    con = duckdb.connect()                  # in-memory: vẫn không đụng warehouse
    con.execute("SET threads=8; SET memory_limit='8GB'; "
                f"SET temp_directory='{TMP.as_posix()}';")

    # ── _data_date: từ hằng số một ngày -> cắt ra từ TÊN FILE ───────────────
    # Pool có _data_date miễn phí: mỗi worker biết nó đang làm ngày nào, nên
    # DATE '{day}' là hằng số. Ở đây một query nuốt 45 file, không còn "ngày
    # nào" nào cả — thông tin đó chỉ còn nằm ở tên file. filename=true bảo
    # DuckDB thêm cột đường dẫn vào mỗi dòng.
    #
    # Lát cắt [-14:-5] trên '.../orders_2026-06-01.csv': DuckDB đánh chỉ số
    # chuỗi từ 1, chỉ số âm đếm ngược từ cuối, và CẢ HAI ĐẦU đều lấy (khác
    # Python). '.csv' là 4 ký tự cuối -> ngày kết thúc ở -5, dài 10 -> bắt đầu
    # ở -14. Regex cũng làm được, nhưng {4} của nó là dấu định dạng f-string
    # Python và bạn sẽ mất 20 phút cho một KeyError.
    # ── BAD_DAY: giới hạn thật của "một query cho cả era" ───────────────────
    # 07-22 nằm trong era v2 và có 151 dòng gãy (A11). Pool nuốt được vì mỗi
    # worker mở MỘT file, nên bật store_rejects=true cho riêng ngày đó. Ở đây
    # một read_csv ăn cả 16 file, và tham số của nó áp cho TOÀN BỘ danh sách.
    # Không có chỗ nào để nói "riêng file này đọc khác".
    #
    # Nên chỉ còn hai nước, cả hai đều tệ hơn pool:
    #   A. --relax : bật store_rejects cho cả 16 file. Chạy được, nhưng giờ 15
    #      file lành cũng mất strict mode — một file gãy sau này sẽ trôi qua
    #      lặng lẽ. Bán vùng phủ sóng để mua một ngày.
    #   B. mặc định: LOẠI ngày hỏng khỏi danh sách, xử riêng bằng pool. Giữ
    #      strict cho 15 file còn lại, đổi lại kết quả không còn đủ era.
    # Mặc định là B: thà thiếu một ngày còn hơn nới luật cho 15 ngày.
    if BAD_DAY in [d for d in daterange(start, end)] and not args.relax:
        files = [f for f in files if BAD_DAY not in f.name]
        print(f"[skip] {BAD_DAY} bị loại: file gãy, strict mode không đọc nổi. "
              f"Chạy lại với --relax để nuốt nó (và mất strict cho cả era).")
    file_list = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"

    read = (f"read_csv({file_list}, header=true, filename=true, "
            + ("store_rejects=true, " if args.relax else "")
            + f"columns={COLS[args.era]})")

    # ALIGN và CANON của A10 đều liệt kê cột TƯỜNG MINH — cột lạ nào không có
    # tên trong danh sách sẽ rụng ở tầng đó. Nên phải chèn vào CẢ HAI, đúng
    # theo thứ tự chúng chạy: src -> aligned -> canon. Chèn một tầng thôi thì
    # SQL vẫn chạy và _data_date vẫn biến mất, im lặng.
    STAMP = "CAST(filename[-14:-5] AS DATE) AS _data_date"
    aligned = (ALIGN[args.era]
               .replace("FROM src", f"FROM {read}")
               .replace("SELECT", f"SELECT {STAMP},", 1))
    canon = CANON.replace("SELECT", "SELECT _data_date,", 1)
    # count=1: chỉ thay SELECT ĐẦU TIÊN. CANON có subquery bên trong; thay hết
    # là hỏng. Đây là lý do string-injection vào SQL người khác viết luôn là
    # thứ phải đọc lại hai lần — nó không có kiểu, không ai kiểm hộ.

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            WITH aligned AS ({aligned}), canon AS ({canon})
            SELECT * FROM canon
        ) TO '{out.as_posix()}' (FORMAT parquet, PARTITION_BY (order_date),
                                 OVERWRITE_OR_IGNORE, ROW_GROUP_SIZE 122880)""")
    wall = time.perf_counter() - t0
    # PARTITION_BY an toàn ở ĐÂY vì chỉ có một writer trên đời. Đúng câu lệnh
    # này trong worker của Task 3 là thảm hoạ im lặng. Kỹ thuật không tốt hay
    # xấu — nó đúng hoặc sai TÙY SỐ WRITER.
    # Nếu full scale ném memory error: SET preserve_insertion_order=false (A14).

    n, stamps, unstamped = con.execute(
        f"""SELECT count(*), count(DISTINCT _data_date),
                   count(*) FILTER (_data_date IS NULL)
            FROM read_parquet('{out.as_posix()}/*/*.parquet')""").fetchone()
    print(f"{wall:.1f}s   {n:,} rows -> {out}")
    print(f"_data_date distinct / NULL: {stamps} / {unstamped}   "
          f"(expect {len(files)} / 0)")
    # stamps < len(files) -> một file không có dòng nào sống sót, hoặc lát cắt
    # sai vị trí. In thử: SELECT DISTINCT filename, _data_date ... LIMIT 5.
    print("Mở thư mục ra đếm: có __HIVE_DEFAULT_PARTITION__ không? Nếu CANON "
          "giữ nguyên WHERE thì KHÔNG — dòng NULL ts đã bị vứt trước khi ghi.")
    print("So với pool trên CÙNG dải ngày. Đo, đừng cãi.")
    print("Trade-off ghi journal: code đơn giản, thường nhanh — nhưng "
          "all-or-nothing: không retry từng ngày, không state trong ops.etl_runs. "
          "Ngôn ngữ A11: đây là restatement, không phải backfill.")
    con.close()


if __name__ == "__main__":
    main()
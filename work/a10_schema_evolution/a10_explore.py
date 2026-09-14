# %% ── A10 explore — chạy theo từng cell (# %%), KHÔNG chạy cả file một lượt ──
# python -i -m work.a10_schema_evolution.a10_explore   hoặc bấm Run Cell trong VS Code
from pathlib import Path

from work.a10_schema_evolution.a10_bootstrap import (
    boot, section, manifest, DAY_V1, DAY_V2, DAY_V3, BAD_DAY)
from work.a10_schema_evolution import a10_canonical as C

con, ROOT, RAW, MANIFEST = boot("small")


# %% ── Task 0 — tiền đề: trả partition 06-19 về trạng thái A02 ──────────────
section("TASK 0 — lake month 1 có khớp manifest không")
con.sql(f"""
SELECT (SELECT count(*) FROM read_parquet(
          '{(ROOT / "lake" / "orders").as_posix()}/*/*.parquet',
          hive_partitioning=true))                        AS lake_rows,
       (SELECT sum(rows) FROM read_json_auto(
          '{(MANIFEST / "orders_*.json").as_posix()}') WHERE era='v1') AS manifest_v1
""").show()
# Nếu lệch 889 (small) / 17.853 (full): đó là số dòng A09 đã dedupe khỏi
# partition 06-19 — kết quả ĐÚNG của A09, nhưng Task 7d đối soát với manifest
# THÔ, vốn không biết gì về dedupe. Chạy trước khi đi tiếp:
#   python -m work.a10_schema_evolution.a10_restore_0619 --scale small --dry-run


# %% ── Task 1 — month 2 đã sinh chưa ────────────────────────────────────────
section("TASK 1 — month 2")
# Chạy ở PowerShell TRƯỚC (2 terminal):
#   python datagen/generate.py --scale small --month 2
#   python datagen/generate.py --scale full  --month 2      # ~5-10 phút, để chạy nền
print("csv :", len(list(RAW.glob("orders_*.csv"))))        # Expect: 69
print("json:", len(list(MANIFEST.glob("orders_*.json"))))  # Expect: 69

# era census — nguồn số liệu cho journal
con.sql(f"""SELECT era, count(*) AS files, sum(rows) AS rows
            FROM read_json_auto('{(MANIFEST / "orders_*.json").as_posix()}')
            GROUP BY era ORDER BY era""").show()
# Expect: v1 45 2,773,566 | v2 16 907,584 | v3 8 435,690


# %% ── Task 2 — phá loader CÓ CHỦ Ý ─────────────────────────────────────────
section("TASK 2 — loader v1 gặp v2 và v3")
# TODO(you): gọi loader A07 của bạn cho 2 ngày dưới, bọc try/except, in
# nguyên văn error ra journal. ĐỪNG sửa loader.
#   load_day("2026-07-16")
#   load_day("2026-08-01")
#
# Rồi chạy contract gate A06 trên CÙNG 2 file đó và so hai thông điệp.
# Điểm của task: error thô nói về "CSV parsing dialect" (nghe như lỗi delimiter),
# gate nói đúng vấn đề (cột lạ). Ghi 1 câu vào journal giải thích vì sao.


# %% ── Task 3a — dò ranh giới era bằng HEADER ───────────────────────────────
section("TASK 3a — header signature")
sigs: dict[str, list[str]] = {}
for f in sorted(RAW.glob("orders_*.csv")):
    header = open(f, encoding="utf-8").readline().strip()
    sigs.setdefault(header, []).append(f.stem[-10:])
for h, dates in sigs.items():
    print(f"{len(dates):3d} files  {dates[0]} .. {dates[-1]}\n     {h[:100]}")
# Expect: 3 header — 45 / 16 / 8 file; ranh giới 07-15|07-16 và 07-31|08-01.
# Vì sao so header chứ không so sniffed type: type sniff dao động theo từng
# file (mẫu quyết định), header thì rẻ và ổn định.


# %% ── Task 3b — union_by_name giấu cái gì ──────────────────────────────────
section("TASK 3b — union_by_name")
files = [str(RAW / f"orders_{d}.csv") for d in (DAY_V1, DAY_V2, DAY_V3)]
con.sql(f"DESCRIBE SELECT * FROM read_csv({files}, union_by_name=true, filename=true)").show()
# Ba thứ phải nhìn thấy:
#  1. order_id ra VARCHAR  -> lịch sử BIGINT của bạn không join được nữa
#  2. CÓ CẢ payment_method LẪN payment_type, mỗi cột NULL một nửa  <- rename bị GIẤU
#  3. v1 rows NULL ở currency/... -> NULL padding tự động

con.sql(f"""SELECT filename[-14:-4] AS day, count(*) AS rows,
                   count(currency) AS has_currency,
                   count(payment_method) AS pm, count(payment_type) AS pt
            FROM read_csv({files}, union_by_name=true, filename=true)
            GROUP BY 1 ORDER BY 1""").show()

# TODO(you): điền bảng phân loại 6 dòng vào journal
#   (change | class: additive/rename/type/nested/reorder | loud hay silent | vì sao)
# Và mở contracts/shopcore_orders_daily.contract.yaml, khối change_management:
# trích dẫn điều khoản bị vi phạm (notice_breaking) — đó là thứ biến "data hỏng"
# thành một incident CÓ CHỦ SỞ HỮU.


# %% ── Task 4 — canonical schema (thiết kế, không đụng data) ────────────────
section("TASK 4 — canonical schema")
# Không có code. Chép bảng 13 dòng trong đề vào journal, mỗi default một dòng
# lý do: "known value" -> điền giá trị; "unknown" -> NULL.
#   currency v1        = 'USD'  (biết chắc)
#   discount_amount v1 = 0      (biết chắc: v1 chưa có discount)  <- dòng này
#       chính là thứ khiến MỘT công thức reconciliation chạy đúng cho cả 3 era
#   channel v1         = NULL   (không biết)
# + Một đoạn văn: gọi tên order_id/order_id_num là warehouse-controlled
#   canonical key, và biện luận nó so với (a) dùng thẳng key của producer và
#   (c) surrogate key thật. Lấy drift ở Task 3 làm BẰNG CHỨNG, không phải ý thích.


# %% ── Task 5 — aligner từng era ────────────────────────────────────────────
section("TASK 5 — aligners")
# TODO(you): viết V3_COLS và ALIGN["v2"] trong a10_canonical.py trước khi chạy cell này.
for day in (DAY_V1, DAY_V2, DAY_V3):
    era = C.open_day(con, day, RAW)
    cols = con.sql("DESCRIBE SELECT * FROM aligned").df()
    print(f"{day} [{era}] -> {len(cols)} cols:", list(cols.column_name))
# Expect: CẢ BA ra ĐÚNG 15 cột, ĐÚNG một thứ tự.

# spot-check v3 — bắt lỗi map theo vị trí
C.open_day(con, DAY_V3, RAW)
con.sql("SELECT order_id, order_id_num, currency, items FROM aligned LIMIT 2").show()
# order_id_num phải là số trần; items phải là chuỗi JSON mảng, KHÔNG phải 'USD'.


# %% ── Task 6a — staging canonical + 2 cột lineage ──────────────────────────
section("TASK 6a — staging.shopcore_orders_canon")
for day in (DAY_V1, DAY_V2, DAY_V3):
    C.open_day(con, day, RAW)
    dropped = con.execute(C.DROPPED_SQL).fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TABLE staging.shopcore_orders_canon AS
      SELECT *, DATE '{day}'         AS _data_date,
                CAST(NULL AS BIGINT) AS _run_id
      FROM ({C.CANON})""")
    kept = con.execute("SELECT count(*) FROM staging.shopcore_orders_canon").fetchone()[0]
    claimed = manifest(MANIFEST, day)["rows"]
    print(f"{day}: kept {kept:,} + dropped {dropped:,} = {kept + dropped:,} "
          f"| manifest {claimed:,} | match={kept + dropped == claimed}")
# Expect: 06-02 -> 59,024 · 07-16 -> 59,613 · 08-01 -> 47,008

con.sql("DESCRIBE staging.shopcore_orders_canon").show()
# Expect: 18 cột, KẾT THÚC đúng bằng _data_date DATE, _run_id BIGINT. Hai cột,
# hết. source_file/loaded_at là fact về RUN, không phải về ORDER (§7.2) —
# chúng sống ở ops.etl_runs, trỏ tới bằng _run_id.


# %% ── Task 6b — MỘT công thức reconciliation cho cả 3 era ──────────────────
section("TASK 6b — reconciliation")
for day in (DAY_V1, DAY_V2, DAY_V3):
    C.open_day(con, day, RAW)
    con.execute(f"""CREATE OR REPLACE TABLE staging.shopcore_orders_canon AS
      SELECT *, DATE '{day}' AS _data_date, CAST(NULL AS BIGINT) AS _run_id
      FROM ({C.CANON})""")
    con.sql(f"""
    SELECT '{day}' AS day,
           count(*) FILTER (abs(order_total - (items_sum - discount_amount)) <= 0.01) AS ok,
           count(*) AS total
    FROM (SELECT order_total, discount_amount,
                 (SELECT sum(i.qty * i.unit_price) FROM unnest(items) AS t(i)) AS items_sum
          FROM staging.shopcore_orders_canon
          WHERE order_total IS NOT NULL AND items IS NOT NULL)""").show()
# Expect: ~99.4% ok ở MỌI era. Phần lệch ~0.5% là rác của A05, không hơn.
# Ngày v1 qua được công thức của era v2 CHỈ VÌ bạn đặt discount_amount = 0.


# %% ── Task 6c — correction join: cú đấm của cả bài ─────────────────────────
section("TASK 6c — correction join")
# hist = staging canonical của 07-25..07-31 (era v2)
con.execute("DROP TABLE IF EXISTS staging.a10_hist")
first = True
for day in [f"2026-07-{d}" for d in range(25, 32)]:
    C.open_day(con, day, RAW)
    verb = "CREATE TABLE staging.a10_hist AS" if first else "INSERT INTO staging.a10_hist"
    con.execute(f"{verb} SELECT order_id_num FROM ({C.CANON})")
    first = False

f_v3 = (RAW / f"orders_{DAY_V3}.csv").as_posix()
con.execute(f"""CREATE OR REPLACE VIEW v3day AS
  SELECT * FROM read_csv('{f_v3}', header=true, columns={C.V3_COLS})""")

con.sql("""SELECT count(*) AS naive FROM v3day v JOIN staging.a10_hist h
             ON v.order_id = CAST(h.order_id_num AS VARCHAR)""").show()
# Expect: 0

con.sql("""SELECT count(DISTINCT v.order_id) AS canonical
           FROM v3day v JOIN staging.a10_hist h
             ON CAST(replace(v.order_id,'ORD-','') AS BIGINT) = h.order_id_num""").show()
# Expect: 705  (= late_rows trong orders_2026-08-01.json)
#
# 0 nghĩa là: mỗi correction đó sẽ được MERGE của A08 upsert thành một ORDER MỚI.
# Double counting, không một lỗi nào được ném ra. Đó là lý do order_id_num tồn tại.


# %% ── Task 9 — giải thích lại (journal) ────────────────────────────────────
section("TASK 9")
# Tối thiểu 3 câu:
#  1. thay đổi nào là breaking, vì sao
#  2. thay đổi nào NGUY HIỂM NHẤT dù trên giấy không "breaking"
#     (gợi ý: cái chưa bao giờ ném ra một error nào)
#  3. bạn sẽ yêu cầu upstream làm khác đi điều gì (version field trong file?
#     báo trước khi ship? cả hai?)

# con.close()   <- chạy khi xong session, tránh khoá file warehouse.duckdb
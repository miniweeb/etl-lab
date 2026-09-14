# %% [markdown]
# # A10 — Schema evolution: month 2 arrives
#
# Chạy file này theo từng cell trong VS Code (`# %%`). Đừng chạy cả file một lượt —
# nó sẽ chết ở Task 5 khi `V3_COLS` còn rỗng, và đó là đúng: các cell sau phụ
# thuộc vào phần bạn tự viết trong `a10_canonical.py`.
#
# **File này không chứa toàn bộ bài.** Task 7 và 8 nặng và chạy lâu nên ở script
# riêng cùng thư mục.
#
# | Task | Chạy ở đâu |
# |---|---|
# | 0 | `a10_restore_0619.py` ở terminal, rồi cell 2 để xác nhận |
# | 1 | `datagen/generate.py` ở terminal, rồi cell 3 |
# | 2 | cell 4 — bạn tự viết, gọi loader A07 |
# | 3, 4, 5, 6, 9 | cell 5 trở đi |
# | 7 | `a10_rebuild_lake.py` ở terminal |
# | 8 | `a10_gate_eras.py` ở terminal |
#
# Warehouse: DuckDB chỉ cho MỘT process ghi. Cell SETUP mở `warehouse.duckdb`.
# Trước khi chạy `a10_rebuild_lake.py` hay `a10_gate_eras.py` ở terminal, phải
# chạy cell cuối (`con.close()`), nếu không sẽ gặp IOException.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Chạy lại cell này mỗi khi kernel mất biến (NameError).
# ─────────────────────────────────────────────────────────────────────────────
import os
from pathlib import Path

# Mở file này trực tiếp thì cwd là work/a10_schema_evolution/ → import lib và
# work đều lỗi. Leo ngược về repo root. Có chặn: tới gốc ổ đĩa thì DỪNG và báo
# lỗi, thay vì os.chdir("..") mãi mãi ở D:\ mà không ai biết.
_here = Path.cwd()
for _cand in [_here, *_here.parents]:
    if (_cand / "lib").is_dir():
        os.chdir(_cand)
        break
else:
    raise RuntimeError(f"không tìm thấy repo root (thư mục có lib/) từ {_here}")
print("cwd:", os.getcwd())

from work.a10_schema_evolution.a10_bootstrap import (
    boot, section, manifest, DAY_V1, DAY_V2, DAY_V3, BAD_DAY)
from work.a10_schema_evolution import a10_canonical as C

SCALE = "small"                     # Task 7(e) mới đổi "full", và chỉ ở script
con, ROOT, RAW, MANIFEST = boot(SCALE)

print("scale:", SCALE, "| raw:", RAW)


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 0 — Xác nhận lake month 1 khớp manifest.
# (Việc restore làm bằng a10_restore_0619.py ở terminal. Cell này chỉ để kiểm.)
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 0 — lake month 1 vs manifest v1")
con.sql(f"""
SELECT (SELECT count(*) FROM read_parquet(
          '{(ROOT / "lake" / "orders").as_posix()}/*/*.parquet',
          hive_partitioning=true))                                     AS lake_rows,
       (SELECT sum(rows) FROM read_json_auto(
          '{(MANIFEST / "orders_*.json").as_posix()}') WHERE era='v1') AS manifest_v1
""").show()
# Kỳ vọng small: 2,773,566 = 2,773,566
#
# Lệch đúng 889 (small) / 17.853 (full) nghĩa là partition 06-19 vẫn ở trạng
# thái A09 dedupe. Đó là kết quả ĐÚNG của A09, nhưng Task 7d đối soát với
# manifest THÔ, vốn không biết gì về dedupe. Chạy ở terminal:
#   python -m work.a10_schema_evolution.a10_restore_0619 --scale small --dry-run


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Month 2 về.
# ─────────────────────────────────────────────────────────────────────────────
#
# Chạy ở TERMINAL trước, hai cửa sổ:
#   python datagen/generate.py --scale small --month 2
#   python datagen/generate.py --scale full  --month 2     # ~5-10 phút, chạy nền
#   python datagen/selftest.py --scale small               # luật A00: sinh xong thì verify

section("TASK 1 — month 2")
print("csv :", len(list(RAW.glob("orders_*.csv"))))        # Kỳ vọng: 69
print("json:", len(list(MANIFEST.glob("orders_*.json"))))  # Kỳ vọng: 69

con.sql(f"""SELECT era, count(*) AS files, sum(rows) AS rows
            FROM read_json_auto('{(MANIFEST / "orders_*.json").as_posix()}')
            GROUP BY era ORDER BY era""").show()
# Kỳ vọng: v1 45 2,773,566 | v2 16 907,584 | v3 8 435,690
#
# Manifest tự khai era — tiện, nhưng ĐỪNG dùng nó để quyết định cách đọc file.
# Task 3 dò era từ chính header, vì header mới là thứ read_csv phải đối mặt.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Phá loader CÓ CHỦ Ý.
# ─────────────────────────────────────────────────────────────────────────────
#
# TODO(you): gọi loader A07 cho hai ngày dưới, bọc try/except, in NGUYÊN VĂN
# error. ĐỪNG sửa loader — cả bài dựa trên việc nó gãy.
#
#   import sys; sys.path.insert(0, "work/a07_idempotency")
#   from a07_load_day import load_day
#   for d in ("2026-07-16", "2026-08-01"):
#       try:
#           load_day(...)
#       except Exception as e:
#           print(d, "->", type(e).__name__, e)
#
# Rồi chạy contract gate A06 trên CÙNG hai file và so hai thông điệp.
#
# %%
# ⚠️ Cell này GHI vào warehouse (loader A07 chạy thật), rồi con.close() để
#    nhường lock cho gate. Chạy lại CELL 1 sau khi xong.
import subprocess
import sys
from datetime import date

sys.path.insert(0, "work/a07_idempotency")
from a07_load_day import load_day

DAYS = (date(2026, 7, 16), date(2026, 8, 1))

print("=== 1. LOADER A07 (schema v1, 10 cột) ===")
for d in DAYS:
    try:
        n = load_day(con, SCALE, d)
        print(f"\n{d} -> SUCCESS, {n:,} rows   <-- KHÔNG ném lỗi. Đọc kỹ.")
    except Exception as e:
        print(f"\n{d} -> {type(e).__name__}\n{e}")

print("\n=== ops.etl_runs, 3 lần chạy gần nhất ===")
con.sql("SELECT run_id, run_date, status, rows FROM ops.etl_runs "
        "ORDER BY run_id DESC LIMIT 3").show()

con.close()          # gate chạy ở process riêng, DuckDB chỉ cho một writer

print("\n=== 2. CONTRACT GATE A06 (cùng hai file) ===")
for d in DAYS:
    r = subprocess.run(
        [sys.executable, "work/a06_data_contract/contract_check.py",
         "--scale", SCALE, "--start", d.isoformat(), "--end", d.isoformat()],
        capture_output=True, text=True)
    print(f"\n--- {d} ---\n{r.stdout}{r.stderr}")


# %%
import subprocess, sys
from datetime import date

DAYS = (date(2026, 7, 16), date(2026, 8, 1))

print("=== 2. CONTRACT GATE A06 (cùng hai file) ===")
for d in DAYS:
    r = subprocess.run(
        [sys.executable, "-m", "work.a06_data_contract.a06_contract_check",
         "--scale", SCALE, "--start", d.isoformat(), "--end", d.isoformat()],
        capture_output=True, text=True)
    print(f"\n--- {d} ---\n{r.stdout}{r.stderr}")


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 3a — Dò ranh giới era bằng HEADER.
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 3a — header signature")
sigs: dict[str, list[str]] = {}
for f in sorted(RAW.glob("orders_*.csv")):
    sigs.setdefault(open(f, encoding="utf-8").readline().strip(), []).append(f.stem[-10:])
for h, dates in sigs.items():
    print(f"{len(dates):3d} files  {dates[0]} .. {dates[-1]}\n     {h}\n")
# Kỳ vọng: đúng 3 header — 45 / 16 / 8 file; ranh giới 07-15|07-16 và 07-31|08-01.
#
# Vì sao so header chứ không so kiểu sniff được: kiểu dao động theo từng file
# (bài học 0.2% của A01 — vài phần nghìn dòng lật kiểu cả cột). Header rẻ và ổn định.
#
# GIỮ LẠI header thứ ba: Task 5 cần nó để viết V3_COLS đúng THỨ TỰ.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 3b — union_by_name giấu cái gì.
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 3b — union_by_name")
files = [str(RAW / f"orders_{d}.csv") for d in (DAY_V1, DAY_V2, DAY_V3)]
con.sql(f"DESCRIBE SELECT * FROM read_csv({files}, union_by_name=true)").show()
# Ba thứ phải tự nhìn ra:
#   1. order_id thành VARCHAR — v3 ép promote. Lịch sử BIGINT hết join được.
#   2. CÓ CẢ payment_method LẪN payment_type, mỗi cột NULL một nửa.
#      union_by_name khớp theo TÊN, nó không thể biết hai cột này là một.
#      Nó vá được thay đổi additive, và GIẤU ĐI rename.
#   3. v1 rows NULL ở currency/discount_amount/channel — NULL padding tự động.

con.sql(f"""SELECT regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) AS day,
                   count(*) AS rows, count(currency) AS currency,
                   count(payment_method) AS pm, count(payment_type) AS pt
            FROM read_csv({files}, union_by_name=true, filename=true)
            GROUP BY 1 ORDER BY 1""").show()
# pm và pt không bao giờ cùng khác 0 trên một ngày — rename hiện nguyên hình.

# TODO(you): bảng phân loại 6 dòng vào journal
#   change | class (additive/rename/type/nested/reorder) | loud hay silent | vì sao
#
# Và mở contracts/shopcore_orders_daily.contract.yaml khối change_management:
# trích dẫn điều khoản bị vi phạm (change_management.notice_breaking — 30 ngày).
# Chính câu trích đó biến "data hỏng" thành incident CÓ CHỦ SỞ HỮU.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Canonical schema. Bước thiết kế, không đụng data.
# ─────────────────────────────────────────────────────────────────────────────
#
# Cell này cố ý không có code. Chép bảng 13 dòng của đề vào journal, mỗi default
# kèm một dòng lý do thuộc đúng một trong hai loại:
#     known value -> điền giá trị          unknown -> NULL
#
#   currency v1        = 'USD'   (biết chắc)
#   discount_amount v1 = 0       (biết chắc: v1 chưa có discount)
#       ^ chính dòng này làm MỘT công thức reconciliation chạy đúng cả ba era.
#         Để NULL thì NULL lan ra và công thức hỏng — xem Task 6b.
#   channel v1         = NULL    (không biết; điền 'web' là BỊA DỮ LIỆU)
#
# + Một đoạn văn: gọi tên cặp order_id/order_id_num là warehouse-controlled
#   canonical key, rồi biện luận nó so với (a) dùng thẳng key của producer và
#   (c) surrogate key thật. Lấy drift ở Task 3 làm BẰNG CHỨNG, không phải ý thích.
#   Đọc kèm guides/data_modeling.md §3 (Keys).


# %%
f = (RAW / "orders_2026-07-16.csv").as_posix()
con.sql(f"""SELECT count(*) AS total,
                   count(*) FILTER (TRY_CAST(discount_amount AS DECIMAL(14,2)) IS NULL
                                    AND discount_amount IS NOT NULL) AS dirty
            FROM read_csv('{f}', header=true, all_varchar=true)""").show()


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — Aligner từng era.
# ⚠️ Cần V3_COLS và ALIGN["v2"] trong a10_canonical.py TRƯỚC khi chạy cell này.
# ─────────────────────────────────────────────────────────────────────────────
#
# V3_COLS dựng từ header thứ ba ở Task 3a, ĐÚNG THỨ TỰ TRONG FILE — columns=
# map theo VỊ TRÍ, không theo tên. Sai thứ tự thì DuckDB nạp 'USD' vào items
# mà không báo lỗi nào.
section("TASK 5 — aligners")
for day in (DAY_V1, DAY_V2, DAY_V3):
    era = C.open_day(con, day, RAW)
    cols = list(con.sql("DESCRIBE SELECT * FROM aligned").df().column_name)
    print(f"{day} [{era}] -> {len(cols)} cols: {cols}")
# Kỳ vọng: CẢ BA ra ĐÚNG 15 cột, ĐÚNG cùng một thứ tự.
# Đó là toàn bộ việc của tầng aligner: sau nó, hạ nguồn không còn cách nào biết
# row đến từ era nào.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 5b — Spot-check v3. Bắt lỗi map-theo-vị-trí, thứ không ném error.
# ─────────────────────────────────────────────────────────────────────────────
C.open_day(con, DAY_V3, RAW)
con.sql("SELECT order_id, order_id_num, payment_method, currency, "
        "substr(items, 1, 40) AS items_head FROM aligned LIMIT 3").show()
# order_id_num phải là số trần. currency phải là 'USD'/'EUR'/'GBP'/'VND'.
# items_head phải bắt đầu bằng '[{'. Nếu nó ra 'USD' thì V3_COLS sai thứ tự.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 6a — Staging canonical + hai cột lineage.
# ⚠️ CREATE OR REPLACE TABLE staging.shopcore_orders_canon, lặp ba lần.
#    Bảng cuối vòng lặp là của ngày v3.
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 6a — staging.shopcore_orders_canon")
for day in (DAY_V1, DAY_V2, DAY_V3):
    C.open_day(con, day, RAW)
    dropped = con.execute(C.DROPPED_SQL).fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TABLE staging.shopcore_orders_canon AS
      SELECT *, DATE '{day}'         AS _data_date,   -- delivery nào giao row này
                CAST(NULL AS BIGINT) AS _run_id       -- sống dậy ở A11
      FROM ({C.CANON})""")
    kept = con.execute("SELECT count(*) FROM staging.shopcore_orders_canon").fetchone()[0]
    claimed = manifest(MANIFEST, day)["rows"]
    print(f"{day}: kept {kept:>6,} + dropped {dropped:>3,} = {kept + dropped:>6,}"
          f" | manifest {claimed:>6,} | match={kept + dropped == claimed}")
# Kỳ vọng: 06-02 → 59,024 · 07-16 → 59,613 · 08-01 → 47,008
#
# dropped = row không parse nổi timestamp → không có order_date → không partition
# được. ĐẾM MỖI LẦN CHẠY: một row bị bỏ mà không đếm là một lời nói dối trong
# phép cộng. (Nếu quarantine A05 đã nối vào loader thì route chúng vào đó.)

con.sql("DESCRIBE staging.shopcore_orders_canon").show()
# Kỳ vọng: 18 cột = 16 canonical + đúng hai cột lineage, KẾT THÚC bằng
# _data_date DATE rồi _run_id BIGINT. Hai, và hết (§7.2): source_file,
# loaded_at, status là fact về LẦN CHẠY chứ không phải về ĐƠN HÀNG — chúng sống
# ở ops.etl_runs, mỗi row trỏ tới bằng _run_id 8 byte.


# %%
con.sql("SELECT items[1].sku, items[1].qty, items[1].disc FROM staging.shopcore_orders_canon LIMIT 3").show()

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 6b — MỘT công thức reconciliation cho cả ba era.
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 6b — reconciliation")
for day in (DAY_V1, DAY_V2, DAY_V3):
    C.open_day(con, day, RAW)
    con.execute(f"CREATE OR REPLACE VIEW canon_day AS {C.CANON}")
    con.sql(f"""
    SELECT '{day}' AS day, count(*) AS total,
           count(*) FILTER (abs(order_total - (items_sum - discount_amount)) <= 0.01) AS ok,
           round(100.0 * count(*) FILTER (
             abs(order_total - (items_sum - discount_amount)) <= 0.01) / count(*), 2) AS pct
    FROM (SELECT order_total, discount_amount,
                 (SELECT sum(i.qty * i.unit_price) FROM unnest(items) AS t(i)) AS items_sum
          FROM canon_day
          WHERE order_total IS NOT NULL AND items IS NOT NULL)""").show()
# Kỳ vọng: ~99.4% ở MỌI era (selftest đã đo v2 ra 99.4454% — con số này phải
# tái tạo lại được). Phần lệch ~0.5% là rác đã biết, địa hạt A05.
#
# Ngày v1 qua được công thức của era v2 CHỈ VÌ bạn đặt discount_amount = 0 chứ
# không phải NULL. Dừng một nhịp ở chỗ này.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 6c — Correction join. Cú đấm của cả bài.
# ─────────────────────────────────────────────────────────────────────────────
section("TASK 6c — correction join")
# hist = order_id_num của 07-25..07-31 (era v2, đã canonical hoá)
con.execute("DROP TABLE IF EXISTS staging.a10_hist")
for i, day in enumerate(f"2026-07-{d}" for d in range(25, 32)):
    C.open_day(con, day, RAW)
    verb = ("CREATE TABLE staging.a10_hist AS" if i == 0
            else "INSERT INTO staging.a10_hist")
    con.execute(f"{verb} SELECT order_id_num FROM ({C.CANON})")
print("hist rows:", con.execute("SELECT count(*) FROM staging.a10_hist").fetchone()[0])

f_v3 = (RAW / f"orders_{DAY_V3}.csv").as_posix()
con.execute(f"""CREATE OR REPLACE VIEW v3day AS
  SELECT * FROM read_csv('{f_v3}', header=true, columns={C.V3_COLS})""")

con.sql("""SELECT count(*) AS naive FROM v3day v JOIN staging.a10_hist h
             ON v.order_id = CAST(h.order_id_num AS VARCHAR)""").show()
# Kỳ vọng: 0

con.sql("""SELECT count(DISTINCT v.order_id) AS canonical
           FROM v3day v JOIN staging.a10_hist h
             ON CAST(replace(v.order_id, 'ORD-', '') AS BIGINT) = h.order_id_num""").show()
# Kỳ vọng: 705  (= late_rows trong orders_2026-08-01.json)
#
# 0 nghĩa là: mỗi correction đó sẽ được MERGE của A08 upsert thành một ĐƠN MỚI.
# Doanh thu đếm hai lần, không một error nào được ném ra, không dashboard nào đỏ.
# Đó là lý do order_id_num tồn tại.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 7.
# ─────────────────────────────────────────────────────────────────────────────
import duckdb
c = duckdb.connect('D:/etl_lab/warehouse/warehouse.duckdb', read_only=True)
print(c.sql("SELECT count(*), count(DISTINCT order_date) FROM core.lake_orders"))
c.close()


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 8.
# ─────────────────────────────────────────────────────────────────────────────

import duckdb
from pathlib import Path
from work.a10_schema_evolution import a10_gate_eras as G

G.EXPECTED = G.load_all_contracts()
con = duckdb.connect()

# file v3 thật, nhưng giả vờ nó mang tên ngày 2026-07-20 (era v2)
p = Path("D:/etl_lab/small/raw/orders/orders_2026-08-01.csv")
print(G.check_one(con, p, "2026-07-20"))
con.close()
# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 9 — Giải thích lại (journal).
# ─────────────────────────────────────────────────────────────────────────────
#
# Tối thiểu ba câu:
#   1. thay đổi nào là breaking, vì sao
#   2. thay đổi nào NGUY HIỂM NHẤT dù trên giấy không "breaking"
#      (gợi ý: cái chưa bao giờ ném ra một error nào)
#   3. bạn yêu cầu upstream làm khác đi điều gì — version field trong file?
#      báo trước khi ship? cả hai?


# %%
# ─────────────────────────────────────────────────────────────────────────────
# ĐÓNG SESSION. Bắt buộc trước khi chạy a10_rebuild_lake.py hay a10_gate_eras.py
# ở terminal — DuckDB chỉ cho một process ghi vào warehouse.duckdb.
# ─────────────────────────────────────────────────────────────────────────────
con.close()
print("closed")
# %%
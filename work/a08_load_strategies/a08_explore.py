# %% [markdown]
# # A08 — Append vs upsert vs partition refresh
#
# Chạy file này theo từng cell trong VS Code (`# %%`). Đừng chạy cả file một lượt.
#
# Bài này dựng BA chiến lược load trên cùng 10 file, rồi chứng minh chúng cho ra
# cùng một trạng thái hiện hành, tới từng dòng. Trọng tâm không phải dựng xong
# một chiến lược nào, mà là chứng minh được sự đồng thuận.
#
# Nhịp làm việc:
#   1. Chạy cell SETUP (cell 1) trước, luôn luôn.
#   2. Mỗi task một cell. Chạy xong thì dán output cho tôi, tôi xác nhận rồi đi tiếp.
#   3. Cell nào có `CREATE OR REPLACE` thì đọc cảnh báo ngay trên nó.
#
# Warehouse: DuckDB chỉ cho MỘT process ghi. Nếu bạn định chạy script ở terminal
# thì phải `con.close()` ở cell cuối file này trước, rồi mở lại sau.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Chạy lại cell này mỗi khi kernel mất biến (NameError).
# ─────────────────────────────────────────────────────────────────────────────
import os

# Nếu bạn mở file này trực tiếp, cwd sẽ là work/a08_load_strategies/ → import lib sẽ lỗi.
# Đưa cwd về repo root, nếu không sẽ gặp ModuleNotFoundError: lib
while not os.path.isdir("lib"):
    os.chdir("..")
print("cwd:", os.getcwd())

import datetime as dt
import time

import duckdb

from lib.labpaths import data_root, manifest_dir, raw_orders_dir, tmp_dir, warehouse_path

SCALE = "full" #đổi sang full ở task 8
ROOT = data_root(SCALE)
RAW = raw_orders_dir(SCALE)                              # thay ROOT / "raw" / "orders"
MAN = (manifest_dir(SCALE) / "orders_*.json").as_posix() # thay ROOT / "raw" / "manifest" / ...

START = dt.date(2026, 6, 1)
DATES = [START + dt.timedelta(days=i) for i in range(10)]  # 06-01 .. 06-10

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
con.execute("CREATE SCHEMA IF NOT EXISTS core")  # đã có từ A03; chạy lại vô hại

TS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"

print("scale:", SCALE, "| window:", DATES[0], "→", DATES[-1])


# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — staged_select(): câu SELECT dùng chung cho cả ba chiến lược.
# ─────────────────────────────────────────────────────────────────────────────
#
# Ba điều đáng để ý trong hàm này, và cả ba đều đã có lý do từ các bài trước:
#
# 1. CHỈ MỘT cột lineage: `_data_date`. Đó là toàn bộ danh sách.
#    Nó vẫn cho biết dòng này đến từ file nào, vì contract quy định
#    path_pattern là một file cho mỗi business date — nên _data_date CHÍNH LÀ
#    orders_<_data_date>.csv. Còn tên file, thời điểm load, status và số dòng là
#    sự thật về LẦN CHẠY, chúng nằm ở dòng trong ops.etl_runs mà _run_id trỏ tới.
#    Mấy bảng thí nghiệm hôm nay được đổ đầy bằng vòng lặp trần, không qua loader
#    có ledger của A07, nên chúng cũng không mang _run_id. Bảng core.orders thật
#    của bạn thì có đủ cả hai.
#
# 2. order_date có COALESCE lùi về ngày của file. Khoảng 0,02% số dòng có
#    timestamp bất khả thi, làm sạch xong thành NULL. Mà KHOÁ PARTITION THÌ
#    TUYỆT ĐỐI KHÔNG ĐƯỢC NULL: một dòng ngày NULL sẽ không bao giờ khớp
#    WHERE order_date BETWEEN … — nên chiến lược C sẽ lặng lẽ đánh mất nó vĩnh viễn.
#    (A07 COALESCE về ngày của updated_at; hôm nay dùng ngày của file để chỉ cần
#    nhìn tên file là biết dòng đó thuộc partition nào. Cách nào cũng được, miễn
#    chọn một rồi giữ nguyên.)
#
# 3. Bỏ items và meta ra ngoài. Ba chiến lược hành xử y hệt khi có thêm cột, mà
#    hai chuỗi JSON đó chỉ làm mỗi lần ghi lại nặng thêm.


def staged_select(d: dt.date) -> str:
    """Một file một ngày, staged tối thiểu bằng cleaner của A03, kèm _data_date."""
    f = (RAW / f"orders_{d}.csv").as_posix()
    return f"""
    SELECT
      order_id,
      customer_id,
      store_id,
      try_strptime(order_ts, {TS}) AS order_ts,
      updated_at,
      status,
      payment_method,
      TRY_CAST(NULLIF(
        CASE WHEN order_total LIKE '%,%'
             THEN replace(replace(order_total, '.', ''), ',', '.')
             ELSE replace(TRIM(order_total), '$', '') END,
        'N/A') AS DECIMAL(14,2)) AS order_total,
      COALESCE(CAST(try_strptime(order_ts, {TS}) AS DATE), DATE '{d}') AS order_date,
      DATE '{d}' AS _data_date
    FROM read_csv('{f}',
        header=true,
        columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                 'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                 'payment_method':'VARCHAR','order_total':'VARCHAR',
                 'items':'VARCHAR','meta':'VARCHAR'}})
    """


# Xem thử một ngày, chưa ghi gì cả:
con.sql(f"SELECT * FROM ({staged_select(DATES[0])}) LIMIT 5").show()


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 1a — Đo hiện tượng TRƯỚC khi dựng bất cứ thứ gì.
# ─────────────────────────────────────────────────────────────────────────────
#
# Đừng bao giờ thiết kế chiến lược load cho thứ dữ liệu mà bạn chưa đo.
# Manifest đã đếm sẵn số dòng trễ cho bạn rồi — chỉ việc đọc.

con.sql(f"""
  SELECT sum(rows) AS rows, sum(late_rows) AS late, sum(dup_rows) AS dups
  FROM read_json_auto('{MAN}')
  WHERE CAST(date AS DATE) BETWEEN DATE '2026-06-01' AND DATE '2026-06-10'
""").show()

# KỲ VỌNG (small): rows=577673, late=7750, dups=284
#
# Không có gì bất ngờ ở đây — đó là contract đang giữ lời. Điều khoản
# late_corrections trong contracts/shopcore_orders_daily.contract.yaml hứa đúng
# cái hình dạng này: gửi lại trễ tối đa 7 ngày, "LATEST updated_at WINS", và
# việc dedupe là phần việc của warehouse chứ không phải của producer.
#
# Còn 284 bản trùng trong file là 0,05% của cửa sổ — nằm khá thoải mái dưới
# ngưỡng within_file_exact_resends 0,2%. Tức là nhiễu at-least-once bình thường,
# KHÔNG phải một incident.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 1b — Chứng minh phần chồng lấn là có thật.
# ─────────────────────────────────────────────────────────────────────────────
#
# Manifest nói có dòng trễ. Giờ tự tay đếm: bao nhiêu order_id xuất hiện ở CẢ
# file 06-01 lẫn file 06-02?
#
# Lưu ý về read_csv: tham số `columns` KHÔNG phải để chọn cột — nó khai báo
# schema ĐẦY ĐỦ của file. Khai thiếu là DuckDB báo lệch số cột. Muốn lấy một cột
# thì vẫn khai đủ mười, rồi SELECT ra cột mình cần.

COLS = ("{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
        "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
        "'payment_method':'VARCHAR','order_total':'VARCHAR',"
        "'items':'VARCHAR','meta':'VARCHAR'}")

f_01 = (RAW / "orders_2026-06-01.csv").as_posix()
f_02 = (RAW / "orders_2026-06-02.csv").as_posix()

con.sql(f"""
SELECT count(*) AS overlap_orders FROM (
  SELECT order_id FROM read_csv('{f_01}', header=true, columns={COLS})
  INTERSECT
  SELECT order_id FROM read_csv('{f_02}', header=true, columns={COLS})
)
""").show()

# KỲ VỌNG: 883
#
# INTERSECT tự loại trùng, nên con số này là số ĐƠN HÀNG phân biệt có mặt ở cả
# hai file — không phải số dòng.

con.sql(f"""
  SELECT date, rows, late_rows, dup_rows
  FROM read_json_auto('{(manifest_dir(SCALE) / "orders_2026-06-02.json").as_posix()}')
""").show()

# KỲ VỌNG: late_rows = 885, tức lệch 2 so với con số 883 ở trên.
#
# TODO(journal): viết một câu giải thích chỗ lệch 2.
# Gợi ý: late_rows đếm DÒNG, còn INTERSECT đếm ĐƠN HÀNG phân biệt.
# Một đơn hàng có thể được sửa hai lần trong CÙNG một file không?


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Chiến lược A: append-only log.
# ⚠️ CELL NGUY HIỂM: có CREATE OR REPLACE TABLE. Chạy lại sẽ xoá sạch bảng log
#    và nạp lại từ đầu. Ở scale full việc đó tốn ~20 giây, không mất mát gì,
#    nhưng đừng chạy lại vô ý khi đang so kết quả.
# ─────────────────────────────────────────────────────────────────────────────
#
# Append log là bản sao TRUNG THỰC của những gì đã về. Không bỏ dòng nào, không
# dedupe gì cả. Một đơn hàng đã bị sửa thì nằm ở đây hai lần — đúng như vậy.
#
# Chú ý lệnh DELETE trước mỗi INSERT: đó là bài học A07 đem áp dụng. Chạy lại
# một ngày bất kỳ thì cũng không nạp đôi được.

con.execute(f"""
-- grain: một dòng = một PHIÊN BẢN đơn hàng đã được giao
CREATE OR REPLACE TABLE core.orders_log AS {staged_select(DATES[0])} LIMIT 0
""")

t0 = time.perf_counter()
for d in DATES:
    con.execute(f"DELETE FROM core.orders_log WHERE _data_date = DATE '{d}'")
    con.execute(f"INSERT INTO core.orders_log {staged_select(d)}")
print(f"append: {time.perf_counter() - t0:.1f}s")

con.sql("""
  SELECT count(*) AS n,
         count(*) FILTER (WHERE order_date IS NULL) AS null_dates
  FROM core.orders_log
""").show()

# KỲ VỌNG (small): n = 577673 — ĐÚNG BẰNG tổng manifest ở Task 1a. Không hơn
# không kém. null_dates = 0 (nhờ COALESCE ở staged_select).
# Thời gian: khoảng 3 giây trên máy chuẩn. Ghi lại con số của máy bạn.
#
# Muốn tự kiểm chứng tính idempotent: chạy lại đúng một ngày rồi đếm lại,
# số dòng phải không đổi.
# %%
con.execute(f"DELETE FROM core.orders_log WHERE _data_date = DATE '{DATES[3]}'")
con.execute(f"INSERT INTO core.orders_log {staged_select(DATES[3])}")
con.sql("SELECT count(*) FROM core.orders_log").show()

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 3a — Chiến lược A phần tiếp: view dedupe lúc đọc.
# ⚠️ CREATE OR REPLACE VIEW — nhẹ, chạy lại thoải mái (view không giữ dữ liệu).
# ─────────────────────────────────────────────────────────────────────────────
#
# Đọc to câu query lên: đánh số các phiên bản của mỗi đơn, updated_at mới nhất
# đứng trước, giữ lại dòng số 1. EXCLUDE (rn) bỏ cột phụ trợ ra khỏi SELECT *.
#
# KHOÁ SẮP XẾP THỨ HAI là thứ đáng nói. Nó phá thế hoà: nếu hai phiên bản có
# CÙNG updated_at thì luật "file về trước thắng" khiến lựa chọn trở nên tất định.
# Và không phải ngẫu nhiên, đó đúng là lựa chọn mà MERGE sẽ đưa ra ở Task 4,
# vì điều kiện dấu ">" của nó từ chối các timestamp bằng nhau.
#
# Không phải chuyện giả định: ở scale full, cửa sổ này có ĐÚNG MỘT thế hoà thật
# — đơn 1520054262, cùng updated_at ở hai file mà hai order_total khác nhau.
# Không có luật phá hoà thì hai chiến lược đều "đúng" vẫn lệch nhau một dòng một
# cách hợp lệ, và bạn sẽ mất cả tiếng để truy cái dòng đó.

con.execute("""
-- grain: một dòng = một ĐƠN HÀNG, ở phiên bản mới nhất của nó
CREATE OR REPLACE VIEW core.orders_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *,
         row_number() OVER (PARTITION BY order_id
                            ORDER BY updated_at DESC, _data_date ASC) AS rn
  FROM core.orders_log
) WHERE rn = 1
""")

con.sql("SELECT count(*) AS orders_latest FROM core.orders_latest").show()

# KỲ VỌNG (small): 569639
#
# Để ý hai dòng comment trên DDL của log và của view — chính là quy tắc grain
# bạn giữ từ A03. Chúng mô tả cùng những đơn hàng đó nhưng ở HAI grain khác nhau:
# bên log một dòng = một phiên bản đã giao, bên view một dòng = một đơn hàng.
# Đó là lý do chúng là hai đối tượng chứ không phải một, và cũng là lý do cùng
# một cửa sổ cho ra 577.673 ở cái đầu và 569.639 ở cái sau.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 3b — Đo hiện tượng trên cả mười file, và đo luôn cái giá của view.
# ─────────────────────────────────────────────────────────────────────────────

con.sql("""
  SELECT count(*) AS ids_in_multiple_files FROM (
    SELECT order_id FROM core.orders_log
    GROUP BY 1 HAVING count(DISTINCT _data_date) > 1)
""").show()

# KỲ VỌNG (small): 7684 order_id xuất hiện ở nhiều hơn một file.
# Tức khoảng 1,35% của 569.639 đơn phân biệt — và hơi thấp hơn con số 7.750 dòng
# trễ trong manifest, vì có những đơn bị sửa HAI lần (một đơn, hai dòng trễ).

t0 = time.perf_counter()
con.sql("SELECT count(*) FROM core.orders_log").fetchone()
t_log = time.perf_counter() - t0

t0 = time.perf_counter()
con.sql("SELECT count(*) FROM core.orders_latest").fetchone()
t_view = time.perf_counter() - t0

print(f"count trên log:  {t_log:.3f}s")
print(f"count trên view: {t_view:.3f}s   <- phép dedupe tốn công thật, MỖI LẦN ĐỌC")

# Đó là KHOẢN THUẾ của chiến lược A. Ghi nó vào journal. Ở scale full nó sẽ vào
# khoảng một giây mỗi lần đọc — trả mãi mãi, và mọi người đọc đều trả.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Chiến lược B: upsert bằng MERGE.
# ⚠️ CELL NGUY HIỂM: CREATE OR REPLACE TABLE core.orders_upsert.
# ─────────────────────────────────────────────────────────────────────────────
#
# Hai quy tắc làm cho upsert chạy đúng, và cả hai đều rất dễ quên.
#
# QUY TẮC 1 — mỗi key chỉ một dòng ở phía NGUỒN.
#   Feed này có bản trùng ngay trong file (284 cái trong cửa sổ này). Nếu nguồn
#   có hai dòng cho cùng một order_id thì MERGE của DuckDB KHÔNG BÁO LỖI. Nó
#   lặng lẽ áp dụng một dòng tuỳ tiện, rồi báo chạy thành công. Nên mỗi ngày
#   phải được dedupe ở staging TRƯỚC đã — đó là cái TEMP TABLE stg_day bên dưới.
#
# QUY TẮC 2 — chặn phép update lại.
#   Chỉ ghi đè khi phiên bản đang về mới hơn HẲN. Bỏ điều kiện
#   s.updated_at > t.updated_at thì một bản sửa CŨ sẽ đè lên dòng mới hơn, và
#   bạn biến "phiên bản mới nhất thắng" thành "file về sau thắng". Số dòng không
#   đổi, nên chỉ có diff nội dung mới bắt được (~1.860 dòng cũ trong im lặng).
#
# Cú pháp: WHEN MATCHED … THEN UPDATE mà không kèm danh sách SET thì update toàn
# bộ cột theo tên; lệnh INSERT trần cũng vậy.

con.execute("""
-- grain: một dòng = một đơn hàng ở phiên bản mới nhất, CÙNG grain với orders_latest
CREATE OR REPLACE TABLE core.orders_upsert AS SELECT * FROM core.orders_log LIMIT 0
""")

t0 = time.perf_counter()
written = 0
for d in DATES:
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE stg_day AS
      SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id
                                     ORDER BY updated_at DESC) AS rn
        FROM ({staged_select(d)})
      ) WHERE rn = 1
    """)
    written += con.execute("""
      MERGE INTO core.orders_upsert t USING stg_day s ON t.order_id = s.order_id
      WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE
      WHEN NOT MATCHED THEN INSERT
    """).fetchone()[0]
print(f"upsert: {time.perf_counter() - t0:.1f}s, rows written {written}")

con.sql("SELECT count(*) AS orders_upsert FROM core.orders_upsert").show()

# KỲ VỌNG (small): written = 575494, count = 569639 (bằng đúng số của view).
#
# Làm phép tính này vào journal:
#   577.673 dòng về
#   −   284 gộp lại vì trùng trong file
#   → còn khoảng 1.900 dòng nguồn KHÔNG tạo ra lần ghi nào cả.
#
# Đó là gì? Là các bản sửa ĐÃ CŨ: chúng về ở file sau nhưng mang updated_at cũ
# hơn thứ bảng đang giữ. Điều kiện chặn đã từ chối chúng. Đúng vậy, feed này làm
# thế thật — ở Task 7 bạn sẽ gặp một cái tận mắt.
#
# Tự kiểm chứng idempotent: chạy lại MERGE của một ngày → phải báo 0 dòng ghi.
# %%
d = DATES[3]
con.execute(f"""
  CREATE OR REPLACE TEMP TABLE stg_day AS
  SELECT * EXCLUDE (rn) FROM (
    SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM ({staged_select(d)})
  ) WHERE rn = 1
""")
print(con.execute("""
  MERGE INTO core.orders_upsert t USING stg_day s ON t.order_id = s.order_id
  WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE
  WHEN NOT MATCHED THEN INSERT
""").fetchone()[0])

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — Chiến lược C: refresh cả partition (delete + insert theo ngày).
# ⚠️ CELL NGUY HIỂM: CREATE OR REPLACE TABLE core.orders_refresh.
# ─────────────────────────────────────────────────────────────────────────────
#
# Không đụng tới từng dòng. File D về thì TÍNH LẠI nguyên các partition [D−7, D]
# từ log. Vì sao lại là 7? Từ contract A06: late_corrections hứa bản sửa về trễ
# tối đa 7 ngày. Cửa sổ refresh phải phủ hết mọi partition mà một phiên bản của
# đơn đang về có thể nằm trong đó. Không có cam kết bằng văn bản thì bạn chỉ đoán.
#
# HAI CHI TIẾT trong câu INSERT đáng đọc kỹ:
#
# (a) `_data_date <= D` là điều kiện TÍNH-TỚI-NGÀY (as-of). Log của bạn đã chứa
#     sẵn cả 10 file, nhưng vào ngày D thì pipeline mới chỉ THẤY các file tới D.
#     Bộ lọc này tái hiện lịch sử một cách trung thực. Trong pipeline chạy thật
#     thì điều đó tự đúng; ở đây bạn phải tự viết, vì đang mô phỏng quá khứ.
#
# (b) HÌNH DẠNG câu query: dedupe trên TOÀN BỘ log as-of TRƯỚC, rồi mới giữ lại
#     các bản thắng nằm trong cửa sổ. Chọn bản thắng toàn cục, đặt vào cục bộ.
#     Cái "tối ưu" nghe rất hợp lý — lọc về cửa sổ rồi mới dedupe — sai một cách
#     tinh vi: bản thắng thật của một đơn có thể nằm NGOÀI cửa sổ, và dedupe cục
#     bộ sẽ làm sống lại một phiên bản đã cũ.
#     Chỗ khó chịu: ở small hai cách viết khớp nhau hoàn hảo; ở full thì cách sai
#     để lại 7 đơn bị trùng. Phát triển trên small được, nhưng ĐỪNG NGHIỆM THU
#     trên small.
#
# Và cả DELETE lẫn INSERT nằm trong MỘT transaction — một lần crash ở giữa không
# được để lại lỗ hổng. Đó là bài học A07.

con.execute("""
-- grain: một dòng = một đơn hàng, ở phiên bản mới nhất tính tới ngày refresh cuối
CREATE OR REPLACE TABLE core.orders_refresh AS SELECT * FROM core.orders_log LIMIT 0
""")

t0 = time.perf_counter()
deleted = written = 0
for d in DATES:
    lo = max(START, d - dt.timedelta(days=7))
    con.execute("BEGIN")
    deleted += con.execute(
        "DELETE FROM core.orders_refresh WHERE order_date BETWEEN ? AND ?",
        [lo, d]).fetchone()[0]
    written += con.execute(f"""
      INSERT INTO core.orders_refresh
      SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id
                                     ORDER BY updated_at DESC, _data_date ASC) AS rn
        FROM core.orders_log
        WHERE _data_date <= DATE '{d}'
      ) WHERE rn = 1 AND order_date BETWEEN DATE '{lo}' AND DATE '{d}'
    """).fetchone()[0]
    con.execute("COMMIT")
print(f"refresh: {time.perf_counter() - t0:.1f}s, deleted {deleted}, written {written}")

con.sql("SELECT count(*) AS orders_refresh FROM core.orders_refresh").show()

# KỲ VỌNG (small): written ≈ 2.980.000, deleted ≈ 2.410.000, count = 569639.
#
# Bạn nhận vào 577 nghìn dòng và ghi lại GẤP NĂM LẦN chừng đó. Sự bất cân xứng
# đó CHÍNH LÀ chiến lược C: trả bằng I/O hàng loạt, đổi lại được sự đơn giản và
# khả năng chạy trên file bất biến.
#
# Một cái bẫy đáng ghi vào journal: chạy lại ngày MỚI NHẤT thì idempotent hoàn
# hảo. Nhưng chạy lại một ngày D CŨ HƠN sẽ tua cửa sổ [D−7, D] về đúng trạng
# thái của thời điểm D — mọi bản sửa về sau ngày D biến mất, cho tới khi bạn
# chạy lại từ D+1 trở đi. Chạy lại một lần refresh thực chất là một lần backfill
# thu nhỏ: khởi động lại từ D rồi cuốn tới. (A11 sẽ dựng cái runner làm đúng việc này.)


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 6a — Reconcile ba chiều: đếm trước, rồi so NỘI DUNG, cả hai chiều. [CÓ TODO]
# ─────────────────────────────────────────────────────────────────────────────


con.sql("""
SELECT
  (SELECT count(*) FROM core.orders_latest)  AS append_view,
  (SELECT count(*) FROM core.orders_upsert)  AS upsert,
  (SELECT count(*) FROM core.orders_refresh) AS refresh,
  (SELECT count(*) FROM (SELECT * FROM core.orders_latest
                         EXCEPT SELECT * FROM core.orders_upsert))  AS av_minus_up,
  (SELECT count(*) FROM (SELECT * FROM core.orders_upsert
                         EXCEPT SELECT * FROM core.orders_latest))  AS up_minus_av,
  (SELECT count(*) FROM (SELECT * FROM core.orders_latest
                         EXCEPT SELECT * FROM core.orders_refresh)) AS av_minus_rf,
  (SELECT count(*) FROM (SELECT * FROM core.orders_refresh
                         EXCEPT SELECT * FROM core.orders_latest)) AS rf_minus_av
""").show()

# KỲ VỌNG: cả ba con số bằng 569639, và cả BỐN phép diff bằng 0.
#
# Vì sao phải làm EXCEPT theo CẢ HAI CHIỀU? Vì A trừ B bằng 0 chỉ nói A là tập
# con của B. Muốn kết luận hai bảng bằng nhau thì cần cả hai chiều.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 6b — Giờ tới phần rút ra bài học: chạy một cửa sổ SAI có chủ đích. [CÓ TODO]
# ⚠️ Tạo bảng nháp core.orders_refresh_bad. Nhớ DROP ở cuối cell.
# ─────────────────────────────────────────────────────────────────────────────
#
# Chép vòng lặp Task 5 sang bảng core.orders_refresh_bad, nhưng dùng cửa sổ ngây
# thơ [D, D]: chỉ xoá đúng ngày D, chỉ insert những dòng có order_date = D.
# Rồi reconcile nó với view.
#
# KỲ VỌNG (small): count ra 569640 — LỆCH ĐÚNG MỘT. Con số chẳng ai để ý bao giờ.
# Nhưng diff nội dung sẽ cho thấy 5.824 DÒNG SAI: mọi đơn hàng mà phiên bản hiện
# hành là một bản sửa đều bị kẹt ở bản gốc đã cũ, vì bản sửa rơi vào một partition
# cũ hơn mà cửa sổ hẹp không hề chạm tới.
#
# Đếm KHÔNG PHẢI là reconcile. Luôn diff nội dung.

# TODO(bạn): copy vòng lặp Task 5 xuống đây, đổi hai chỗ:
#   - bỏ dòng `lo = max(START, d - dt.timedelta(days=7))`, dùng lo = d
#   - đổi tên bảng thành core.orders_refresh_bad
# Rồi chạy:
#   count(*) trên bảng _bad
#   count(*) của (orders_latest EXCEPT orders_refresh_bad)

# con.execute("DROP TABLE IF EXISTS core.orders_refresh_bad")   # dọn khi xong
con.execute("DROP TABLE IF EXISTS core.orders_refresh_bad")
con.execute("CREATE TABLE core.orders_refresh_bad AS SELECT * FROM core.orders_log LIMIT 0")

for d in DATES:
    con.execute("BEGIN")
    con.execute("DELETE FROM core.orders_refresh_bad WHERE order_date = ?", [d])
    con.execute(f"""
        INSERT INTO core.orders_refresh_bad
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC, _data_date ASC) AS rn
            FROM core.orders_log
            WHERE _data_date <= DATE '{d}'
        ) WHERE rn = 1 AND order_date = DATE '{d}'
    """)
    con.execute("COMMIT")

bad = con.execute("SELECT count(*) FROM core.orders_refresh_bad").fetchone()[0]
wrong = con.execute("""
    SELECT count(*) FROM (SELECT * FROM core.orders_latest
                          EXCEPT SELECT * FROM core.orders_refresh_bad)
""").fetchone()[0]
print(f"count = {bad}   (đúng phải là 569639)")
print(f"dòng SAI nội dung = {wrong}")

# %%
con.execute("DROP TABLE IF EXISTS core.orders_refresh_bad")


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 7 — Truy vết MỘT bản sửa từ đầu tới cuối.
# ─────────────────────────────────────────────────────────────────────────────
#
# Con số thuyết phục được kỹ sư; một dòng được truy vết thì thuyết phục được tất
# cả những người còn lại. Task 6 đã chứng minh ba chiến lược đồng thuận trên
# 569.639 dòng. Giờ nhìn MỘT dòng và xem sự đồng thuận đó hình thành thế nào.

# ── 7.1 Tìm đơn bị sửa nhiều nhất ────────────────────────────────────────────
con.sql("""
SELECT order_id, count(*) AS versions, count(DISTINCT _data_date) AS files
FROM core.orders_log
GROUP BY 1 HAVING count(DISTINCT _data_date) > 1
ORDER BY versions DESC, order_id LIMIT 3
""").show()

# Ứng viên đứng đầu ở small: 1510000111.
# Cách đánh id có mã hoá ngày: số thứ tự ngày 151 = 2026-06-01, sequence 111.
# Ba phiên bản trải trên ba file.

# ── 7.2 Lịch sử đầy đủ từ log ────────────────────────────────────────────────
# Sắp xếp theo updated_at, KHÔNG phải theo _data_date. Đó là toàn bộ chỗ hay.
con.sql("""
SELECT _data_date, status, order_total, updated_at
FROM core.orders_log WHERE order_id = 1510000111 ORDER BY updated_at
""").show()

# KỲ VỌNG — nhìn KỸ cột updated_at, đối chiếu với cột _data_date:
#   _data_date | status    | order_total | updated_at
#   2026-06-02 | refunded  |      435.92 | 2026-06-02 00:10:35   <- bản sửa đã THUA
#   2026-06-01 | delivered |       84.48 | 2026-06-02 20:27:19   <- bản gốc
#   2026-06-06 | refunded  |      230.65 | 2026-06-06 07:53:51   <- bản thắng
#
# Đọc chậm hai dòng đầu: bản nằm trong file 06-02 mang updated_at SỚM HƠN bản
# nằm trong file 06-01. File về sau, nhưng nội dung lại cũ hơn.
#
# Đó chính xác là loại dòng đã tạo nên con số ~1.895 "không ghi gì" ở Task 4:
# điều kiện s.updated_at > t.updated_at đã từ chối nó. Nếu bỏ điều kiện chặn,
# đơn này sẽ dừng ở refunded/435.92 — sai, mà count vẫn không đổi một dòng nào.
#
# Bài học: THỨ TỰ FILE KHÔNG CÓ Ý NGHĨA GÌ. Chỉ updated_at mới xếp hạng được.

# ── 7.3 Trạng thái cuối ở cả ba chiến lược ───────────────────────────────────
con.sql("""
SELECT 'A: view (read-time)' AS strategy, order_id, status, order_total, updated_at
FROM core.orders_latest  WHERE order_id = 1510000111
UNION ALL
SELECT 'B: upsert (merge)',  order_id, status, order_total, updated_at
FROM core.orders_upsert  WHERE order_id = 1510000111
UNION ALL
SELECT 'C: refresh (window)', order_id, status, order_total, updated_at
FROM core.orders_refresh WHERE order_id = 1510000111
""").show()

# KỲ VỌNG: ba dòng giống hệt nhau — refunded / 230.65 / 2026-06-06 07:53:51.
#
# Ba cơ chế khác nhau hoàn toàn cùng chọn ra một bản thắng:
#   A — row_number() ORDER BY updated_at DESC, giữ rn = 1
#   B — MERGE chỉ update khi s.updated_at > t.updated_at
#   C — dedupe toàn cục trên log as-of, rồi lọc theo cửa sổ
# Đó không phải trùng hợp: cả ba đang thực thi cùng một điều khoản contract,
# "LATEST updated_at WINS".

# ── 7.4 Câu chuyện nghiệp vụ ─────────────────────────────────────────────────
# Kể lại đơn hàng này bằng ngôn ngữ người thường, và tách bạch hai mốc thời gian:
# chuyện XẢY RA lúc nào, và chúng ta BIẾT chuyện đó lúc nào.
#
#   01/06  đơn được đặt, giao thành công, 84,48.
#   02/06  hệ thống nguồn gửi một bản ghi hoàn tiền 435,92 — nhưng bản ghi này
#          mang dấu thời gian sớm hơn chính bản gốc, tức là một trạng thái đã
#          lỗi thời được gửi lại. Warehouse từ chối, đúng.
#   06/06  hoàn tiền thật, 230,65. Đây là trạng thái hiện hành.
#
# Nhìn từ phía kinh doanh: đơn 01/06 chỉ được biết là đã hoàn tiền vào ngày
# 06/06 — trễ năm ngày. Một báo cáo doanh thu chốt ngày 02/06 sẽ ghi nhận đơn
# này là bán được, và con số đó KHÔNG SAI ở thời điểm chốt. Nó chỉ lỗi thời.
# Đó là lý do append log đáng giữ: nó là thứ duy nhất trả lời được câu
# "chúng ta biết điều này từ khi nào".

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 8 — Chuyển sang scale FULL.
# ⚠️ Chạy lâu: ~10 file, ~3,5 GB CSV, ~11,6 triệu dòng. Mở Task Manager trước.
# ─────────────────────────────────────────────────────────────────────────────
#
# Cách làm: đóng kết nối hiện tại, đổi SCALE ở CELL 1 thành "full", chạy lại
# CELL 1, CELL 2, rồi Task 2 → Task 6. Thí nghiệm cửa sổ sai (6b) không bắt buộc.
#
# Nhớ: warehouse full là MỘT FILE KHÁC (data_root("full") không có thư mục con
# "full/"), có ops.run_id_seq riêng. Không có gì bị đè lên nhau.

con.close()
print("đã đóng kết nối small — giờ đổi SCALE ở CELL 1 rồi chạy lại từ đầu")

# KỲ VỌNG ở full, trên máy chuẩn:
#   append  ~20 giây,  11.553.524 dòng ghi
#   upsert  ~1 phút,   11.509.661 dòng ghi
#   refresh ~1 phút,   ~59,6 triệu dòng ghi để giao 11.392.663 dòng
#   view dedupe: ~1 giây mỗi lần đọc  <- thuế của chiến lược A khi lên quy mô
#   ba chiến lược đồng thuận ở 11.392.663
#   153.678 order_id (1,35%) xuất hiện ở nhiều hơn một file
#
# Trong lúc nó chạy, nhìn Task Manager và thư mục <DATA_ROOT>/tmp. Vòng lặp
# refresh ghi lại hàng triệu dòng mỗi ngày — lúc này sự bất cân xứng khi ghi trở
# thành thứ bạn CẢM NHẬN được.
#
# Nếu số dòng log ở full lệch với tổng manifest thì có một ngày load hỏng: tìm
# nó bằng GROUP BY _data_date TRƯỚC khi đụng vào bất cứ thứ gì khác.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 8b — Điền bảng, rồi VIẾT RA quyết định.  [journal]
# ─────────────────────────────────────────────────────────────────────────────
#
#                 | dòng ghi (small/full) | thời gian | chi phí đọc    | giữ lịch sử | chạy trên file bất biến | hợp khi…
# append + view   | 577.673 / 11.553.524  |   ?/?     | dedupe mỗi lần | đầy đủ      | được                    | event bất biến, cần audit
# upsert          | 575.494 / 11.509.661  |   ?/?     | không          | mất         | không                   | thực thể đổi được, trong DB
# refresh         | ~2,98M / ~59,6M       |   ?/?     | không          | mất         | ĐƯỢC — cách duy nhất    | file lake, độ trễ có giới hạn
#
# Rồi quyết định, bằng văn bản: pipeline CỦA CHÚNG TA chọn chiến lược nào cho
# core.orders, và vì sao?
#
# Câu trả lời của lab: giữ append log làm NGUỒN BỀN VỮNG — nó là thứ duy nhất
# trả lời được "chúng ta biết chuyện này từ lúc nào" — rồi phục vụ core.orders
# bằng cách refresh partition từ log đó. Vẫn là transaction của A07, nhưng giờ
# với cửa sổ [D−7, D] cho đúng. Cách này cho cả lịch sử lẫn đọc rẻ, đổi lại phải
# chịu chi phí ghi lại hàng loạt.
#
# A09 sẽ lo an toàn cho người đọc TRONG LÚC ghi lại (hiện tại vẫn có khoảng thời
# gian bảng bị ghi dở dang). A15 cho thấy dbt đóng gói đúng chiến lược này thành
# ba dòng cấu hình: incremental strategy kiểu delete+insert của nó chính là C.
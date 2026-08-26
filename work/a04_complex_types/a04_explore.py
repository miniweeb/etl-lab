# %% work/a04_complex_types/a04_explore.py
"""A04 — Complex types: JSON trong ô CSV, đổi grain có chủ đích, mart đầu tiên.

Task 1-7 và 12 ở SCALE small; Task 8 cuối và Task 9 dùng full.
"""
import os
while not os.path.exists("lib") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print(os.getcwd())

# %%
import time

import duckdb

from lib.labpaths import data_root, warehouse_path

SCALE = "full"                 # Task 8 cuối bài đổi "full"
DAY = "2026-06-02"              # v1 day nào cũng được; số verify tính cho ngày này
ROOT = data_root(SCALE)
R = ROOT.as_posix()
RAW = f"{R}/raw/orders/orders_{DAY}.csv"
PRODUCTS = f"{R}/raw/dims/products.csv"

V1_COLS = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
     'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
     'payment_method':'VARCHAR','order_total':'VARCHAR',
     'items':'VARCHAR','meta':'VARCHAR'}"""

# struct đích cho items — dùng lại ở Task 2, 3, 9
ITEM_STRUCT = "STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[]"


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{R}/tmp';")
print(f"scale={SCALE}  day={DAY}  root={R}")

# %% ── Task 1 — nhìn vào chuỗi thô trước ──────────────────────────────
section("TASK 1 — items là gì, meta là gì?")

# Bảng ngày này CỐ Ý không khử trùng, để mọi con số hôm nay khớp manifest.
# TEMP: sống trong kết nối, không nằm ở staging dùng chung, và vì thế
# không mang 2 cột lineage — bảng thiếu chúng không có tư cách ngồi trong staging.
con.execute(f"""
CREATE OR REPLACE TEMP TABLE shopcore_orders_day AS
SELECT order_id,
       try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                               '%Y-%m-%dT%H:%M:%S']) AS order_ts,
       items, meta
FROM read_csv('{RAW}', header=true, columns={V1_COLS});
""")

con.sql("SELECT items, meta FROM shopcore_orders_day LIMIT 5").show()
con.sql("SELECT count(*) AS rows FROM shopcore_orders_day").show()   # 59.024 = manifest

# %% ── Task 2 — parse items: chặt và lỏng ─────────────────────────────
section("TASK 2a — hai cách, cùng hình dạng kết quả")

con.sql(f"""
    SELECT CAST(items AS JSON)::{ITEM_STRUCT} AS item_list
    FROM shopcore_orders_day LIMIT 5
""").show()

con.sql("""
    SELECT json_transform(items,
           '[{"sku":"VARCHAR","qty":"INTEGER","unit_price":"DOUBLE"}]') AS item_list
    FROM shopcore_orders_day LIMIT 5
""").show()

# %%
section("TASK 2b — dò rác: qty ghi dạng CHUỖI JSON")

con.sql("""
    SELECT count(*) AS string_typed_qty
    FROM shopcore_orders_day WHERE json_type(items, '$[0].qty') = 'VARCHAR'
""").show()          # 161 (~0,3%)

con.sql(f"""
    SELECT count(*) AS lists_with_null_qty FROM (
        SELECT list_filter(CAST(items AS JSON)::{ITEM_STRUCT},
                           x -> x.qty IS NULL) AS bad
        FROM shopcore_orders_day
    ) WHERE length(bad) > 0
""").show()          # 0 — phép ép kiểu ÂM THẦM SỬA "2" thành 2
# Đó là KHOẢNG TRỐNG hợp đồng (kiểu bên trong JSON chưa ghim), không phải vi phạm.
# json_type ở trên là dấu vết kiểm toán của bạn: đếm được thứ mà cast đã sửa.

# %% ── Task 3 — unnest: đổi grain ─────────────────────────────────────
section("TASK 3 — core.order_items, order grain -> item grain")

con.execute(f"""
-- grain: one row = one line item of one delivered order *version*
-- (bảng ngày cố ý chưa khử trùng, nên (order_id, line_no) CHƯA duy nhất ở đây;
--  một dòng một đơn hàng là chuyện của A08)
CREATE OR REPLACE TABLE core.order_items AS
SELECT
    order_id,
    generate_subscripts(item_list, 1) AS line_no,   -- 1,2,3... trong mỗi đơn
    unnest(item_list, recursive := true),           -- bung struct thành CỘT
    DATE '{DAY}'         AS _data_date,             -- lineage: cha là TEMP nên
    CAST(NULL AS BIGINT) AS _run_id                 -- không có gì để thừa kế
FROM (
    SELECT order_id, CAST(items AS JSON)::{ITEM_STRUCT} AS item_list
    FROM shopcore_orders_day
);
""")

con.sql("DESCRIBE core.order_items").show()      # 7 cột, không còn số dạng VARCHAR

# %%
section("TASK 3b — PHÉP KIỂM TRA BẢO TOÀN")

con.sql("""
    SELECT (SELECT count(*) FROM core.order_items)                    AS item_rows,
           (SELECT sum(json_array_length(items))
              FROM shopcore_orders_day)                               AS expected
""").show()      # phải bằng nhau: 149.190
# Sau MỌI lần đổi grain: chứng minh không dòng nào tự sinh ra hay biến mất.
# Cùng kỷ luật với cleaning report của A03, áp cho biến đổi CẤU TRÚC.

# %% ── Task 4 — làm quen grain mới ────────────────────────────────────
section("TASK 4 — phân bố")

print("-- Phân bố số lượng mặt hàng (items) per order --")
con.sql("""
    SELECT json_array_length(items) AS n_items, count(*) AS orders,
           round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
    FROM shopcore_orders_day GROUP BY 1 ORDER BY 1
""").show()      # 1-6 mặt hàng, ~30% ở mức 1

print("-- Phân bố số lượng đơn vị sản phẩm (qty) per line item --")
con.sql("""
    SELECT qty, count(*) AS n_items,
           round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
    FROM core.order_items
    GROUP BY 1 ORDER BY 1
""").show()      # 1-5, ~55% ở mức 1

con.sql("""
    SELECT round(count(*) * 1.0 /
           (SELECT count(*) FROM shopcore_orders_day), 2) AS avg_items_per_order
    FROM core.order_items
""").show()      # ~2.53 — Task 9 cần con số này

# %% ── Task 5 — orphan sku ────────────────────────────────────────────
section("TASK 5 — sku bán ra mà chưa có trong bảng chiều")

con.sql(f"""
    SELECT count(*) AS orphan_lines,
           round(100.0 * count(*) /
                 (SELECT count(*) FROM core.order_items), 2) AS pct
    FROM core.order_items i
    LEFT JOIN read_csv('{PRODUCTS}', header=true) p USING (sku)
    WHERE p.sku IS NULL
""").show()      # 310 dòng (0,21%)
# LEFT chứ không INNER: INNER sẽ âm thầm LOẠI chúng, doanh thu co lại, không ai hay.
# Hôm nay chỉ ĐO và BÁO CÁO — cách ly là việc của A05.

# %% ── Task 6 — meta: ba kiểu "không có giá trị" ──────────────────────
section("TASK 6a — device")

con.sql("""
    SELECT CAST(meta AS JSON)->>'device' AS device, count(*) AS n
    FROM shopcore_orders_day GROUP BY 1 ORDER BY 2 DESC
""").show()      # android 20.399 · ios 17.863 · web 14.850 · desktop 5.912

# %%
section("TASK 6b — utm: sentinel / json null / vắng mặt")

con.sql("""
    SELECT
      count(*) FILTER (WHERE CAST(meta AS JSON)->>'utm' = 'none') AS sentinel_none,
      count(*) FILTER (WHERE json_type(meta, '$.utm') = 'NULL')   AS json_null,
      count(*) FILTER (WHERE json_extract(meta, '$.utm') IS NULL) AS key_absent,
      count(*) FILTER (WHERE CAST(meta AS JSON)->>'utm' IS NULL)  AS arrow_null,
      count(*)                                                    AS total_rows
    FROM shopcore_orders_day
""").show()
# 5.930 / 5.981 / 5.945 / 11.926 / 59.024
# arrow_null = json_null + key_absent: ->> GỘP hai kiểu, nhưng KHÔNG gộp 'none'.
#   json_extract trả NULL của SQL CHỈ khi khoá vắng mặt.
#   json_type nói 'NULL' khi khoá chứa null của JSON (KHÔNG phải NULL của SQL).

con.sql("""
    SELECT NULLIF(CAST(meta AS JSON)->>'utm', 'none') AS utm, count(*) AS n
    FROM shopcore_orders_day GROUP BY 1 ORDER BY 2 DESC
""").show()      # 4 nguồn thật + NULL — gấp cả ba kiểu thành một NULL trung thực

con.sql("""
    SELECT
      -- Gom cả 3 trường hợp (sentinel_none, json_null, key_absent) về 1 chỉ số
      count(*) FILTER (WHERE NULLIF(CAST(meta AS JSON)->>'utm', 'none') IS NULL)     AS total_invalid_or_missing_utm,
      
      -- Chỉ đếm các UTM hợp lệ (facebook, email, google, ...)
      count(*) FILTER (WHERE NULLIF(CAST(meta AS JSON)->>'utm', 'none') IS NOT NULL) AS valid_utm,
      
      count(*) AS total_rows
    FROM shopcore_orders_day
""").show()
# %%
section("TASK 6c — chặt HỎNG ở đây, lỏng thì không")

try:
    con.sql("""
        SELECT CAST(meta AS JSON)::STRUCT(device VARCHAR, utm VARCHAR, sess VARCHAR)
        FROM shopcore_orders_day LIMIT 5
    """).show()
except Exception as e:
    print("CHẶT HỎNG:", e)      # Object {...} does not have key "utm"

con.sql("""
    SELECT json_transform(meta,
           '{"device":"VARCHAR","utm":"VARCHAR","sess":"VARCHAR"}') AS m
    FROM shopcore_orders_day LIMIT 5
""").show()      # khoá thiếu -> trường NULL, không kịch tính
# Đánh đổi: CHẶT cho items (hình dạng cố định, muốn hỏng ồn ào),
#           LỎNG cho meta (~10% dòng hợp lệ mà không có utm).

# %% ── Task 7 — products: pipe-list và JSON object ────────────────────
section("TASK 7a — tags ngăn bằng dấu gạch đứng")

con.sql(f"""
    SELECT tag, count(*) AS products FROM (
      SELECT unnest(string_split(tags, '|')) AS tag
      FROM read_csv('{PRODUCTS}', header=true)
    ) GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").show()
# LUẬT: unnest trong truy vấn CON, GROUP BY ở NGOÀI. DuckDB không cho chung SELECT.

# %%
section("TASK 7b — attrs là JSON object")

# 1. Trích xuất thuộc tính: brand, weight_g (kiểu INT), color
con.sql(f"""
    SELECT 
        attrs->>'brand' AS brand,
        TRY_CAST(attrs->>'weight_g' AS INTEGER) AS weight_g,
        attrs->>'color' AS color
    FROM read_csv('{PRODUCTS}', header=true)
""").show()

# 2. Bảng đếm số lượng sản phẩm theo Thương hiệu (Brand Count Table)
con.sql(f"""
    SELECT 
        attrs->>'brand' AS brand,
        count(*) AS n_products
    FROM read_csv('{PRODUCTS}', header=true)
    WHERE attrs->>'brand' IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
""").show()
# TODO: trích brand, weight_g, color bằng công cụ Task 6.
#       -> trả JSON, ->> trả VARCHAR: weight_g phải ra INTEGER, không phải text.
#       Rồi trả lời: thương hiệu nào nhiều sản phẩm nhất?

# %% ── Task 8 — mart đầu tiên ─────────────────────────────────────────
section("TASK 8 — marts.category_daily_revenue")

con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
con.execute(f"""
CREATE OR REPLACE TABLE marts.category_daily_revenue AS
SELECT
    CAST(s.order_ts AS DATE)              AS order_date,
    COALESCE(p.category, '(unknown sku)') AS category,
    round(sum(i.qty * i.unit_price), 2)   AS revenue,
    sum(i.qty)                            AS units,
    count(DISTINCT i.order_id)            AS orders
FROM core.order_items i
JOIN shopcore_orders_day s USING (order_id)
LEFT JOIN read_csv('{PRODUCTS}', header=true) p USING (sku)
GROUP BY 1, 2;
""")

con.sql("""
    SELECT order_date, count(*) AS categories, round(sum(revenue)) AS revenue
    FROM marts.category_daily_revenue GROUP BY 1 ORDER BY 1
""").show()
# BA nhóm ngày từ MỘT file:
#   2026-06-01  <- ~1,5% bản sửa trễ (đúng thứ đã thấy trong lake A02)
#   2026-06-02  <- ngày của chính file
#   NULL        <- ~0,02% mốc thời gian bất khả thi, A03 đã cho về NULL
# Một file != một ngày. Đó là toàn bộ cốt truyện của A08.

con.sql("""
    SELECT category, round(revenue) AS revenue FROM marts.category_daily_revenue
    WHERE order_date = DATE '2026-06-02' ORDER BY revenue DESC LIMIT 5
""").show()      # office dẫn đầu, ~734k

# %% ── Task 9 — thăm dò quy mô ────────────────────────────────────────
section("TASK 9 — phần A: dòng chảy")

spike = (data_root("full") / "raw" / "orders" / "orders_2026-06-19.csv").as_posix()

t0 = time.perf_counter()
con.sql(f"""
    SELECT count(*) AS item_rows FROM (
        SELECT unnest(CAST(items AS JSON)::{ITEM_STRUCT})
        FROM read_csv('{spike}', header=true, columns={V1_COLS})
    )
""").show()
print(f"stream: {time.perf_counter() - t0:.1f}s")
# 9.214.235 dòng. Nhìn Task Manager: bộ nhớ gần như không nhúc nhích.
# DuckDB đọc một khối, đếm, vứt đi.

# %%
section("TASK 9 — phần B: vật chất hoá")

t0 = time.perf_counter()
con.execute(f"""
CREATE OR REPLACE TEMP TABLE spike_items AS
SELECT order_id,
       generate_subscripts(item_list, 1) AS line_no,
       unnest(item_list, recursive := true)
FROM (
    SELECT order_id, CAST(items AS JSON)::{ITEM_STRUCT} AS item_list
    FROM read_csv('{spike}', header=true, columns={V1_COLS})
);
""")
print(f"materialize: {time.perf_counter() - t0:.1f}s")
con.sql("SELECT count(*) FROM spike_items").show()   # vẫn 9.214.235

# Cùng số dòng, nhưng giờ chúng phải SỐNG ở đâu đó: thời gian gấp đôi, RAM tăng rõ.
#
# NGOẠI SUY (viết vào journal, đừng chạy):
#   1 ngày cao điểm vật chất hoá  ~9,2M dòng, gần trọn 1 GB
#   cả lịch sử 69 ngày            ~82M đơn x ~2,5 = ~200M dòng mặt hàng
#                                 = hơn 20 ngày cao điểm CÙNG LÚC
#   + bộ nhớ làm việc cho sort/join chồng lên
#   -> vượt xa memory_limit 8GB, DuckDB tràn ra temp_directory và chậm dần
# Đó là lý do pipeline thật xử lý THEO TỪNG PHÂN VÙNG — mỗi lần một ngày,
# như bộ nạp A02 — và là lý do A12 song song hoá các đơn vị ngày đó.

con.execute("DROP TABLE spike_items")     # trả RAM lại

# %% ── Task 8 ở quy mô đầy đủ ─────────────────────────────────────────
# Đổi SCALE = "full" ở cell 2, restart kernel, chạy lại từ Task 1.
# Dự kiến: vài giây tới một phút — DuckDB phân tích JSON song song.
# ~1,18M đơn, ~3M dòng mặt hàng (gấp ~20 lần).
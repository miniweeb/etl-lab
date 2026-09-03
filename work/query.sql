-- ============================================================================
-- A01: FIRST CONTACT
-- ============================================================================

-- ============================================================================
-- TASK 7 — Mở nắp file Parquet
-- parquet_metadata() đọc METADATA, không chạm data page.
-- ============================================================================

-- Row groups. column_id = 0 để mỗi row group chỉ trả 1 dòng
-- (không lọc thì ra 10 row group x 10 cột = 100 dòng).
SELECT row_group_id, row_group_num_rows
FROM parquet_metadata('D:/etl_lab/tmp/orders_2026-06-02.parquet')
WHERE column_id = 0;

-- Statistics: mỗi row group mang min/max từng cột -> engine bỏ qua được
-- nguyên khối không thể khớp filter. Không đọc là IO nhanh nhất.
-- Để ý total_compressed_size: items áp đảo mọi cột khác.
SELECT path_in_schema, stats_min_value, stats_max_value, total_compressed_size
FROM parquet_metadata('D:/etl_lab/tmp/orders_2026-06-02.parquet')
WHERE row_group_id = 0
  AND path_in_schema IN ('order_id', 'status', 'items');

-- Toàn cảnh: cột nào tốn dung lượng nhất trên cả file
SELECT path_in_schema,
       sum(total_compressed_size)   AS compressed_bytes,
       sum(total_uncompressed_size) AS uncompressed_bytes
FROM parquet_metadata('D:/etl_lab/tmp/orders_2026-06-02.parquet')
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================================================
-- STRETCH GOALS
-- ============================================================================

-- 1. Đếm cả tháng trong 1 query. filename=true thêm cột tên file.
--    Thấy dip cuối tuần (~x0.75) và spike 2026-06-19 (~x3).
SELECT parse_filename(filename) AS file, count(*) AS rows
FROM read_csv('D:/etl_lab/small/raw/orders/orders_*.csv',
              filename = true, all_varchar = true)
GROUP BY 1
ORDER BY 1;

-- 2. So codec nén. zstd nhỏ hơn snappy đáng kể — đánh đổi là gì? (A14 đo)
COPY (SELECT * FROM orders_full)
TO 'D:/etl_lab/tmp/orders_2026-06-02_zstd.parquet' (FORMAT parquet, COMPRESSION zstd);

-- 3. Đo thêm dạng bẩn khác
SELECT status, count(*) FROM orders_full GROUP BY 1 ORDER BY 2 DESC;
SELECT payment_method, count(*) FROM orders_full GROUP BY 1 ORDER BY 2 DESC;

SELECT
    count(*) FILTER (WHERE order_total LIKE '$%')  AS has_currency_symbol,
    count(*) FILTER (WHERE order_total LIKE '%,%') AS has_eu_decimal_comma,
    count(*) FILTER (WHERE order_total = 'N/A')    AS literal_na
FROM orders_full;

-- ============================================================================
-- A03: TRANSFORMATION — TASK 6: làm sạch bảng chiều vào core
-- ============================================================================
-- Đường dẫn giả định SCALE = small. Đổi 'D:/etl_lab/small' -> 'D:/etl_lab'
-- khi chạy full.

-- ----------------------------------------------------------------------------
-- 6.0 — KHÁM PHÁ TRƯỚC KHI VIẾT CODE
-- customers.csv là bảng bẩn nhất tính theo đầu dòng trong cả lab.
-- Chạy hai câu này và ĐỌC kết quả trước khi viết bảng ánh xạ.
-- ----------------------------------------------------------------------------

-- 24 cách viết cho 10 quốc gia, cộng NULL.
-- Cột variant bên dưới lấy từ chính danh sách này.
SELECT country, count(*) AS n
FROM read_csv('D:/etl_lab/small/raw/dims/customers.csv', header = true)
GROUP BY 1
ORDER BY 2 DESC;

-- 11 biến thể true/false, cộng NULL. Đây là lý do phải dùng danh sách trắng
-- tường minh cho cả hai phía thay vì TRY_CAST(... AS BOOLEAN):
-- TRY_CAST chấp nhận 'yes' nhưng không chấp nhận 'N'.
SELECT is_active, count(*) AS n
FROM read_csv('D:/etl_lab/small/raw/dims/customers.csv', header = true)
GROUP BY 1
ORDER BY 2 DESC;

-- signup_ts: MỘT cột chứa BA định dạng. Nhìn tận mắt từng loại.
SELECT signup_ts, count(*) AS n
FROM read_csv('D:/etl_lab/small/raw/dims/customers.csv', header = true)
WHERE signup_ts IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- Đếm theo loại: chuỗi toàn chữ số = epoch giây (số giây kể từ 1970).
SELECT
    count(*) FILTER (WHERE regexp_full_match(signup_ts, '[0-9]+'))  AS epoch_form,
    count(*) FILTER (WHERE signup_ts LIKE '%/%')                    AS ddmmyyyy_form,
    count(*) FILTER (WHERE signup_ts LIKE '____-__-__ %')           AS iso_form,
    count(*) FILTER (WHERE signup_ts IS NULL)                       AS missing
FROM read_csv('D:/etl_lab/small/raw/dims/customers.csv', header = true);

-- ============================================================================
-- A04: COMPLEX DATA TYPES — JSON, STRUCT, LIST
-- Đường dẫn giả định small. Đổi 'D:/etl_lab/small' -> 'D:/etl_lab' khi chạy full.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BA THẾ GIỚI KIỂU DỮ LIỆU
-- VARCHAR  -> database coi là văn bản thuần, không với vào được
-- JSON     -> đã parse, với vào được bằng đường dẫn, nhưng kiểu vẫn LỎNG
-- STRUCT/LIST -> kiểu nguyên bản, lược đồ cố định, Parquet lưu thẳng
-- ----------------------------------------------------------------------------

-- TASK 1 — nhìn chuỗi thô
SELECT items, meta FROM shopcore_orders_day LIMIT 5;
-- items: MẢNG JSON chứa các ĐỐI TƯỢNG, 1-6 cái
-- meta : một ĐỐI TƯỢNG JSON


-- ----------------------------------------------------------------------------
-- TASK 2 — CHẶT vs LỎNG
-- ----------------------------------------------------------------------------

-- CHẶT: thứ gì không vừa kiểu đích -> LỖI, cả truy vấn chết.
-- Tốt vì vấn đề hỏng ồn ào. Đúng cho items: hình dạng cố định.
SELECT CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[]
FROM shopcore_orders_day LIMIT 5;

-- LỎNG: thứ gì không vừa -> NULL, im lặng.
-- Tốt vì sống sót qua thực tế bừa bộn. Dở vì im lặng.
SELECT json_transform(items, '[{"sku":"VARCHAR","qty":"INTEGER","unit_price":"DOUBLE"}]')
FROM shopcore_orders_day LIMIT 5;

-- Dò rác: ~0,3% dòng ghi qty dạng CHUỖI JSON ("qty":"2" thay vì "qty":2)
SELECT count(*) AS string_typed_qty
FROM shopcore_orders_day
WHERE json_type(items, '$[0].qty') = 'VARCHAR';                       -- 161

-- Nhưng phép ép kiểu chặt vẫn sống sót — nó ÂM THẦM ép "2" thành 2.
-- Đếm list có qty NULL sau khi cast: ra 0.
SELECT count(*) AS lists_with_null_qty FROM (
    SELECT list_filter(
        CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[],
        x -> x.qty IS NULL) AS bad
    FROM shopcore_orders_day
) WHERE length(bad) > 0;                                              -- 0
-- Tiện, nhưng cast đã SỬA dữ liệu. Câu json_type ở trên là dấu vết kiểm toán.
-- Xét hợp đồng: đây là KHOẢNG TRỐNG (kiểu bên trong JSON chưa ghim), không phải
-- vi phạm. Ứng viên cho danh sách sửa đổi ở A06.


-- ----------------------------------------------------------------------------
-- TASK 3 — UNNEST: ĐỔI GRAIN
-- order grain (1 dòng = 1 đơn) -> item grain (1 dòng = 1 dòng hàng)
-- Số dòng nhân lên theo độ dài trung bình của list (~2,5 ở đây).
-- ----------------------------------------------------------------------------

-- grain: one row = one line item of one delivered order version
CREATE OR REPLACE TABLE core.order_items AS
SELECT
    order_id,
    generate_subscripts(item_list, 1) AS line_no,   -- đi song song với unnest
    unnest(item_list, recursive := true),           -- bung TRƯỜNG thành CỘT
    DATE '2026-06-02'    AS _data_date,
    CAST(NULL AS BIGINT) AS _run_id
FROM (
    SELECT order_id,
           CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[]
           AS item_list
    FROM shopcore_orders_day
);

-- PHÉP KIỂM TRA BẢO TOÀN — bắt buộc sau MỌI lần đổi grain
SELECT (SELECT count(*) FROM core.order_items)                     AS item_rows,
       (SELECT sum(json_array_length(items)) FROM shopcore_orders_day) AS expected;
-- phải bằng nhau: 149.190


-- ----------------------------------------------------------------------------
-- TASK 4 — phân bố ở grain mới
-- ----------------------------------------------------------------------------

SELECT json_array_length(items) AS n_items, count(*) AS orders,
       round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
FROM shopcore_orders_day GROUP BY 1 ORDER BY 1;        -- 1-6, ~30% ở mức 1

SELECT qty, count(*) AS lines,
       round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
FROM core.order_items GROUP BY 1 ORDER BY 1;           -- 1-5, ~55% ở mức 1


-- ----------------------------------------------------------------------------
-- TASK 5 — ORPHAN SKU: đo và báo cáo, KHÔNG sửa
-- ----------------------------------------------------------------------------

SELECT count(*) AS orphan_lines,
       round(100.0 * count(*) / (SELECT count(*) FROM core.order_items), 2) AS pct
FROM core.order_items i
LEFT JOIN read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true) p
       USING (sku)
WHERE p.sku IS NULL;                                   -- 310 dòng (0,21%)
-- LEFT chứ không INNER: INNER âm thầm LOẠI chúng -> doanh thu co lại, không ai hay.
-- Phát hiện và cách ly là việc của A05. Hôm nay chỉ đo.


-- ----------------------------------------------------------------------------
-- TASK 6 — META: BA KIỂU "KHÔNG CÓ GIÁ TRỊ"
-- Cột phẳng chỉ có NULL. JSON có ba cách nói điều đó.
-- ----------------------------------------------------------------------------

-- Trích một trường: ->> trả VARCHAR, json_extract_string là cách viết dài
SELECT CAST(meta AS JSON)->>'device' AS device, count(*) AS n
FROM shopcore_orders_day GROUP BY 1 ORDER BY 2 DESC;
-- android 20.399 · ios 17.863 · web 14.850 · desktop 5.912

-- Ba kiểu, đếm riêng từng cái. CHÍNH CÁC VỊ TỪ NÀY là bài học.
SELECT
  count(*) FILTER (WHERE CAST(meta AS JSON)->>'utm' = 'none') AS sentinel_none,
  count(*) FILTER (WHERE json_type(meta, '$.utm') = 'NULL')   AS json_null,
  count(*) FILTER (WHERE json_extract(meta, '$.utm') IS NULL) AS key_absent,
  count(*) FILTER (WHERE CAST(meta AS JSON)->>'utm' IS NULL)  AS arrow_null
FROM shopcore_orders_day;
-- 5.930 / 5.981 / 5.945 / 11.926
--
-- json_extract  -> NULL của SQL CHỈ khi khoá VẮNG MẶT
-- json_type     -> 'NULL' khi khoá chứa null của JSON (KHÔNG phải NULL của SQL)
-- ->>           -> GỘP json-null và vắng-mặt thành NULL của SQL
--                  (5.981 + 5.945 = 11.926), nhưng KHÔNG gộp chuỗi 'none'
--
-- Để PHÂN BIỆT (kiểm toán, hợp đồng): json_extract / json_type
-- Để CHUẨN HOÁ: chính sự gộp đó là tính năng

-- Gấp cả ba kiểu thành một NULL trung thực
SELECT NULLIF(CAST(meta AS JSON)->>'utm', 'none') AS utm, count(*) AS n
FROM shopcore_orders_day GROUP BY 1 ORDER BY 2 DESC;   -- 4 nguồn thật + NULL

-- CHẶT hỏng ở đây: struct cast đòi ĐỦ MỌI trường, mà ~10% dòng hợp lệ mà không có utm
SELECT CAST(meta AS JSON)::STRUCT(device VARCHAR, utm VARCHAR, sess VARCHAR)
FROM shopcore_orders_day LIMIT 5;
-- Binder Error: Object {...} does not have key "utm"

-- LỎNG thì được: khoá thiếu -> trường NULL
SELECT json_transform(meta, '{"device":"VARCHAR","utm":"VARCHAR","sess":"VARCHAR"}')
FROM shopcore_orders_day LIMIT 5;


-- ----------------------------------------------------------------------------
-- TASK 7 — PRODUCTS: pipe-list và JSON object
-- ----------------------------------------------------------------------------

-- tags là danh sách của nhà nghèo: "wireless|audio|sale"
-- LUẬT: unnest trong truy vấn CON, GROUP BY ở NGOÀI.
SELECT tag, count(*) AS products FROM (
  SELECT unnest(string_split(tags, '|')) AS tag
  FROM read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true)
) GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- attrs là JSON object: {"brand":…,"weight_g":…,"color":…}
-- ->  trả JSON   ->> trả VARCHAR   -> weight_g phải CAST sang INTEGER
SELECT
    CAST(attrs AS JSON)->>'brand'                   AS brand,
    CAST(CAST(attrs AS JSON)->>'weight_g' AS INTEGER) AS weight_g,
    CAST(attrs AS JSON)->>'color'                   AS color,
    count(*)                                        AS products
FROM read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true)
GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10;

-- Thương hiệu nào nhiều sản phẩm nhất?
SELECT CAST(attrs AS JSON)->>'brand' AS brand, count(*) AS products
FROM read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true)
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;


-- ----------------------------------------------------------------------------
-- TASK 8 — MART: phần thưởng cho toàn bộ công đoạn đường ống
-- ----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS marts;

-- grain: one row = one category per order_date
CREATE OR REPLACE TABLE marts.category_daily_revenue AS
SELECT
    CAST(s.order_ts AS DATE)              AS order_date,
    COALESCE(p.category, '(unknown sku)') AS category,
    round(sum(i.qty * i.unit_price), 2)   AS revenue,
    sum(i.qty)                            AS units,
    count(DISTINCT i.order_id)            AS orders
FROM core.order_items i
JOIN shopcore_orders_day s USING (order_id)
LEFT JOIN read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true) p
       USING (sku)
GROUP BY 1, 2;

-- BA nhóm ngày từ MỘT file — giải thích từng nhóm trước khi đọc tiếp
SELECT order_date, count(*) AS categories, round(sum(revenue)) AS revenue
FROM marts.category_daily_revenue GROUP BY 1 ORDER BY 1;
--   2026-06-01  <- ~1,5% bản sửa trễ: đơn XẢY RA hôm trước, hôm nay mới cập nhật
--                  (đúng thứ đã sinh ra 8 file mỗi thư mục ở lake A02)
--   2026-06-02  <- ngày của chính file
--   NULL        <- ~0,02% mốc thời gian bất khả thi, A03 đã cho về NULL
-- MỘT FILE != MỘT NGÀY. Toàn bộ cốt truyện của A08.

SELECT category, round(revenue) AS revenue
FROM marts.category_daily_revenue
WHERE order_date = DATE '2026-06-02' ORDER BY revenue DESC LIMIT 5;
-- office dẫn đầu, ~734k


-- ----------------------------------------------------------------------------
-- BẪY: CỘNG SAI GRAIN
-- ----------------------------------------------------------------------------

-- SAI: nối item grain về order grain rồi cộng cột của đơn hàng
SELECT sum(s.order_total)          -- mỗi đơn 3 mặt hàng bị cộng tổng tiền 3 LẦN
FROM core.order_items i JOIN staging.shopcore_orders s USING (order_id);

-- ĐÚNG: cộng mỗi đại lượng ở ĐÚNG grain của nó
SELECT sum(qty * unit_price) FROM core.order_items;      -- item grain
SELECT sum(order_total)      FROM staging.shopcore_orders;  -- order grain


-- ----------------------------------------------------------------------------
-- BÀI MỞ RỘNG
-- ----------------------------------------------------------------------------

-- 1. Phiên truy cập: rất ít phiên có hơn 1 đơn
SELECT count(DISTINCT CAST(meta AS JSON)->>'sess') AS sessions,
       count(*)                                    AS orders
FROM shopcore_orders_day;                          -- 58.995 / 59.024

-- 2. Xem trước phép đối chiếu của A05: tổng đơn có khớp tổng dòng hàng không?
SELECT count(*) AS mismatched_orders
FROM (
    SELECT i.order_id, sum(i.qty * i.unit_price) AS items_total
    FROM core.order_items i GROUP BY 1
) a
JOIN staging.shopcore_orders s USING (order_id)
WHERE abs(s.order_total - a.items_total) > 0.01;
-- ~0,5-0,8%: rác lệch 0,5% cài có chủ đích + bản sửa trễ làm đổi tổng
-- Hôm nay chỉ đếm; A05 biến nó thành phép kiểm tra chính thức kèm cách ly.

-- 3. Parquet lưu kiểu lồng nhau NGUYÊN BẢN — không còn chuỗi JSON ở đâu cả
COPY (
    SELECT order_id,
           CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[]
           AS items
    FROM shopcore_orders_day
) TO 'D:/etl_lab/small/tmp/items_nested.parquet' (FORMAT parquet);

DESCRIBE SELECT * FROM read_parquet('D:/etl_lab/small/tmp/items_nested.parquet');
-- cột items quay về đúng kiểu STRUCT(...)[] đầy đủ

-- ============================================================================
-- A05: VALIDATION & QUARANTINE
-- ============================================================================
-- Đường dẫn giả định SCALE = small. Đổi 'D:/etl_lab/small' -> 'D:/etl_lab'
-- khi chạy full. Ngày mẫu: 2026-06-02.
--
-- LUẬT LỚN NHẤT CỦA BÀI: đếm -> cách ly -> gate -> nạp.
-- Gate đặt sau INSERT là chuông báo cháy lắp sau khi cháy xong.


-- ============================================================================
-- TASK 2 — MANIFEST: LỜI KHAI CỦA PRODUCER
-- read_json_auto đọc JSON như một bảng.
-- ============================================================================

SELECT rows, late_rows, dup_rows
FROM read_json_auto('D:/etl_lab/small/manifests/orders_2026-06-02.json');
-- rows = 59024. late_rows = dòng mang order_ts của ngày TRƯỚC (bản sửa trễ).
-- dup_rows = order_id lặp trong CÙNG file (bản sửa trong ngày). Không phải rác.


-- ============================================================================
-- TASK 3 — STAGING ĐÃ LÀM SẠCH
-- Chỉ chuẩn hoá CÁCH BIỂU DIỄN. Không map 'unknown' -> NULL, không drop dòng.
-- Hai cột enum cố ý KHÔNG dùng clean_enum: status rác phải tới quarantine với
-- giá trị còn nguyên làm bằng chứng.
-- ============================================================================

CREATE OR REPLACE TABLE staging.shopcore_orders_day AS
SELECT
    order_id,
    CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT)            AS customer_id,
    store_id,
    try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',
                            '%d/%m/%Y %H:%M:%S',
                            '%Y-%m-%dT%H:%M:%S'])              AS order_ts,
    updated_at,
    NULLIF(LOWER(TRIM(status)), '')                            AS status,
    NULLIF(LOWER(TRIM(payment_method)), '')                    AS payment_method,
    TRY_CAST(NULLIF(
      CASE WHEN order_total LIKE '%,%'
           THEN replace(replace(order_total, '.', ''), ',', '.')
           ELSE replace(TRIM(order_total), '$', '') END,
      'N/A') AS DECIMAL(14,2))                                 AS order_total,
    items,
    meta,
    DATE '2026-06-02'                                          AS _data_date,
    CAST(NULL AS BIGINT)                                       AS _run_id
FROM read_csv('D:/etl_lab/small/raw/orders/orders_2026-06-02.csv',
    header = true,
    columns = {'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
               'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
               'payment_method':'VARCHAR','order_total':'VARCHAR',
               'items':'VARCHAR','meta':'VARCHAR'});

-- RECONCILIATION không chính thức: phải bằng ĐÚNG manifest rows.
SELECT count(*) FROM staging.shopcore_orders_day;   -- 59024


-- ============================================================================
-- TASK 4 — ITEMS_AGG: items nói đơn hàng đáng bao nhiêu
-- GROUP BY (order_id, updated_at) chứ KHÔNG phải order_id: ~0,05% id lặp.
-- unnest KHÔNG ngồi chung SELECT với GROUP BY -> bung ở truy vấn con.
-- ============================================================================

CREATE OR REPLACE TEMP TABLE items_agg AS
SELECT order_id, updated_at,
       round(sum(it.qty * it.unit_price), 2) AS items_total,
       count(*) FILTER (p.sku IS NULL)       AS n_orphan_sku
FROM (
    SELECT order_id, updated_at,
           unnest(CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER,
                                              unit_price DOUBLE)[]) AS it
    FROM staging.shopcore_orders_day
) LEFT JOIN read_csv('D:/etl_lab/small/raw/dims/products.csv', header = true) p
       ON it.sku = p.sku
GROUP BY 1, 2;

SELECT count(*) FROM items_agg;                          -- = staged
SELECT count(*) FROM items_agg WHERE n_orphan_sku > 0;   -- ~310


-- ============================================================================
-- TASK 5 — BỘ CHECK -> MỘT reject_reason MỖI DÒNG
-- concat_ws BỎ QUA NULL; NULLIF(...,'') để dòng sạch có reject_reason NULL.
-- Một dòng có thể hỏng nhiều luật cùng lúc -> danh sách ngăn bằng '; '.
-- ============================================================================

CREATE OR REPLACE TEMP TABLE flagged AS
SELECT s.*, i.items_total,
       NULLIF(concat_ws('; ',
         CASE WHEN s.order_id IS NULL THEN 'null_order_id' END,
         CASE WHEN s.order_ts IS NULL THEN 'bad_order_ts' END,
         CASE WHEN s.order_ts IS NOT NULL AND NOT (
                s.order_ts >= GREATEST(DATE '2026-06-01',
                                       DATE '2026-06-02' - INTERVAL 7 DAY)
            AND s.order_ts <  DATE '2026-06-02' + INTERVAL 1 DAY)
              THEN 'ts_out_of_range' END,
         CASE WHEN s.status IS NULL OR s.status NOT IN
              ('created','paid','shipped','delivered','cancelled','refunded')
              THEN 'bad_status' END,
         -- TODO(you) 'bad_payment': card/wallet/bank_transfer/cod/paypal
         CASE WHEN s.order_total IS NULL THEN 'null_total' END,
         CASE WHEN s.order_total < 0 THEN 'negative_total' END,
         CASE WHEN s.order_total >= 0 AND i.items_total IS NOT NULL
               AND abs(s.order_total - i.items_total) > 0.01
              THEN 'total_mismatch' END,
         -- TODO(you) 'null_customer' / 'orphan_customer' / 'orphan_store'
         CASE WHEN i.n_orphan_sku > 0 THEN 'orphan_sku' END
       ), '') AS reject_reason
FROM staging.shopcore_orders_day s
LEFT JOIN items_agg i USING (order_id, updated_at)
LEFT JOIN read_csv('D:/etl_lab/small/raw/dims/customers.csv', header = true) c
       ON s.customer_id = c.customer_id
LEFT JOIN read_csv('D:/etl_lab/small/raw/dims/stores.csv', header = true) st
       ON s.store_id = st.store_id;

-- count(reject_reason) đếm non-NULL = số dòng vào quarantine. Đích: 992 (1,68%).
SELECT count(*) AS staged, count(reject_reason) AS bad FROM flagged;

-- Nhìn mười nạn nhân. Mỗi lý do phải khiến bạn gật đầu.
SELECT order_id, status, payment_method, order_total, items_total, reject_reason
FROM flagged WHERE reject_reason IS NOT NULL LIMIT 10;

-- Phân rã theo từng lý do (bung danh sách '; ' ra thành dòng)
SELECT reason, count(*) AS n
FROM (SELECT unnest(string_split(reject_reason, '; ')) AS reason
      FROM flagged WHERE reject_reason IS NOT NULL)
GROUP BY 1 ORDER BY 2 DESC;


-- ============================================================================
-- TASK 6 — SPLIT LOAD
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 6.1 — BẰNG CHỨNG luôn được ghi, kể cả (nhất là) vào ngày tồi tệ
-- Nhớ mkdir thư mục CHA trước: COPY tạo folder partition, không tạo folder cha.
-- ----------------------------------------------------------------------------
COPY (
  SELECT *, now() AT TIME ZONE 'UTC' AS _rejected_at,
         COALESCE(CAST(order_ts AS DATE), DATE '2026-06-02') AS order_date
  FROM flagged WHERE reject_reason IS NOT NULL
) TO 'D:/etl_lab/small/quarantine/orders'
  (FORMAT parquet, PARTITION_BY (order_date), OVERWRITE_OR_IGNORE,
   FILENAME_PATTERN 'from_2026-06-02_{i}');
-- COALESCE: dòng timestamp hỏng mượn ngày của file thay vì thành khoá NULL.
-- FILENAME_PATTERN: dòng xấu đến trễ rơi vào phân vùng CŨ HƠN -> nhiều ngày cùng
--   ghi một folder; tên mặc định data_0.parquet sẽ để D+1 ĐÈ lên D.

-- ----------------------------------------------------------------------------
-- 6.2 — GATE (phần so ngưỡng làm bằng Python; đây là truy vấn nó dựa vào)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.alerts (
  alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
  source VARCHAR, message VARCHAR);

SELECT reason, round(100.0 * count(*) / 59024, 3) AS pct
FROM (SELECT unnest(string_split(reject_reason, '; ')) AS reason
      FROM flagged WHERE reject_reason IS NOT NULL)
GROUP BY 1;

-- ----------------------------------------------------------------------------
-- 6.3 — DÒNG TỐT
-- grain: một dòng = MỘT PHIÊN BẢN được giao của một đơn (order_id, _data_date).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.orders AS
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE 1 = 0;          -- WHERE 1=0: sao chép HÌNH DẠNG, 0 dòng

INSERT INTO core.orders
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE reject_reason IS NULL;
-- INSERT trần KHÔNG idempotent. Chạy lại = DROP TABLE core.orders rồi nạp lại.
-- A07 sửa tử tế.

SELECT count(*) FROM core.orders;   -- 58032 = 59024 - 992


-- ============================================================================
-- TASK 7 — ops.dq_report: DẤU VẾT GIẤY TỜ
-- Ghi CẢ check 0 lỗi. bad_payment = 0 CHỨNG MINH chuẩn hoá A03 đang chạy đúng.
-- ============================================================================

CREATE TABLE IF NOT EXISTS ops.dq_report (
  run_at TIMESTAMP, order_date DATE, check_type VARCHAR, check_name VARCHAR,
  rows_checked BIGINT, rows_failed BIGINT, pct_failed DOUBLE,
  threshold_pct DOUBLE, passed BOOLEAN);

-- ROW: quan sát được LEFT JOIN lên DANH SÁCH ĐẦY ĐỦ 12 check.
-- Threshold hard-code ở đây CHỈ để đọc; trong work/ nó phải chảy từ YAML.
INSERT INTO ops.dq_report
WITH observed AS (
  SELECT reason, count(*) AS n
  FROM (SELECT unnest(string_split(reject_reason, '; ')) AS reason
        FROM flagged WHERE reject_reason IS NOT NULL)
  GROUP BY 1
)
SELECT current_timestamp, DATE '2026-06-02', 'row', c.check_name,
       59024, coalesce(o.n, 0),
       round(100.0 * coalesce(o.n, 0) / 59024, 3) AS pct,
       c.threshold_pct,
       coalesce(pct <= c.threshold_pct, true)
FROM (VALUES ('null_order_id', NULL), ('bad_order_ts', 0.5),
             ('ts_out_of_range', NULL), ('bad_status', 1.0),
             ('bad_payment', NULL), ('null_total', 1.0),
             ('negative_total', NULL), ('total_mismatch', 1.0),
             ('null_customer', NULL), ('orphan_customer', 0.5),
             ('orphan_store', NULL), ('orphan_sku', 1.0)
     ) c(check_name, threshold_pct)
LEFT JOIN observed o ON o.reason = c.check_name;

-- AGGREGATE: cặp (order_id, updated_at) phải DUY NHẤT — lời khẳng định Task 4
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '2026-06-02', 'aggregate',
       'unique_order_id_updated_at',
       59024, count(*), round(100.0 * count(*) / 59024, 3), 0.0, count(*) = 0
FROM (SELECT order_id, updated_at FROM staging.shopcore_orders_day
      GROUP BY 1, 2 HAVING count(*) > 1);

-- TODO(you) AGGREGATE 'dup_order_id' — cùng khuôn, GROUP BY riêng order_id.
--   Informational (threshold NULL, passed true). Bản sửa trong ngày là bình
--   thường: BÁO CÁO tỉ lệ, KHÔNG loại dòng nào. A08 gấp chúng lại.

-- RECONCILIATION: check rẻ nhất, giá trị cao nhất trong cả bộ suite
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '2026-06-02', 'reconciliation',
       'rowcount_vs_manifest',
       59024, abs(59024 - rows), NULL, 0.0, 59024 = rows
FROM read_json_auto('D:/etl_lab/small/manifests/orders_2026-06-02.json');

-- TODO(you) AGGREGATE 'quarantine_rate' — tỉ lệ chung cả ngày.
--   threshold_pct NULL (contract cấp ngân sách theo RULE, không theo ngày).
--   Đây là dòng mà biểu đồ xu hướng Stretch 1 dựng lên.


-- ============================================================================
-- VERIFY YOURSELF
-- ============================================================================

SELECT count(*) FROM core.orders;
-- 414095   (58032 + 179044 + 177019)

SELECT check_name, rows_failed, pct_failed FROM ops.dq_report
WHERE order_date = DATE '2026-06-02' AND check_type = 'row'
ORDER BY rows_failed DESC;
-- orphan_sku 310 · total_mismatch 308 · null_total 117 · null_customer 94
-- bad_status 71 · negative_total 52 · orphan_customer 27 · bad_order_ts 18
-- ts_out_of_range, bad_payment, orphan_store, null_order_id: đều 0

SELECT count(*) FROM ops.dq_report;
-- 48 (16 x 3 ngày; run hỏng của Task 10 sẽ thêm 16 nữa)

-- Tổng theo check của 06-02 (997) LỚN HƠN số dòng quarantine (992):
-- năm dòng hỏng hai luật cùng lúc. Chứng minh:
SELECT count(*) FROM read_parquet('D:/etl_lab/small/quarantine/orders/**/*.parquet')
WHERE _data_date = DATE '2026-06-02' AND reject_reason LIKE '%;%';
-- 5
-- Chú ý: chọn theo _data_date, KHÔNG theo folder order_date — dòng xấu đến trễ
-- không nằm cùng phân vùng với file nó đến cùng.

-- Dấu vết pager sau demo Task 9 + Task 10:
SELECT severity, source, message FROM ops.alerts ORDER BY alert_ts;
-- error · freshness · FRESHNESS BREACH: ... max(_data_date) = 2026-07-11, expected 2026-07-12
-- error · dq_gate   · DQ HARD FAIL 2026-06-19: total_mismatch at 0.522% exceeds ... 0.1%


-- ============================================================================
-- STRETCH GOALS
-- ============================================================================

-- 1. Đường xu hướng: chạy cả 45 ngày v1 ở small (~2 phút) rồi nhìn theo ngày.
--    Phải PHẲNG ở ~1,7%, kể cả ngày cao điểm. Đây là dashboard DQ hữu ích nhất
--    trong đội thật.
SELECT order_date, pct_failed FROM ops.dq_report
WHERE check_name = 'quarantine_rate' ORDER BY order_date;

-- 2. Dải sản lượng: contract nói load ngoài 0,5x–4x trung vị 7 ngày gần nhất thì
--    phải cảnh báo. Dựng như một aggregate check trên lịch sử rows_checked.
--    (Chạy stretch 1 trước để có lịch sử.) Ghi alert severity 'warn'.
SELECT order_date, rows_checked,
       median(rows_checked) OVER (ORDER BY order_date
                                  ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS med7
FROM ops.dq_report WHERE check_name = 'rowcount_vs_manifest'
ORDER BY order_date;
-- Ngày cao điểm phải NẰM TRONG dải — độ rộng đó là cố ý.

-- 3. Replay: đọc dòng đã cách ly, chạy lại bộ check HIỆN TẠI, đếm xem bao nhiêu
--    dòng nay sẽ pass. Đó là cách phục hồi dữ liệu sau khi nới một luật quá chặt,
--    và là mùi vị đầu tiên của tư duy backfill ở A11.
SELECT reject_reason, count(*) AS n
FROM read_parquet('D:/etl_lab/small/quarantine/orders/**/*.parquet')
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
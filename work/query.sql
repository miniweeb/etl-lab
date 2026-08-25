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
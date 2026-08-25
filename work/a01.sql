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
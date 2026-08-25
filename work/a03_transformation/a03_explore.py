# %%
import os
while not os.path.exists("lib") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print(os.getcwd())      # phải in ra D:\ETL


# %%
from work.a03_transformation.a03_bootstrap import con, section, R, DAY, SCALE
from work.a03_transformation.a03_cleaners import (PAYMENT_VALUES, STATUS_VALUES, clean_bool, clean_enum,
                           clean_id, clean_money, clean_order_ts, clean_signup_ts)

print(f"scale={SCALE}  day={DAY}  root={R}")

# %% ── Task 1 — bốn schema ───────────────────────────────────────────
section("TASK 1 — schemas")

con.execute("""
CREATE SCHEMA IF NOT EXISTS staging;   -- clean, typed, 1:1 with raw
CREATE SCHEMA IF NOT EXISTS core;      -- joined, enriched
CREATE SCHEMA IF NOT EXISTS marts;     -- aggregates for consumers (A04+)
CREATE SCHEMA IF NOT EXISTS ops;       -- pipeline bookkeeping (A05, A07)
""")
con.sql("""
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name IN ('staging','core','marts','ops') ORDER BY 1
""").show()

# %% ── Task 2 — nạp 1 ngày, y nguyên ─────────────────────────────────
section("TASK 2 — raw staging, cột bẩn để dạng VARCHAR")

con.execute(f"""
-- grain: one row = one order version as delivered in the {DAY} file
CREATE OR REPLACE TABLE staging.shopcore_orders_raw AS
SELECT *,
    DATE '{DAY}'         AS _data_date,   -- lineage: the delivery's business date
    CAST(NULL AS BIGINT) AS _run_id       -- lineage: comes alive in A07
FROM read_csv('{R}/raw/orders/orders_{DAY}.csv',
    header=true,
    columns={{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'}});
""")

# đây là cả bài A03 gói trong một truy vấn:
con.sql("""
    SELECT count(*)                                          AS rows,
           count(*) FILTER (WHERE customer_id IS NULL)        AS missing,
           count(*) FILTER (WHERE customer_id LIKE '%.0')     AS excel_float
    FROM staging.shopcore_orders_raw
""").show()
# 59.024 / 94 / 120 — đọc BIGINT thì 120 kia biến mất mà bạn không hay

# %% ── Task 3 — từng cleaner một, nhìn rác trước ─────────────────────
section("TASK 3a — mốc thời gian: 3 định dạng + ngày bất khả thi")

con.sql("""
    SELECT order_ts FROM staging.shopcore_orders_raw
    WHERE order_ts LIKE '%/%' OR order_ts LIKE '%T%' LIMIT 5
""").show()

con.sql(f"""
    SELECT count(*) FILTER (WHERE {clean_order_ts()} IS NULL) AS unparseable
    FROM staging.shopcore_orders_raw
""").show()
# 18 dòng (0,03%)

# %%
section("TASK 3b — tiền: thử cleaner trên bảng VALUES trước")

con.sql(f"""
    SELECT v, {clean_money('v')} AS cleaned
    FROM (VALUES ('93,32'),('1.234,56'),('$79.98'),('N/A'),(''),('-42.10')) t(v)
""").show()
# 93.32 | 1234.56 | 79.98 | NULL | NULL | -42.10
# số âm sống sót CÓ CHỦ ĐÍCH: dấu là vấn đề hợp lệ (A05), không phải định dạng

# %%
section("TASK 3c+3d — id và enum")

con.sql(f"""
    SELECT {clean_id('v')} AS id FROM (VALUES ('123456.0'),('789'),(''),('abc')) t(v)
""").show()

con.sql(f"""
    SELECT v, {clean_enum('v', STATUS_VALUES)} AS status
    FROM (VALUES ('delivered'),('PAID '),('Paid'),('unknown')) t(v)
""").show()

# %% ── Task 4 — lắp ráp staging ──────────────────────────────────────
section("TASK 4 — staging.shopcore_orders + khử trùng trong file")

con.execute(f"""
-- grain: one row = one order (its latest version in this file)
CREATE OR REPLACE TABLE staging.shopcore_orders AS
WITH cleaned AS (
    SELECT
        order_id,
        {clean_id('customer_id')}                       AS customer_id,
        store_id,
        {clean_order_ts('order_ts')}                    AS order_ts,
        updated_at,
        {clean_enum('status', STATUS_VALUES)}           AS status,
        {clean_enum('payment_method', PAYMENT_VALUES)}  AS payment_method,
        {clean_money('order_total')}                    AS order_total,
        items,          -- still a JSON string: parsing it is A04's whole show
        meta,
        _data_date, _run_id
    FROM staging.shopcore_orders_raw
),
numbered AS (
    SELECT *, row_number() OVER (PARTITION BY order_id
                                 ORDER BY updated_at DESC) AS rn FROM cleaned
)
SELECT * EXCLUDE (rn), CAST(order_ts AS DATE) AS order_date
FROM numbered WHERE rn = 1;
""")

# bốn phép kiểm tra của Task 4
con.sql("SELECT count(*) AS staged FROM staging.shopcore_orders").show()
# 58.993 = 59.024 − 31. Manifest nói dup_rows=29 — 2 dòng còn lại là gì?

con.sql("DESCRIBE staging.shopcore_orders").show()
# 13 cột, đúng 4 cột VARCHAR sống sót: items, meta, 2 enum

con.sql("""
    SELECT order_date, count(*) FROM staging.shopcore_orders
    GROUP BY 1 ORDER BY 1
""").show()
# 2026-06-01: 883 (bản sửa trễ!) | 2026-06-02: 58.092 | NULL: 18

con.sql("SELECT max(_data_date) AS freshness_probe FROM staging.shopcore_orders").show()

# %% ── Task 5 — báo cáo làm sạch ─────────────────────────────────────
section("TASK 5 — ops.cleaning_report, 13 quy tắc")

con.execute(f"""
-- grain: one row = one cleaning rule for one file_date
CREATE OR REPLACE TABLE ops.cleaning_report AS
WITH raw AS (SELECT * FROM staging.shopcore_orders_raw)
SELECT DATE '{DAY}' AS file_date, rule, rows_affected FROM (
    SELECT 'ts_ddmmyyyy_fixed' AS rule, count(*) AS rows_affected
    FROM raw WHERE order_ts LIKE '%/%'
    UNION ALL SELECT 'ts_isot_fixed', count(*) FROM raw WHERE order_ts LIKE '%T%'
    UNION ALL SELECT 'ts_unparseable_to_null', count(*) FROM raw
        WHERE {clean_order_ts()} IS NULL
    UNION ALL SELECT 'total_na_or_empty_to_null', count(*) FROM raw
        WHERE order_total IS NULL OR TRIM(order_total) = 'N/A'
    UNION ALL SELECT 'customer_id_float_fixed', count(*) FROM raw
        WHERE customer_id LIKE '%.0'
    UNION ALL SELECT 'dup_order_id_removed', count(*) - count(DISTINCT order_id) FROM raw
        UNION ALL SELECT 'total_dollar_stripped', count(*) FROM raw
        WHERE order_total LIKE '%$%'
    UNION ALL SELECT 'total_decimal_comma_fixed', count(*) FROM raw
        WHERE order_total LIKE '%,%'
    UNION ALL SELECT 'total_negative_kept', count(*) FROM raw
        WHERE {clean_money()} < 0
    UNION ALL SELECT 'status_recased', count(*) FROM raw
        WHERE status IS NOT NULL
          AND status <> lower(TRIM(status))
          AND {clean_enum('status', STATUS_VALUES)} IS NOT NULL
    UNION ALL SELECT 'status_junk_to_null', count(*) FROM raw
        WHERE {clean_enum('status', STATUS_VALUES)} IS NULL
    UNION ALL SELECT 'payment_recased', count(*) FROM raw
        WHERE payment_method IS NOT NULL
          AND payment_method <> lower(TRIM(payment_method))
    UNION ALL SELECT 'customer_id_missing', count(*) FROM raw
        WHERE customer_id IS NULL
    -- BẪY: ô CSV rỗng về dạng NULL, không phải ''. Dùng IS NULL.
);
""")
con.sql("SELECT rule, rows_affected FROM ops.cleaning_report ORDER BY rule").show()

# %% ── Task 6 — bảng chiều ───────────────────────────────────────────
section("TASK 6 — khám phá customers.csv TRƯỚC khi viết code")

con.sql(f"""
    SELECT country, count(*) FROM read_csv('{R}/raw/dims/customers.csv', header=true)
    GROUP BY 1 ORDER BY 2 DESC
""").show()          # 24 cách viết cho 10 quốc gia, cộng NULL

con.sql(f"""
    SELECT is_active, count(*) FROM read_csv('{R}/raw/dims/customers.csv', header=true)
    GROUP BY 1 ORDER BY 2 DESC
""").show()          # 11 biến thể true/false, cộng NULL

# %%
section("TASK 6a — bảng ánh xạ quốc gia")

con.execute("""
-- grain: one row = one spelling variant of one country
CREATE OR REPLACE TABLE staging.shopcore_country_map(variant VARCHAR, iso2 VARCHAR);
INSERT INTO staging.shopcore_country_map VALUES
 ('us','US'),('usa','US'),('united states','US'),
 ('vn','VN'),('vietnam','VN'),('viet nam','VN'),
 ('gb','GB'),('uk','GB'),('united kingdom','GB');
 -- TODO: DE, FR, JP, AU, CA, BR, IN — mở rộng tới khi country check đạt
""")
con.execute("""
INSERT INTO staging.shopcore_country_map VALUES
 ('de','DE'),('germany','DE'),
 ('fr','FR'),('france','FR'),
 ('jp','JP'),('japan','JP'),
 ('au','AU'),('australia','AU'),
 ('ca','CA'),('canada','CA'),
 ('br','BR'),('brazil','BR'),
 ('in','IN'),('india','IN');
""")

# %%
con.sql(f"""
    SELECT DISTINCT lower(TRIM(country)) AS unmapped
    FROM read_csv('{R}/raw/dims/customers.csv', header=true)
    WHERE country IS NOT NULL
      AND lower(TRIM(country)) NOT IN (SELECT variant FROM staging.shopcore_country_map)
    ORDER BY 1
""").show()

# %%
section("TASK 6b — core.customers")

con.execute(f"""
-- grain: one row = one customer
CREATE OR REPLACE TABLE core.customers AS
WITH raw AS (
    SELECT * FROM read_csv('{R}/raw/dims/customers.csv', header=true,
        columns={{'customer_id':'BIGINT','name':'VARCHAR','email':'VARCHAR',
                 'country':'VARCHAR','city':'VARCHAR','signup_ts':'VARCHAR',
                 'is_active':'VARCHAR'}})
),
fixed AS (
    SELECT
        customer_id,
        CASE WHEN name LIKE '%,%'    -- "NGUYEN, Linh" -> "Linh Nguyen"
             THEN TRIM(split_part(name, ',', 2)) || ' ' ||
                  upper(substr(TRIM(split_part(name, ',', 1)), 1, 1)) ||
                  lower(substr(TRIM(split_part(name, ',', 1)), 2))
             ELSE TRIM(name) END                    AS name,
        lower(TRIM(email))                          AS email,
        TRIM(country)                               AS country_raw,
        city,
        {clean_signup_ts('signup_ts')}              AS signup_ts,
        {clean_bool('is_active')}                   AS is_active
    FROM raw
)
SELECT f.customer_id, f.name, f.email, m.iso2 AS country, f.city,
       f.signup_ts, f.is_active,
       current_date         AS _data_date,   -- dims: sync date = load date
       CAST(NULL AS BIGINT) AS _run_id
FROM fixed f
LEFT JOIN staging.shopcore_country_map m ON lower(f.country_raw) = m.variant;
""")

con.sql("""
    SELECT count(*) AS customers,
           count(*) FILTER (WHERE signup_ts IS NULL) AS null_signup,
           count(*) FILTER (WHERE is_active IS NULL) AS null_active,
           count(*) FILTER (WHERE country   IS NULL) AS null_country,
           count(*) FILTER (WHERE name LIKE '%,%')   AS comma_names
    FROM core.customers
""").show()
# 60000 / 1175 / 1164 / 548 / 0

con.sql("SELECT min(signup_ts), max(signup_ts) FROM core.customers").show()
# 2019-01-01 .. 2026-05-31 — ngày hợp lý = múi giờ đã ghim đúng

# %%
section("TASK 6c — products + stores")

# TODO: core.products, bạn tự viết. Nhớ ghi grain lên đầu: one row = one sku
#       unit_price dùng {clean_money('unit_price')} rồi ::DECIMAL(10,2)
#       tags/attrs giữ VARCHAR — nguyên liệu của A04
#       và cho cả hai bảng 2 cột lineage

con.execute(f"""
-- grain: one row = one store
CREATE OR REPLACE TABLE core.stores AS
SELECT *, current_date AS _data_date, CAST(NULL AS BIGINT) AS _run_id
FROM read_csv('{R}/raw/dims/stores.csv', header=true);
""")
con.sql("DESCRIBE core.stores").show()     # DESCRIBE trước khi tin auto-detect


# %% ── Task 8 — cleaners
con.sql(f"""
    SELECT v, {clean_signup_ts('v')} AS parsed
    FROM (VALUES ('2024-04-05 18:14:38'),('05/04/2024'),('1712345678'),('rác')) t(v)
""").show()

# %% ── Task 9 — làm giàu và tổng hợp ─────────────────────────────────
section("TASK 9 — core.orders_enriched")

con.execute("""
-- grain: one row = one order — unchanged by the joins (the count below proves it)
CREATE OR REPLACE TABLE core.orders_enriched AS
SELECT
    o.*,
    c.country AS customer_country, c.is_active  AS customer_is_active,
    s.store_name, s.region                      AS store_region,
    extract(hour FROM o.order_ts)               AS order_hour,
    extract(isodow FROM o.order_date) IN (6, 7) AS is_weekend,
    CASE WHEN o.order_total IS NULL THEN 'unknown'
         WHEN o.order_total < 0    THEN 'invalid'
         WHEN o.order_total < 20   THEN 'small'
         WHEN o.order_total < 100  THEN 'medium'
         WHEN o.order_total < 500  THEN 'large'
         ELSE 'huge' END                        AS total_bucket
FROM staging.shopcore_orders o
LEFT JOIN core.customers c USING (customer_id)
LEFT JOIN core.stores    s USING (store_id);
""")

con.sql("SELECT count(*) AS enriched FROM core.orders_enriched").show()   # 58.993

con.sql("""
    SELECT count(*) FILTER (WHERE customer_id IS NULL)  AS missing_id,
           count(*) FILTER (WHERE customer_id IS NOT NULL
                            AND c.customer_id IS NULL)  AS orphan_id
    FROM staging.shopcore_orders o LEFT JOIN core.customers c USING (customer_id)
""").show()      # 94 / 27 — ghi vào journal, A05 sẽ bắt bạn chịu trách nhiệm

# %%
section("TASK 9b — core.daily_store_sales")

con.execute("""
-- grain: one row = one store per order_date
CREATE OR REPLACE TABLE core.daily_store_sales AS
SELECT
    order_date, store_id,
    any_value(store_name) AS store_name, any_value(store_region) AS region,
    count(*) AS orders, count(DISTINCT customer_id) AS customers,
    sum(order_total) AS gross_revenue, round(avg(order_total), 2) AS avg_order_value,
    count(*) FILTER (WHERE status = 'cancelled')   AS cancelled_orders,
    count(*) FILTER (WHERE order_total IS NULL)    AS orders_missing_total,
    any_value(_data_date) AS _data_date,
    any_value(_run_id)    AS _run_id
FROM core.orders_enriched
GROUP BY order_date, store_id;
""")
con.sql("SELECT count(*) AS rollup_rows FROM core.daily_store_sales").show()
# 723 — vì sao nhiều hơn 400 cửa hàng? Câu hỏi này nằm trong bài thi A08.

con.sql("""
    SELECT total_bucket, count(*) FROM core.orders_enriched GROUP BY 1 ORDER BY 2 DESC
""").show()
# %%

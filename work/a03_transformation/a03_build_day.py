# work/a03_transformation/a03_build_day.py
"""A03 — dựng lại toàn bộ 1 ngày từ một câu lệnh.

Chạy:  python -m work.a03_transformation.a03_build_day --date 2026-06-02 --scale small
       python -m work.a03_transformation.a03_build_day --date 2026-06-02 --scale full

Ngày là THAM SỐ, không ghi cứng: A07 và A11 sẽ chạy theo khoảng ngày.
Mọi quy tắc làm sạch import từ a03_cleaners — không dán lại dòng SQL nào.
"""
import argparse
import json
import time

import duckdb

from lib.labpaths import data_root, warehouse_path, ensure_dirs
from work.a03_transformation.a03_cleaners import (
    PAYMENT_VALUES, STATUS_VALUES, clean_bool, clean_enum, clean_id,
    clean_money, clean_order_ts, clean_signup_ts)

V1_COLUMNS = """{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
     'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
     'payment_method':'VARCHAR','order_total':'VARCHAR',
     'items':'VARCHAR','meta':'VARCHAR'}"""


def build_day(con, r: str, day: str) -> None:
    """Tasks 1-6 và 9, dựng lại từ đầu cho một ngày."""

    # ── Task 1 — schemas ────────────────────────────────────────────────
    # Bắt buộc ở đây: một database mới (đổi scale) chưa có schema nào.
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS core;
        CREATE SCHEMA IF NOT EXISTS marts;
        CREATE SCHEMA IF NOT EXISTS ops;
    """)

    # ── Task 2 — raw staging: cột bẩn để dạng VARCHAR để còn đếm được ───
    con.execute(f"""
    -- grain: one row = one order version as delivered in the {day} file
    CREATE OR REPLACE TABLE staging.shopcore_orders_raw AS
    SELECT *,
        DATE '{day}'         AS _data_date,
        CAST(NULL AS BIGINT) AS _run_id
    FROM read_csv('{r}/raw/orders/orders_{day}.csv',
        header=true, columns={V1_COLUMNS});
    """)

    # ── Task 4 — staging đã làm sạch + khử trùng trong file ─────────────
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
            items, meta,
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

    # ── Task 5 — cleaning report: 13 quy tắc, mỗi quy tắc đếm dòng nó chạm
    con.execute(f"""
    -- grain: one row = one cleaning rule for one file_date
    CREATE OR REPLACE TABLE ops.cleaning_report AS
    WITH raw AS (SELECT * FROM staging.shopcore_orders_raw)
    SELECT DATE '{day}' AS file_date, rule, rows_affected FROM (
        SELECT 'ts_ddmmyyyy_fixed' AS rule, count(*) AS rows_affected
        FROM raw WHERE order_ts LIKE '%/%'
        UNION ALL SELECT 'ts_isot_fixed', count(*) FROM raw WHERE order_ts LIKE '%T%'
        UNION ALL SELECT 'ts_unparseable_to_null', count(*) FROM raw
            WHERE {clean_order_ts()} IS NULL
        UNION ALL SELECT 'total_na_or_empty_to_null', count(*) FROM raw
            WHERE order_total IS NULL OR TRIM(order_total) = 'N/A'
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
        UNION ALL SELECT 'customer_id_float_fixed', count(*) FROM raw
            WHERE customer_id LIKE '%.0'
        UNION ALL SELECT 'customer_id_missing', count(*) FROM raw
            WHERE customer_id IS NULL
        UNION ALL SELECT 'dup_order_id_removed', count(*) - count(DISTINCT order_id) FROM raw
    );
    """)

    # ── Task 6a — conformance: nhiều cách viết vào, một mã chuẩn ra ─────
    con.execute("""
    -- grain: one row = one spelling variant of one country
    CREATE OR REPLACE TABLE staging.shopcore_country_map(variant VARCHAR, iso2 VARCHAR);
    """)
    con.execute("""
    INSERT INTO staging.shopcore_country_map VALUES
     ('us','US'),('usa','US'),('united states','US'),
     ('vn','VN'),('vietnam','VN'),('viet nam','VN'),
     ('gb','GB'),('uk','GB'),('united kingdom','GB'),
     ('de','DE'),('germany','DE'),
     ('fr','FR'),('france','FR'),
     ('jp','JP'),('japan','JP'),
     ('au','AU'),('australia','AU'),
     ('ca','CA'),('canada','CA'),
     ('br','BR'),('brazil','BR'),
     ('in','IN'),('india','IN');
    """)

    # ── Task 6b — core.customers: signup_ts có BA định dạng ────────────
    # Phiên đã SET TimeZone='UTC' ở dưới: to_timestamp nhận biết múi giờ.
    con.execute(f"""
    -- grain: one row = one customer
    CREATE OR REPLACE TABLE core.customers AS
    WITH raw AS (
        SELECT * FROM read_csv('{r}/raw/dims/customers.csv', header=true,
            columns={{'customer_id':'BIGINT','name':'VARCHAR','email':'VARCHAR',
                     'country':'VARCHAR','city':'VARCHAR','signup_ts':'VARCHAR',
                     'is_active':'VARCHAR'}})
    ),
    fixed AS (
        SELECT
            customer_id,
            CASE WHEN name LIKE '%,%'      -- "NGUYEN, Linh" -> "Linh Nguyen"
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

    # ── Task 6c — core.products và core.stores ─────────────────────────
    con.execute(f"""
    -- grain: one row = one sku
    CREATE OR REPLACE TABLE core.products AS
    SELECT sku, product_name, category, subcategory,
           CAST({clean_money('unit_price')} AS DECIMAL(10,2))  AS unit_price,
           tags, attrs,          -- pipe-list và JSON: nguyên liệu của A04
           current_date         AS _data_date,
           CAST(NULL AS BIGINT) AS _run_id
    FROM read_csv('{r}/raw/dims/products.csv', header=true,
        columns={{'sku':'VARCHAR','product_name':'VARCHAR','category':'VARCHAR',
                 'subcategory':'VARCHAR','unit_price':'VARCHAR',
                 'tags':'VARCHAR','attrs':'VARCHAR'}});
    """)
    con.execute(f"""
    -- grain: one row = one store
    CREATE OR REPLACE TABLE core.stores AS
    SELECT *, current_date AS _data_date, CAST(NULL AS BIGINT) AS _run_id
    FROM read_csv('{r}/raw/dims/stores.csv', header=true);
    """)

    # ── Task 9 — làm giàu: phi chuẩn hoá CÓ CHỦ ĐÍCH ───────────────────
    # LEFT JOIN vì ~0,2% đơn có customer_id thiếu hoặc mồ côi;
    # INNER JOIN sẽ âm thầm loại chúng và doanh thu co lại mà không ai hay.
    con.execute("""
    -- grain: one row = one order — unchanged by the joins
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

    # ── Task 9b — rollup: GROUP BY là một lần đổi grain có chủ đích ─────
    # any_value trung thực vì một lần dựng ứng với một bản giao.
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


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--scale", choices=["small", "full"], default="small")
    args = ap.parse_args()

    ensure_dirs(args.scale)
    root = data_root(args.scale)
    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute("SET memory_limit='8GB'; SET threads=8; SET TimeZone='UTC';")
    con.execute(f"SET temp_directory='{root.as_posix()}/tmp';")

    t0 = time.perf_counter()
    build_day(con, root.as_posix(), args.date)
    elapsed = time.perf_counter() - t0

    man = json.loads((root / "raw" / "manifest" / f"orders_{args.date}.json").read_text())
    got = con.execute("SELECT count(*) FROM staging.shopcore_orders_raw").fetchone()[0]
    staged = con.execute("SELECT count(*) FROM staging.shopcore_orders").fetchone()[0]
    enriched = con.execute("SELECT count(*) FROM core.orders_enriched").fetchone()[0]

    print(f"{args.date} [{args.scale}]: raw={got:,} manifest={man['rows']:,} "
          f"match={got == man['rows']}  staged={staged:,}  enriched={enriched:,}  "
          f"{elapsed:.1f}s")
"""A05 — chạy trọn một business date qua bộ validation.

Công việc: stage -> flag -> report -> quarantine -> gate -> load, ĐÚNG thứ tự đó.
Gate phải đứng TRƯỚC khi core thấy một dòng nào; báo cáo phải đứng trước gate
(để dòng report vẫn tồn tại khi run chết).

Giả định:
  - staging/core/ops schema đã tồn tại trên connection truyền vào.
  - contract nằm ở CONTRACT_PATH trong repo, không phải dưới DATA_ROOT.
  - biểu thức làm sạch IMPORT từ a03_cleaners, không paste lại (§7.4).

A07 (pipeline) và A11 (runner) sẽ import module NÀY, không sao chép nó.
"""
from __future__ import annotations

import duckdb
import yaml

from lib.labpaths import (manifest_dir, quarantine_dir, raw_dims_dir,
                          raw_orders_dir)
from work.a03_transformation.a03_cleaners import (clean_id, clean_money,
                                                  clean_order_ts)

CONTRACT_PATH = "contracts/shopcore_orders_daily.contract.yaml"

# tên rule trong contract -> tên check trong bộ suite
CHECK_FOR = {
    "null_or_invalid_status":  "bad_status",
    "unparseable_order_ts":    "bad_order_ts",
    "unparseable_order_total": "null_total",
    "orphan_customer_id":      "orphan_customer",
    "orphan_sku_lines":        "orphan_sku",
    "total_vs_items_mismatch": "total_mismatch",
}

# danh sách ĐẦY ĐỦ các row check — dq_report ghi cả check 0 lỗi
ROW_CHECKS = ['null_order_id', 'bad_order_ts', 'ts_out_of_range', 'bad_status',
              'bad_payment', 'null_total', 'negative_total', 'total_mismatch',
              'null_customer', 'orphan_customer', 'orphan_store', 'orphan_sku']

STATUS_WHITELIST = "('created','paid','shipped','delivered','cancelled','refunded')"
ITEM_STRUCT = "STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[]"

V1_COLS = ("{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',"
           "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
           "'payment_method':'VARCHAR','order_total':'VARCHAR',"
           "'items':'VARCHAR','meta':'VARCHAR'}")


def load_thresholds(path: str = CONTRACT_PATH) -> dict[str, float]:
    """Ngưỡng dung sai đến TỪ contract đã ký, không từ ý kiến của ta.

    YAML không có kiểu percent: '0.5%' về Python là chuỗi -> rstrip rồi float.
    """
    contract = yaml.safe_load(open(path, encoding="utf-8"))
    return {CHECK_FOR[rule]: float(str(tol).rstrip('%'))
            for rule, tol in contract["quality"]["row_level_tolerances"].items()
            if rule in CHECK_FOR}


# ── Task 3 ───────────────────────────────────────────────────────────────
def stage_day(con: duckdb.DuckDBPyConnection, day: str, scale: str) -> int:
    """Dựng staging.shopcore_orders_day cho một ngày; trả về số dòng đã stage.

    Chỉ chuẩn hoá CÁCH BIỂU DIỄN. Không phán xét giá trị, không drop dòng nào:
    status rác phải tới quarantine với giá trị còn nguyên làm bằng chứng, nên
    hai cột enum cố ý KHÔNG dùng clean_enum (whitelist của nó đã phán xét).
    """
    csv = (raw_orders_dir(scale) / f"orders_{day}.csv").as_posix()
    con.execute(f"""
    CREATE OR REPLACE TABLE staging.shopcore_orders_day AS
    SELECT
        order_id,
        {clean_id('customer_id')}                           AS customer_id,
        store_id,
        {clean_order_ts('order_ts')}                        AS order_ts,
        updated_at,
        NULLIF(LOWER(TRIM(status)), '')                     AS status,
        NULLIF(LOWER(TRIM(payment_method)), '')             AS payment_method,
        {clean_money('order_total')}                        AS order_total,
        CAST(0 AS DECIMAL(14,2))                            AS discount_amount,
        items,
        meta,
        DATE '{day}'                                        AS _data_date,
        CAST(NULL AS BIGINT)                                AS _run_id
    FROM read_csv('{csv}', header=true, columns={V1_COLS});
    """)
    return con.execute(
        "SELECT count(*) FROM staging.shopcore_orders_day").fetchone()[0]


# ── Task 4 + 5 ───────────────────────────────────────────────────────────
def flag_rows(con: duckdb.DuckDBPyConnection, day: str, scale: str) -> None:
    """Dựng TEMP items_agg rồi TEMP flagged; mỗi dòng mang MỌI lý do nó hỏng."""
    products = (raw_dims_dir(scale) / "products.csv").as_posix()
    customers = (raw_dims_dir(scale) / "customers.csv").as_posix()
    stores = (raw_dims_dir(scale) / "stores.csv").as_posix()

    # (order_id, updated_at) chứ không phải order_id: ~0,05% id lặp trong 1 file.
    # unnest không ngồi chung SELECT với GROUP BY -> bung ở truy vấn con.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE items_agg AS
    SELECT order_id, updated_at,
           round(sum(it.qty * it.unit_price), 2) AS items_total,
           count(*) FILTER (p.sku IS NULL)       AS n_orphan_sku
    FROM (
        SELECT order_id, updated_at,
               unnest(CAST(items AS JSON)::{ITEM_STRUCT}) AS it
        FROM staging.shopcore_orders_day
    ) LEFT JOIN read_csv('{products}', header=true) p ON it.sku = p.sku
    GROUP BY 1, 2;
    """)

    # concat_ws bỏ qua NULL; NULLIF(...,'') để dòng sạch có reject_reason NULL.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE flagged AS
    SELECT s.*, i.items_total,
           NULLIF(concat_ws('; ',
             CASE WHEN s.order_id IS NULL THEN 'null_order_id' END,
             CASE WHEN s.order_ts IS NULL THEN 'bad_order_ts' END,
             -- range clause: [business_date - 7d, business_date + 1d);
             -- GREATEST chỉ tôn trọng mốc bắt đầu kỷ nguyên dữ liệu
             CASE WHEN s.order_ts IS NOT NULL AND NOT (
                    s.order_ts >= GREATEST(DATE '2026-06-01',
                                           DATE '{day}' - INTERVAL 7 DAY)
                AND s.order_ts <  DATE '{day}' + INTERVAL 1 DAY)
                  THEN 'ts_out_of_range' END,
             CASE WHEN s.status IS NULL OR s.status NOT IN {STATUS_WHITELIST}
                  THEN 'bad_status' END,
             CASE WHEN s.payment_method IS NULL OR s.payment_method NOT IN
                  ('card','wallet','bank_transfer','cod','paypal')
                  THEN 'bad_payment' END,
             CASE WHEN s.order_total IS NULL THEN 'null_total' END,
             CASE WHEN s.order_total < 0 THEN 'negative_total' END,
                          -- cross-field, dung sai một xu; chặn >= 0 để total âm bị cờ MỘT lần.
             -- 2.0.0 đổi NGHĨA của order_total: nó là tổng items TRỪ discount.
             -- Ở đây discount_amount luôn bằng 0 vì stage_day đọc V1_COLS —
             -- với era v1 đó là GIÁ TRỊ ĐÚNG, không phải giá trị tạm (cùng lý
             -- lẽ với ALIGN['v1'] của A10). Viết công thức đầy đủ ngay từ giờ
             -- để lúc A11 nối bộ check này vào staging.shopcore_orders_canon
             -- thì không phải sửa lại biểu thức nào.
             CASE WHEN s.order_total >= 0 AND i.items_total IS NOT NULL
                   AND abs(s.order_total - (i.items_total - s.discount_amount)) > 0.01
                  THEN 'total_mismatch' END,
             -- customer_id NULL là khách vãng lai: contract ghim nullable: true,
             -- nên đây là KHOẢNG TRỐNG, không phải vi phạm (A06 phân loại)
             CASE WHEN s.customer_id IS NULL THEN 'null_customer' END,
             CASE WHEN s.customer_id IS NOT NULL AND c.customer_id IS NULL
                  THEN 'orphan_customer' END,
             CASE WHEN st.store_id IS NULL THEN 'orphan_store' END,
             CASE WHEN i.n_orphan_sku > 0 THEN 'orphan_sku' END
           ), '') AS reject_reason
    FROM staging.shopcore_orders_day s
    LEFT JOIN items_agg i USING (order_id, updated_at)
    LEFT JOIN read_csv('{customers}', header=true) c ON s.customer_id = c.customer_id
    LEFT JOIN read_csv('{stores}',    header=true) st ON s.store_id   = st.store_id;
    """)


# ── Task 7 ───────────────────────────────────────────────────────────────
def report(con: duckdb.DuckDBPyConnection, day: str, scale: str, staged: int,
           bad: int, pct: float, thresholds: dict[str, float]) -> None:
    """Một dòng mỗi (ngày, check) — KỂ CẢ check 0 lỗi. Chạy TRƯỚC gate."""
    manifest = (manifest_dir(scale) / f"orders_{day}.json").as_posix()
    con.execute("""
    CREATE TABLE IF NOT EXISTS ops.dq_report (
      run_at TIMESTAMP, order_date DATE, check_type VARCHAR, check_name VARCHAR,
      rows_checked BIGINT, rows_failed BIGINT, pct_failed DOUBLE,
      threshold_pct DOUBLE, passed BOOLEAN);
    """)
    check_rows = ", ".join(f"('{c}', {thresholds.get(c, 'NULL')})"
                           for c in ROW_CHECKS)
    con.execute(f"""
    INSERT INTO ops.dq_report
    WITH observed AS (
      SELECT reason, count(*) AS n FROM (
        SELECT unnest(string_split(reject_reason, '; ')) AS reason
        FROM flagged WHERE reject_reason IS NOT NULL)
      GROUP BY 1
    )
    SELECT current_timestamp, DATE '{day}', 'row', c.check_name,
           {staged}, coalesce(o.n, 0),
           round(100.0 * coalesce(o.n, 0) / {staged}, 3) AS pct,
           c.threshold_pct,
           coalesce(pct <= c.threshold_pct, true)
    FROM (VALUES {check_rows}) c(check_name, threshold_pct)
    LEFT JOIN observed o ON o.reason = c.check_name;
    """)

    # aggregate: cặp (order_id, updated_at) phải duy nhất
    con.execute(f"""
    INSERT INTO ops.dq_report
    SELECT current_timestamp, DATE '{day}', 'aggregate',
           'unique_order_id_updated_at',
           {staged}, count(*), round(100.0 * count(*) / {staged}, 3), 0.0,
           count(*) = 0
    FROM (SELECT order_id, updated_at FROM staging.shopcore_orders_day
          GROUP BY 1, 2 HAVING count(*) > 1);
    """)

    # aggregate: order_id LẶP trong một file — bản sửa trong ngày, chuyện bình
    # thường. Informational: báo cáo tỉ lệ, KHÔNG loại dòng nào. A08 gấp lại.
    con.execute(f"""
    INSERT INTO ops.dq_report
    SELECT current_timestamp, DATE '{day}', 'aggregate', 'dup_order_id',
           {staged}, count(*), round(100.0 * count(*) / {staged}, 3), NULL, true
    FROM (SELECT order_id FROM staging.shopcore_orders_day
          GROUP BY order_id HAVING count(*) > 1);
    """)

    # reconciliation: số của ta vs lời khai của producer — check rẻ nhất, giá trị nhất
    con.execute(f"""
    INSERT INTO ops.dq_report
    SELECT current_timestamp, DATE '{day}', 'reconciliation',
           'rowcount_vs_manifest',
           {staged}, abs({staged} - rows), NULL, 0.0, {staged} = rows
    FROM read_json_auto('{manifest}');
    """)

    # aggregate: tỉ lệ chung cả NGÀY. threshold NULL vì contract cấp ngân sách
    # theo RULE chứ không theo ngày. Đây là dòng biểu đồ xu hướng dựng lên.
    con.execute(f"""
    INSERT INTO ops.dq_report
    SELECT current_timestamp, DATE '{day}', 'aggregate', 'quarantine_rate',
           {staged}, {bad}, round({pct}, 3), NULL, true;
    """)


# ── Task 6, bước 1 ───────────────────────────────────────────────────────
def quarantine_bad(con: duckdb.DuckDBPyConnection, day: str, scale: str) -> None:
    """Ghi bằng chứng LUÔN LUÔN, kể cả vào ngày tệ nhất — nhất là ngày tệ nhất."""
    target = quarantine_dir(scale) / "orders"
    target.mkdir(parents=True, exist_ok=True)   # COPY tạo folder partition,
                                                # KHÔNG tạo folder cha
    con.execute(f"""
    COPY (
      SELECT *, now() AT TIME ZONE 'UTC' AS _rejected_at,
             COALESCE(CAST(order_ts AS DATE), DATE '{day}') AS order_date
      FROM flagged WHERE reject_reason IS NOT NULL
    ) TO '{target.as_posix()}'
      (FORMAT parquet, PARTITION_BY (order_date), OVERWRITE_OR_IGNORE,
       FILENAME_PATTERN 'from_{day}_{{i}}');
    """)
    # FILENAME_PATTERN: dòng xấu đến trễ rơi vào phân vùng order_date cũ hơn, nên
    # nhiều ngày cùng ghi vào một folder; tên mặc định sẽ để D+1 đè lên D.


# ── Task 6, bước 2 ───────────────────────────────────────────────────────
def check_gate(con: duckdb.DuckDBPyConnection, day: str, staged: int,
               thresholds: dict[str, float]) -> None:
    """So tỉ lệ từng check với tolerance của contract. Vượt -> alert + raise."""
    con.execute("""
    CREATE TABLE IF NOT EXISTS ops.alerts (
      alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
      source VARCHAR, message VARCHAR);
    """)
    rate = dict(con.execute(f"""
        SELECT reason, round(100.0 * count(*) / {staged}, 3) FROM (
          SELECT unnest(string_split(reject_reason, '; ')) AS reason
          FROM flagged WHERE reject_reason IS NOT NULL) GROUP BY 1""").fetchall())
    breaches = [(c, rate.get(c, 0.0), t)
                for c, t in thresholds.items() if rate.get(c, 0.0) > t]
    if not breaches:
        return
    for check, got, limit in breaches:
        msg = (f"DQ HARD FAIL {day}: {check} at {got}% "
               f"exceeds contract tolerance {limit}%")
        con.execute("INSERT INTO ops.alerts VALUES "
                    "(current_timestamp, NULL, 'error', 'dq_gate', ?)", [msg])
        print("ALERT:", msg)
    # RuntimeError kiêm exit code khác 0: trong production, đây là chỗ pager réo.
    raise RuntimeError(f"DQ HARD FAIL {day}: {len(breaches)} contract "
                       f"tolerance(s) breached. Nothing loaded to core.")


# ── Task 6, bước 3 ───────────────────────────────────────────────────────
def load_good(con: duckdb.DuckDBPyConnection) -> None:
    """Chỉ dòng sạch mới vào core.

    grain: một dòng = một PHIÊN BẢN được giao của một đơn (order_id, _data_date).
    KHÔNG rerun-safe — chạy hai lần thì ngày đó nằm hai lần. Cho tới A07:
    chạy lại = DROP TABLE core.orders rồi nạp lại.
    """
    con.execute("""
    CREATE TABLE IF NOT EXISTS core.orders AS
    SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
    FROM flagged WHERE 1 = 0;
    """)
    con.execute("""
    INSERT INTO core.orders
    SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
    FROM flagged WHERE reject_reason IS NULL;
    """)


# # ── Task 8 ───────────────────────────────────────────────────────────────
# def validate_day(con: duckdb.DuckDBPyConnection, day: str, scale: str = "small",
#                  thresholds: dict[str, float] | None = None) -> None:
#     """Một ngày, đầu đến cuối. Thứ tự các lời gọi CHÍNH LÀ bài học của A05."""
#     thresholds = load_thresholds() if thresholds is None else thresholds
#     staged = stage_day(con, day, scale)
#     flag_rows(con, day, scale)
#     bad = con.execute("SELECT count(reject_reason) FROM flagged").fetchone()[0]
#     pct = 100.0 * bad / staged
#     report(con, day, scale, staged, bad, pct, thresholds)   # trước gate: dòng
#     quarantine_bad(con, day, scale)                         # report phải sống sót
#     check_gate(con, day, staged, thresholds)                # kể cả khi run chết
#     load_good(con)
#     print(f"{day}: staged={staged} core+={staged - bad} "
#           f"quarantined={bad} ({pct:.2f}%)")

# ── Task 8 ───────────────────────────────────────────────────────────────
def validate_day(con: duckdb.DuckDBPyConnection, day: str, scale: str = "small",
                 thresholds: dict[str, float] | None = None,
                 csv_path: Path | None = None) -> None:
    """Một ngày, đầu đến cuối. Thứ tự các lời gọi CHÍNH LÀ bài học của A05."""
    from pathlib import Path
    from work.a06_data_contract.a06_contract_check import load_contract, check_file

    if csv_path is None:
        csv_path = raw_orders_dir(scale) / f"orders_{day}.csv"

    problems = check_file(csv_path, load_contract())
    if problems:
        for msg in problems:
            print(" -", msg)
        raise SystemExit(f"[contract gate] {csv_path.name} FAILED - nothing was loaded.")
    print(f"[contract gate] {csv_path.name} PASS")

    thresholds = load_thresholds() if thresholds is None else thresholds
    staged = stage_day(con, day, scale)
    flag_rows(con, day, scale)
    bad = con.execute("SELECT count(reject_reason) FROM flagged").fetchone()[0]
    pct = 100.0 * bad / staged
    report(con, day, scale, staged, bad, pct, thresholds)   # trước gate: dòng
    quarantine_bad(con, day, scale)                         # report phải sống sót
    check_gate(con, day, staged, thresholds)                # kể cả khi run chết
    load_good(con)
    print(f"{day}: staged={staged} core+={staged - bad} "
          f"quarantined={bad} ({pct:.2f}%)")
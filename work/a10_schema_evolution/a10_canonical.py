"""A10 — canonical schema: 3 era -> 1 schema duy nhất.

Hai tầng, tách rời CÓ CHỦ Ý:
  1. ALIGNER (mỗi era một bản)  -> chỉ làm việc SCHEMA: rename, id, cột thiếu.
                                   Ra đúng 15 cột giống hệt nhau cho mọi era.
  2. CANON (dùng chung, 1 bản)  -> chỉ làm việc VALUE: cleaner của A03.
Logic-theo-era nằm gọn ở tầng 1. Hạ nguồn không bao giờ cần biết era.

Import: from work.a10_schema_evolution.a10_canonical import era_of, open_day, CANON
"""
from __future__ import annotations

from datetime import date
from pathlib import Path


# ── era ──────────────────────────────────────────────────────────────────────
def era_of(d: date | str) -> str:
    if isinstance(d, str):
        d = date.fromisoformat(d)
    if d <= date(2026, 7, 15):
        return "v1"
    if d <= date(2026, 7, 31):
        return "v2"
    return "v3"


# ── schema khai tường minh cho read_csv ──────────────────────────────────────
# CẢNH BÁO: columns= map theo VỊ TRÍ, không theo TÊN. Header chỉ bị bỏ qua,
# không được đối chiếu. v3 đã ĐỔI THỨ TỰ cột -> nếu bạn liệt kê theo thứ tự v2,
# DuckDB nạp chuỗi 'USD' vào cột items mà KHÔNG báo lỗi nào.
V1_COLS = {'order_id': 'BIGINT', 'customer_id': 'BIGINT', 'store_id': 'INTEGER',
           'order_ts': 'VARCHAR', 'updated_at': 'TIMESTAMP', 'status': 'VARCHAR',
           'payment_method': 'VARCHAR', 'order_total': 'VARCHAR',
           'items': 'VARCHAR', 'meta': 'VARCHAR'}

V2_COLS = dict(V1_COLS, **{'currency': 'VARCHAR',
                           'discount_amount': 'DECIMAL(14,2)',
                           'channel': 'VARCHAR'})

# TODO(you) — Task 5.
# Dựng từ HEADER THẬT bạn in ở Task 3, đúng thứ tự trong file.
# Gợi ý những gì đã đổi ở v3: order_id thành VARCHAR, payment_method đổi tên
# thành payment_type, thêm loyalty_tier, và thứ tự cột khác v2.
# Sau khi viết xong: đọc thử 1 dòng và spot-check 2 giá trị (items phải là JSON
# mảng, currency phải là 'USD'/... chứ không phải một mảng).
V3_COLS: dict[str, str] = {
    'order_id': 'VARCHAR',                 # v3 ép kiểu sang VARCHAR
    'customer_id': 'BIGINT',
    'store_id': 'INTEGER',
    'order_ts': 'VARCHAR',
    'updated_at': 'TIMESTAMP',
    'status': 'VARCHAR',
    'payment_type': 'VARCHAR',             # Đổi tên từ payment_method
    'order_total': 'VARCHAR',
    'currency': 'VARCHAR',                 # Đảo lên trước items, meta
    'discount_amount': 'DECIMAL(14,2)',
    'channel': 'VARCHAR',
    'loyalty_tier': 'VARCHAR',             # Cột mới bổ sung ở v3
    'items': 'VARCHAR',                    # Bị đẩy xuống áp chót
    'meta': 'VARCHAR'                      # Bị đẩy xuống cuối cùng
}

# ── aligner: 15 cột giống hệt nhau cho mọi era ───────────────────────────────
ALIGN = {
    "v1": """SELECT
    printf('ORD-%010d', order_id) AS order_id,
    order_id                      AS order_id_num,
    customer_id, store_id, order_ts, updated_at, status,
    payment_method, order_total,
    'USD'                    AS currency,          -- known value -> default
    CAST(0 AS DECIMAL(14,2)) AS discount_amount,   -- known: v1 chưa có discount
    CAST(NULL AS VARCHAR)    AS channel,           -- unknown  -> NULL
    CAST(NULL AS VARCHAR)    AS loyalty_tier,      -- unknown  -> NULL
    items, meta
  FROM src""",

    # TODO(you) — Task 5. Bắt đầu từ v1, đúng BA dòng đổi:
    # currency / discount_amount / channel từ default thành pass-through.
    # loyalty_tier vẫn NULL (v2 chưa có). order_id vẫn phải printf.
    "v2": """SELECT
    printf('ORD-%010d', order_id) AS order_id,
    order_id                      AS order_id_num,
    customer_id, store_id, order_ts, updated_at, status,
    payment_method, order_total,
    currency,                                      -- pass-through (v2 đã có sẵn)
    discount_amount,                               -- pass-through (v2 đã có sẵn)
    channel,                                       -- pass-through (v2 đã có sẵn)
    CAST(NULL AS VARCHAR)         AS loyalty_tier, -- v2 chưa có -> giữ NULL
    items, meta
  FROM src""",

    "v3": """SELECT
    order_id,
    CAST(replace(order_id, 'ORD-', '') AS BIGINT) AS order_id_num,
    customer_id, store_id, order_ts, updated_at, status,
    payment_type AS payment_method,                -- rename, gói lại tại đây
    order_total, currency, discount_amount, channel, loyalty_tier,
    items, meta
  FROM src""",
}


# ── canonicalizer dùng chung: cleaner A03, mù era ────────────────────────────
TS = ("try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',"
      "'%d/%m/%Y %H:%M:%S','%Y-%m-%dT%H:%M:%S'])")

CANON = f"""
SELECT
  order_id, order_id_num, customer_id, store_id,
  {TS} AS order_ts, updated_at, status, payment_method,
  TRY_CAST(NULLIF(
    CASE WHEN order_total LIKE '%,%'
         THEN replace(replace(order_total, '.', ''), ',', '.')
         ELSE replace(TRIM(order_total), '$', '') END,
    'N/A') AS DECIMAL(14,2)) AS order_total,
  currency, discount_amount, channel, loyalty_tier,
  TRY_CAST(CAST(items AS JSON) AS
    STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE, disc DOUBLE)[]) AS items,
  meta,
  CAST({TS} AS DATE) AS order_date
FROM aligned
WHERE {TS} IS NOT NULL
"""

# Dòng WHERE vứt các row không parse nổi timestamp (~0.02%): không có order_date
# thì không partition được. LUẬT: đếm chúng MỖI LẦN CHẠY. Một row bị bỏ mà
# không đếm là một lời nói dối trong phép cộng row.
DROPPED_SQL = f"SELECT count(*) FROM aligned WHERE {TS} IS NULL"


# ── mở nguồn ─────────────────────────────────────────────────────────────────
def open_day(con, day: str, raw_dir: Path, era: str | None = None) -> str:
    """Tạo view src (raw CSV 1 ngày) + view aligned (15 cột canonical-shape)."""
    era = era or era_of(day)
    cols = {"v1": V1_COLS, "v2": V2_COLS, "v3": V3_COLS}[era]
    if not cols:
        raise ValueError(f"schema cho era {era} chưa được khai (V3_COLS rỗng?)")
    f = (raw_dir / f"orders_{day}.csv").as_posix()
    con.execute(f"CREATE OR REPLACE VIEW src AS "
                f"SELECT * FROM read_csv('{f}', header=true, columns={cols})")
    con.execute(f"CREATE OR REPLACE VIEW aligned AS {ALIGN[era]}")
    return era


def open_lake_v1(con, lake_orders_dir: Path) -> None:
    """Task 7(a): lake month-1 đã có = một nguồn v1. Tái dùng đúng ALIGN['v1'].

    ĐÃ KIỂM: lake A02 giữ CẢ `order_ts` gốc lẫn `order_ts_clean`, nên ALIGN['v1']
    lấy `order_ts` rồi để CANON parse lại là chạy được, không cần sửa gì.
    Đề gợi ý pass-through `order_ts_clean` cho nhanh hơn — làm được, nhưng như
    vậy CANON có hai phiên bản (một cho migrate, một cho load). Giữ một nhánh
    code thay vì tiết kiệm vài giây: mọi dòng trong lake mới đi qua đúng một
    đường parse, nên không thể có hai cách hiểu timestamp trong cùng một bảng.
    """
    con.execute(f"""CREATE OR REPLACE VIEW src AS
      SELECT * FROM read_parquet('{lake_orders_dir.as_posix()}/*/*.parquet',
                                 hive_partitioning=true)""")
    con.execute(f"CREATE OR REPLACE VIEW aligned AS {ALIGN['v1']}")


if __name__ == "__main__":
    for d in ("2026-06-02", "2026-07-16", "2026-08-01"):
        print(d, "->", era_of(d))
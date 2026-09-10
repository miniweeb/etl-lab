# %% work/_explore/model_explore.py
"""Soi cấu trúc lake + warehouse. Không thuộc assignment nào.

Ba tầng đã dựng:

    Fact       core.order_items        A04-t3   grain: (order_id, line_no)
               core.orders             A03/A07  grain: (order_id)
    Dimension  core.customers          A03-t6b  grain: (customer_id)
               core.products           A03-t6c  grain: (sku)
               core.stores             A03-t6c  grain: (store_id)
    Mart       marts.category_daily_revenue  A04-t8  grain: (order_date, category)

Kết nối mở read_only: chạy song song với notebook khác được, không đụng dữ liệu.
Nhưng nếu pipeline đang giữ DB ở chế độ ghi thì DuckDB từ chối mở — đóng cái kia trước.
"""
import os
while not os.path.exists("lib") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print(os.getcwd())

# %%
import duckdb

from lib.labpaths import data_root, lake_dir, warehouse_path

SCALE = "small"                 # đổi "full" để soi kho lớn

ROOT = data_root(SCALE)
R = ROOT.as_posix()
LAKE = lake_dir(SCALE).as_posix()


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


def q(sql: str) -> None:
    """Bảng chưa tồn tại thì in lý do rồi đi tiếp, không giết cả cell."""
    try:
        con.sql(sql).show(max_rows=40)
    except Exception as e:
        print(f"  [bỏ qua] {type(e).__name__}: {str(e).splitlines()[0][:100]}")


con = duckdb.connect(str(warehouse_path(SCALE)), read_only=True)
print(f"scale={SCALE}  root={R}  db={warehouse_path(SCALE)}")

# %% ── Có gì trên đĩa ──────────────────────────────────────────────────
section("PATHS — thư mục nào tồn tại, nặng bao nhiêu")

from lib.labpaths import (manifest_dir, quarantine_dir, raw_dims_dir,
                          raw_orders_dir, tmp_dir)

for name, p in [("raw/orders", raw_orders_dir(SCALE)),
                ("raw/dims", raw_dims_dir(SCALE)),
                ("raw/manifest", manifest_dir(SCALE)),
                ("lake", lake_dir(SCALE)),
                ("warehouse", warehouse_path(SCALE).parent),
                ("quarantine", quarantine_dir(SCALE)),
                ("tmp", tmp_dir(SCALE))]:
    if not p.exists():
        print(f"  {name:14} MISSING  {p}")
        continue
    files = [f for f in p.rglob("*") if f.is_file()]
    mb = sum(f.stat().st_size for f in files) / 1024 / 1024
    print(f"  {name:14} {len(files):>5} file  {mb:>9.1f} MB  {p}")

# %% ── Lake: partition key nằm ở TÊN THƯ MỤC ───────────────────────────
section("LAKE — layout hive")

# Hive partitioning: order_date=2026-06-02/ là thư mục, không phải cột trong file.
# hive_partitioning=true bảo DuckDB đọc ngược tên thư mục ra thành cột — đó là
# thứ cho phép partition pruning ở A02.
for d in sorted(x for x in lake_dir(SCALE).iterdir() if x.is_dir()):
    subs = sorted(s.name for s in d.iterdir() if s.is_dir())
    print(f"  {d.name}/  — {len(subs)} partition")
    for s in subs[:3]:
        print(f"      {s}/")
    if len(subs) > 3:
        print(f"      ... còn {len(subs) - 3}")

# %%
section("LAKE — schema và số dòng của orders")

GLOB = f"{LAKE}/orders/**/*.parquet"

q(f"DESCRIBE SELECT * FROM read_parquet('{GLOB}', hive_partitioning=true)")

q(f"""
    SELECT order_date, count(*) AS rows, count(DISTINCT filename) AS files
    FROM read_parquet('{GLOB}', hive_partitioning=true, filename=true)
    GROUP BY 1 ORDER BY 1
""")

# %% ── Warehouse: bảng nào đang có ─────────────────────────────────────
section("WAREHOUSE — schema và bảng")

q("SELECT schema_name FROM duckdb_schemas() WHERE NOT internal ORDER BY 1")

q("""
    SELECT schema_name AS sch, table_name AS tbl,
       estimated_size AS est_rows, column_count AS cols
    FROM duckdb_tables() WHERE NOT internal ORDER BY 1, 2
""")
# estimated_size là ƯỚC LƯỢNG từ catalog, không phải count(*). Xem cho nhanh,
# đừng dùng để đối chiếu.

# %%
section("COLUMNS — mọi cột của core và marts")

q("""
    SELECT schema_name AS sch, table_name AS tbl,
       column_index AS pos, column_name AS col, data_type AS typ
       FROM duckdb_columns()
       WHERE schema_name IN ('core', 'marts')
       ORDER BY 1, 2, 3
""")

# %%
section("LINEAGE — bảng nào mang _data_date, _run_id")

# A03 quyết định giữ đúng hai cột này và từ chối _source_file, _loaded_at.
# A07 làm _run_id sống dậy cùng ops.etl_runs. Bảng core thiếu chúng là bảng
# không truy ngược được về lần chạy nào đã sinh ra nó.
q("""
    SELECT schema_name AS sch, table_name AS tbl,
       bool_or(column_name = '_data_date') AS has_data_date,
       bool_or(column_name = '_run_id')    AS has_run_id
       FROM duckdb_columns()
       WHERE schema_name IN ('core', 'marts', 'staging')
       GROUP BY 1, 2 ORDER BY 1, 2
""")

# %% ── Grain: phép kiểm tra quan trọng nhất ────────────────────────────
section("GRAIN — rows phải bằng uniq_key")

# Lệch nghĩa là một trong hai: join nhân dòng (fan-out), hoặc cùng một khoá
# được nạp hai lần. Cùng kỷ luật với phép bảo toàn của A04-t3b.
#
# LƯU Ý core.order_items: A04 dựng nó từ bảng ngày CHƯA khử trùng, nên
# (order_id, line_no) CHƯA duy nhất — đó là chuyện A08 xử lý, không phải bug.
GRAIN = [
    ("core.order_items", "order_id, line_no"),
    ("core.orders", "order_id"),
    ("core.customers", "customer_id"),
    ("core.products", "sku"),
    ("core.stores", "store_id"),
    ("marts.category_daily_revenue", "order_date, category"),
]

for tbl, key in GRAIN:
    q(f"""
        SELECT '{tbl}' AS tbl, '{key}' AS grain_key,
               count(*) AS rows,
               count(DISTINCT ({key})) AS uniq_key,
               count(*) - count(DISTINCT ({key})) AS dup
        FROM {tbl}
    """)

# %% ── Khoá mồ côi: fact trỏ sang dimension ────────────────────────────
section("FK — dòng fact không tìm thấy dimension")

# LEFT chứ không INNER. INNER âm thầm LOẠI dòng mồ côi, doanh thu co lại,
# không ai hay — đúng cái bẫy A04-t5.
q("""
    SELECT 'order_items.sku -> products' AS check,
           count(*) FILTER (WHERE p.sku IS NULL) AS orphan,
           count(*) AS total
    FROM core.order_items i LEFT JOIN core.products p USING (sku)
""")

q("""
    SELECT 'orders.customer_id -> customers' AS check,
           count(*) FILTER (WHERE c.customer_id IS NULL) AS orphan,
           count(*) AS total
    FROM core.orders o LEFT JOIN core.customers c USING (customer_id)
""")

q("""
    SELECT 'orders.store_id -> stores' AS check,
           count(*) FILTER (WHERE s.store_id IS NULL) AS orphan,
           count(*) AS total
    FROM core.orders o LEFT JOIN core.stores s USING (store_id)
""")

# %% ── Mart có khớp fact không ─────────────────────────────────────────
section("RECON — tổng của mart phải bằng tổng của fact")

q("""
    SELECT
      (SELECT round(sum(qty * unit_price), 2) FROM core.order_items) AS from_fact,
      (SELECT round(sum(revenue), 2) FROM marts.category_daily_revenue) AS from_mart
""")
# Lệch thì mart đang lọc mất gì đó. Thủ phạm hay gặp nhất nằm ngay dưới đây.

q("""
    SELECT order_date IS NULL AS date_is_null,
           count(*) AS rows, round(sum(revenue)) AS revenue
    FROM marts.category_daily_revenue GROUP BY 1 ORDER BY 1
""")
# Nhóm NULL là ~0,02% mốc thời gian bất khả thi mà A03 cho về NULL. Ai đó thêm
# WHERE order_date IS NOT NULL vào mart là số biến mất — im lặng.

q("""
    SELECT order_date, count(*) AS categories, round(sum(revenue)) AS revenue
    FROM marts.category_daily_revenue GROUP BY 1 ORDER BY 1
""")
# Ba nhóm ngày từ MỘT file: ngày của file, ~1,5% bản sửa trễ về ngày trước,
# và NULL. Một file != một ngày.

# %% ── Soi sâu một bảng ────────────────────────────────────────────────
section("SUMMARIZE — đổi TBL rồi chạy lại cell này")

TBL = "core.order_items"

q(f"DESCRIBE {TBL}")
q(f"SELECT count(*) AS rows FROM {TBL}")
q(f"SUMMARIZE {TBL}")
# SUMMARIZE cho min/max/approx_unique/null_percentage từng cột trong một lần quét.
# Nhanh hơn viết tay chục câu count(*) FILTER.
q(f"FROM {TBL} LIMIT 5")

# %% ── ops: dấu vết vận hành ───────────────────────────────────────────
section("OPS — bảng vận hành của A05/A07/A08")

q("SELECT table_name, estimated_size FROM duckdb_tables() "
  "WHERE schema_name = 'ops' ORDER BY 1")

for t in ("etl_runs", "dq_report", "cleaning_report", "alerts"):
    print(f"\n-- ops.{t}")
    q(f"FROM ops.{t} ORDER BY 1 DESC LIMIT 5")

# %%
con.close()
print("xong.")
# %%

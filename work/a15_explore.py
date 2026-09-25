# %% [markdown]
# # A15 — dbt: bài tổng kết
#
# Khác A13 và A14: hôm nay **gần như mọi thứ chạy ở terminal**, vì sản phẩm của
# bài là một dự án dbt, không phải một script Python. File này là **sổ kiểm** —
# nó chạy các câu query nghiệm thu sau mỗi lần `dbt build`, và in ra lệnh cần gõ.
#
# **Mọi lệnh dbt phải chạy từ trong `dbt_lab/`** — dbt tìm cấu hình ngay tại chỗ
# bạn đứng. Lỗi "Could not find profile" gần như luôn nghĩa là sai thư mục, chứ
# không phải YAML hỏng.
#
# **Đóng kernel này trước khi chạy `dbt build`.** dbt ghi vào
# `warehouse.duckdb`; một tiến trình, một writer — luật A12. Các cell dưới đây
# đều mở, đọc, rồi `close()` ngay trong cùng cell, nhưng kernel còn sống thì
# vẫn có rủi ro.
#
# | Task | Ở đâu | Scale |
# |---|---|---|
# | 1 Dựng khung | terminal (`dbt debug`) | cả hai |
# | 2 Source + freshness | cell 2 in lệnh | small |
# | 3 Seed + staging | terminal, rồi cell 3 | small |
# | 4 Dimension | terminal, rồi cell 4 | small |
# | 5 fct_orders | terminal, rồi cell 5 | small |
# | 6 Items + marts | terminal, rồi cell 6 | small |
# | 7 Test | terminal (`dbt build`) | small |
# | 8 Backfill theo cửa sổ | cell 7 in lệnh, cell 8 kiểm | small |
# | 9 Full scale | cell 9 in lệnh | full |
# | 10, 11 Mô tả + catalog | terminal (`dbt docs`) | — |
# | 12, 13 Lineage | cell 10, 11 | — |
# | 14, 15 Thiết kế + bản đồ | giấy bút, cell 12 gợi ý | — |

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Chỉ đọc warehouse, không bao giờ giữ connection.
# ─────────────────────────────────────────────────────────────────────────────
import os
import subprocess
from pathlib import Path

p = Path.cwd()
while not (p / "lib" / "labpaths.py").exists():
    if p.parent == p:
        raise RuntimeError("Không tìm thấy repo root (lib/labpaths.py).")
    p = p.parent
os.chdir(p)

import duckdb                                                   # noqa: E402
from lib.labpaths import warehouse_path                          # noqa: E402

SCALE = "small"          # đổi thành "full" ở Task 9
DBT = Path.cwd() / "dbt_lab"


def q(sql: str):
    """Mở, đọc, đóng — trong cùng một lời gọi. Không giữ file quá một câu query."""
    con = duckdb.connect(str(warehouse_path(SCALE)), read_only=True)
    try:
        return con.sql(sql)
    finally:
        con.close()


def show(sql: str) -> None:
    print(q(sql))


print("repo :", Path.cwd())
print("dbt  :", DBT, "->", "OK" if (DBT / "dbt_project.yml").exists() else "CHƯA CÓ")
print("wh   :", warehouse_path(SCALE))
print("ETL_LAB_DATA =", os.environ.get("ETL_LAB_DATA", "CHƯA ĐẶT — dbt sẽ báo lỗi"))
#   dbt CỐ TÌNH báo lỗi khi env_var() thiếu, nên cái đường lui <repo>/data của
#   labpaths sẽ không cứu bạn ở đây.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Task 1 và 2: lệnh terminal.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
cd dbt_lab          # MỌI lệnh dbt chạy từ đây

# Task 1:
dbt debug
dbt debug --target full

# Task 2 — thử source, rồi gặp freshness:
dbt show --inline "select count(*) as lake_rows from {{ source('shopcore','orders') }}"
dbt source freshness
""")
print("""Task 2 kỳ vọng: ĐÚNG 4.116.024 — bằng manifest 4.116.840 trừ 816 dòng
timestamp không parse nổi mà pipeline A10/A11 đã bỏ VÀ ĐÃ ĐẾM. Chính đẳng thức
Task 7 của A10, quay lại.

freshness tính max(updated_at) — small ra 2026-08-10 23:31:14 — rồi so với BÂY
GIỜ, nên kết quả phụ thuộc ngày bạn chạy: trong 7 ngày kể từ mốc đó thì WARN,
muộn hơn thì ERROR. CẢ HAI ĐỀU ĐÚNG. Feed này dừng thật vào 2026-08-08, bản sửa
cuối về ngày 10. Một feed đã chết thì PHẢI đỏ.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Task 3 nghiệm thu: seed + staging.
#   dbt build --select staging country_map
# ─────────────────────────────────────────────────────────────────────────────
print("Task 3 — kỳ vọng: 5/5 pass, 60.000 khách với ĐÚNG 548 country NULL\n")
show("""
    SELECT count(*)                                  AS customers,
           count(*) FILTER (country IS NULL)         AS country_null,
           count(*) FILTER (email_hash IS NULL)      AS hash_null
    FROM staging.stg_shopcore__customers""")
print("""country_null > 548 nghĩa là SEED CÒN THIẾU biến thể. Mở bảng
staging.shopcore_country_map của A03 ra lấy nốt DE, FR, JP, AU, CA, BR, IN.""")
show("""
    SELECT count(*) AS products,
           count(*) FILTER (unit_price IS NULL) AS price_null
    FROM staging.stg_shopcore__products""")
print("price_null phải là 0.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Task 4 nghiệm thu: dimension.
#   dbt build --select core
# ─────────────────────────────────────────────────────────────────────────────
print("Task 4 — kỳ vọng: 60.000 / 2.000 / 400, và tags kiểu VARCHAR[]\n")
show("""
    SELECT (SELECT count(*) FROM core.dim_customers) AS customers,
           (SELECT count(*) FROM core.dim_products)  AS products,
           (SELECT count(*) FROM core.dim_stores)    AS stores""")
show("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'core' AND table_name = 'dim_products'
      AND column_name IN ('tags', 'weight_g', 'unit_price')""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Task 5 nghiệm thu: fct_orders.
#   dbt build --select fct_orders
# ─────────────────────────────────────────────────────────────────────────────
print("Task 5 — kỳ vọng: 4.053.208 dòng\n")
show("""
    SELECT count(*)                       AS rows,
           count(DISTINCT order_id)       AS distinct_orders,
           4116024 - count(*)             AS folded
    FROM core.fct_orders""")
print("""folded = 62.816 dòng trùng lặp và bản sửa đã gộp lại — chính phần khử
trùng lặp XUYÊN FILE mà bạn đo ở A08. Ghi con số đó vào journal.

Rồi mở target/run/etl_lab_dbt/models/core/fct_orders.sql. Ở lần chạy ĐẦU nó chỉ
là CREATE TABLE trên toàn bộ lịch sử, KHÔNG có delete+insert nào, vì
is_incremental() trả false. Sau Task 8 mở LẠI đúng file đó — cặp DELETE/INSERT
của A07 sẽ nằm sẵn trong đấy. Tận mắt nhìn cả hai.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Task 6 nghiệm thu: items + marts.
#   dbt build
# ─────────────────────────────────────────────────────────────────────────────
print("Task 6 — kỳ vọng: 10.253.421 / 50.282 / 1.990 (143 dòng category NULL)\n")
show("""
    SELECT (SELECT count(*) FROM core.fct_order_items)        AS items,
           (SELECT count(*) FROM marts.daily_store_sales)     AS store_daily,
           (SELECT count(*) FROM marts.category_daily_revenue) AS cat_daily,
           (SELECT count(*) FROM marts.category_daily_revenue
            WHERE category IS NULL)                            AS cat_null""")
print("""~2,53 dòng hàng mỗi đơn — đối chiếu lại A04 cho chắc.
cat_null = 0 nghĩa là bạn đã lỡ dùng INNER JOIN: ~0,2% SKU mồ côi của A04 phải
rơi vào một category NULL NHÌN THẤY ĐƯỢC, chứ không được biến mất.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Task 8: bài tập backfill. Lệnh terminal.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
# 1) nền: build toàn phần (đã có từ Task 7)
# 2) cửa sổ 3 ngày, đúng khuôn --start/--end của A11:
dbt build --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"
# 3) chạy LẠI đúng lệnh đó — bài kiểm chạy-hai-lần của A07:
dbt build --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"

# hai biến thể:
dbt build --select fct_orders+ --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"
dbt build --full-refresh
""")
print("""fct_orders+ nghĩa là "và mọi thứ phía dưới" — bản mổ chính xác, dims và
staging không bị đụng. --full-refresh chính là --force của A11: một lần
restatement, kèm tất cả những gì chữ đó hàm ý về việc phải báo trước.

GHI VÀO JOURNAL TRƯỚC KHI BỊ CẮN: khoản dữ liệu về muộn vẫn còn hiệu lực. Khi
file ngày D về tới lake, các phân vùng D-7 tới D đều thay đổi (contract A06, đo
ở A08). Cửa sổ chạy hằng ngày ĐÚNG là [D-7, D], không bao giờ chỉ mỗi D.
dbt hình thức hoá cái pipeline của bạn, KHÔNG hình thức hoá phép tính lịch.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Task 8 nghiệm thu: số đếm phải KHÔNG ĐỔI sau khi chạy lại.
# ─────────────────────────────────────────────────────────────────────────────
print("Task 8 — kỳ vọng: 4.053.208 / 4.053.208, và ba ngày giữ nguyên số\n")
show("SELECT count(*) AS rows, count(DISTINCT order_id) AS uniq FROM core.fct_orders")
show("""
    SELECT order_date, count(*) AS rows
    FROM core.fct_orders
    WHERE order_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-03'
    GROUP BY 1 ORDER BY 1""")
print("""Kỳ vọng: 08-01: 46.273 · 08-02: 45.874 · 08-03: 56.940

Số đếm y hệt sau lần chạy lại = tính idempotent của delete+insert. Thứ bạn tự
tay dựng ở A07, ở đây tốn đúng một dòng config().""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Task 9, 10, 11: full scale, mô tả, catalog. Lệnh terminal.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
# Task 9 — 10 tới 30 phút cho lần build toàn phần đầu tiên:
dbt debug --target full
dbt build --target full
dbt build --target full --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"

# Task 11 — sau khi đã viết mô tả ở Task 10:
dbt docs generate
dbt docs serve          # http://localhost:8080, Ctrl+C để dừng
""")
print("""Task 9: xem Task Manager — bộ nhớ nằm dưới trần 8GB, và spill có thể
xuất hiện trong <DATA_ROOT>/tmp, đúng như A13 dạy bạn chờ đợi. Ghi cả ba mốc
thời gian: build toàn phần, chạy cửa sổ, chạy cửa sổ lần nữa.

Task 11: lệnh generate kết thúc bằng "Catalog written to target/catalog.json".
dbt dùng đúng chữ catalog, và nó nói thật. Hai file nuôi trang đó:
manifest.json giữ những gì BẠN KHAI (mô tả, test, phụ thuộc), catalog.json giữ
những gì KHO DỮ LIỆU BÁO VỀ (kiểu cột thật). Trang web là phép join của hai —
và đó là lý do Task 10 phải làm TRƯỚC: docs generate XUẤT BẢN metadata của bạn
chứ không BỊA ra nó.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — Task 12: lineage mức bảng. Chạy dbt ls và bắt output.
# ─────────────────────────────────────────────────────────────────────────────
print("Task 12 — bán kính ảnh hưởng nếu shopcore bỏ cột channel\n")
r = subprocess.run(
    ["dbt", "ls", "--quiet", "--select", "source:shopcore.orders+"],
    cwd=DBT, capture_output=True, text=True, shell=True)
#   --quiet in RA MỖI node. Thiếu nó thì ba dòng log khởi động của dbt cũng rơi
#   vào stdout và phá hỏng mọi phép đếm bạn nối vào sau.
nodes = [l for l in r.stdout.splitlines() if l.strip()]
models = [n for n in nodes if ".models." in n or n.count(".") >= 2 and "test" not in n]
print(f"{len(nodes)} node phía dưới source:\n")
for n in nodes:
    print("  ", n)
print(f"""
Kỳ vọng 15 node: 5 model + bản thân source + 9 test. Giống nhau ở cả hai target
vì lệnh này đọc CẤU TRÚC DỰ ÁN, không đọc dữ liệu.

Thư trả lời shopcore có ba phần, và không phần nào là "ừ cứ làm đi":
  1. bán kính, lấy từ DAG — gọi tên năm model.
  2. contract — bỏ cột là MAJOR theo change_management: 30 ngày báo trước, bump
     version, ghi chú di trú, thông báo đúng kênh. Cổng tiền-nạp của A06 sẽ làm
     đỏ file đầu tiên thiếu cột, và đó là hành vi ĐÚNG.
  3. thứ cần nhận lại — một NGÀY HIỆU LỰC, để cho cột nghỉ hưu khỏi staging SAU
     file cuối cùng còn mang nó. Đúng kiểu xử lý ranh giới era của A10.

GIỚI HẠN TRUNG THỰC, và là toàn bộ lý do Task 13 tồn tại: dbt ls cho biết model
nào đọc cái NGUỒN orders. Nó KHÔNG cho biết model nào đọc cái CỘT channel.
Lineage mức bảng luôn báo dư — an toàn, nhưng cùn.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — Task 13: lineage mức cột, LÀM BẰNG TAY. dbt Core không vẽ tầng này.
#
# Đây không phải cell chạy ra đáp án — nó là cái khuôn để bạn điền, bằng cách
# ĐỌC BỐN FILE. Chính là việc một catalog thương mại bán tự động hoá.
# ─────────────────────────────────────────────────────────────────────────────
print("""Task 13 — lần theo order_total, ghi từng chặng kèm BIỂU THỨC:

1  shopcore.orders (lake)             DECIMAL(14,2), đã sạch — bộ dọn $ / N/A /
                                      dấu phẩy thập phân của A03 chạy TRƯỚC Parquet
2  stg_shopcore__orders.order_total   đi qua, không đụng gì
3  fct_orders.order_total             chỉ sống sót trên dòng THẮNG của cửa sổ khử
                                      trùng lặp — một bản sửa THAY THẾ giá trị này
4  daily_store_sales.gross_revenue    sum(order_total) theo (order_date, store_id, currency)
4' daily_store_sales.avg_order_value  round(avg(order_total), 2)
4" daily_store_sales.orders_missing_total  count(*) filter (order_total is null)
4' tests/assert_order_totals_reconcile     so với sum(qty*unit_price) - discount_amount

PHẦN THƯỞNG: marts.category_daily_revenue CÓ TÊN trong danh sách năm model của
Task 12, mà KHÔNG xuất hiện ở bất cứ đâu trong bảng trên. Nó tính doanh thu từ
qty * unit_price trên dòng hàng; order_total không hề chạm tới nó.
Lineage mức bảng nói "có nguy cơ". Lineage mức cột nói "không đụng gì".

GIỜ LÀM Y VẬY VỚI channel — cột shopcore thật sự hỏi:
  channel -> stg_shopcore__orders -> fct_orders -> DỪNG.
  Không mart nào select nó. Không test nào kiểm nó.
  Đáp án thật: HAI model phải sửa, không mart nào đổi đầu ra, không test nào đổi.
  Một việc hai chục phút, không phải một dự án.

Đó là một lá thư rất khác so với lá thư mà riêng cái DAG sẽ khiến bạn viết.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — Task 14: bản thiết kế mart returns. GIẤY BÚT TRƯỚC.
#
# ĐỪNG chạy cell này trước khi điền xong sáu đề mục trong journal. Đọc đáp án
# rồi thì nó trông hiển nhiên, mà cảm giác hiển nhiên đó chẳng dạy được gì.
# ─────────────────────────────────────────────────────────────────────────────
print("""Yêu cầu: "Sản phẩm nào bị trả lại nhiều nhất, theo vùng và theo tháng?
Mình muốn xem hằng tháng luôn."

Sáu đề mục: (1) Grain (2) Khoá (3) Dimension (4) Measure — additive hay không,
nói rõ vì sao (5) Chiến lược làm mới (6) Tái sử dụng.
Và MỘT DÒNG trên cả sáu: câu hỏi đó THẬT RA nghĩa là gì? Có BA chữ mơ hồ —
nhận ra chúng đã là một nửa bài tập.""")

# Chạy phần dưới SAU KHI đã điền xong, để kiểm cái bẫy additive.
print("\n--- kiểm tính additive (chạy sau khi đã dựng mart) ---")
try:
    show("""
        SELECT round(sum(returned_units) * 1.0 / sum(sold_units), 6) AS pooled_rate,
               round(avg(returned_units * 1.0 / sold_units), 6)      AS avg_of_cell_rates
        FROM marts.monthly_product_returns
        WHERE sold_units > 0""")
    print("""
ĐỌC CHO TRUNG THỰC: trên feed này hai số lệch nhau chưa tới một phần trăm, vì
bộ sinh dữ liệu hoàn tiền mọi sản phẩm với CÙNG một tỷ lệ nền và các ô đều xấp
xỉ bằng nhau. Chuyện đó là đặc tính của BỘ DỮ LIỆU NÀY, không phải của tỷ lệ —
và là kiểu may mắn NGUY HIỂM NHẤT, kiểu để một thói quen sai vượt qua bài kiểm
tra đầu tiên.

Danh mục hàng thật thì lồi lõm: một mã xả hàng bán 9 cái trả lại 1 là 11%, và
trong trung bình không trọng số nó ĐÈ BẸP một mã bán chạy ba chục nghìn cái.
Chỉ pooled_rate mới trả lời được "bao nhiêu phần trăm số lượng quay về".

Nên: LƯU TỬ SỐ VÀ MẪU SỐ, chia lúc đọc. Đừng bao giờ lưu sẵn tỷ lệ.""")
except Exception as e:
    print(f"(chưa có marts.monthly_product_returns — {type(e).__name__})")
    print("Mart này là TUỲ CHỌN. Dựng nó thì TOTAL chuyển 25 -> 26.")
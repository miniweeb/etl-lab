# %% work/a05_validation/a05_explore.py
"""A05 — Validation & quarantine: phát hiện chứ không sửa, cách ly chứ không xoá.

A03 sửa thứ rác CÓ THỂ sửa. Hôm nay xử lý thứ KHÔNG thể: đơn hàng không có tổng
tiền, customer_id không tồn tại trong bảng chiều nào, tổng tiền mâu thuẫn với
chính các dòng hàng của nó.

Điều tệ nhất pipeline có thể làm với những dòng đó là ÂM THẦM cho qua — số sai,
không ai biết suốt nhiều tháng. Tệ nhì là làm SẬP cả lần nạp vì 1% dòng hỏng.
Validation + quarantine là đường giữa: nạp 98% tốt, đỗ 2% xấu KÈM BẰNG CHỨNG, và
chỉ dừng dây chuyền khi MỨC RÁC tự nó bất thường.

Thứ tự BẤT DI BẤT DỊCH của một ngày:
    stage -> flag -> report -> quarantine -> gate -> load
Gate đặt sau INSERT là chuông báo cháy lắp sau khi cháy xong.

Task 1-10 và 12 ở SCALE small; Task 11 đổi "full" rồi chạy lại Task 8.
"""
import os
# Leo ngược lên tới gốc repo (nơi có thư mục lib/). Chạy cell theo kiểu notebook
# thì cwd là thư mục chứa file, nên lib.labpaths sẽ không import được.
while not os.path.exists("lib") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print(os.getcwd())

# %%
import time

import duckdb
import yaml

from lib.labpaths import (manifest_dir, quarantine_dir, raw_dims_dir,
                          raw_orders_dir, tmp_dir, warehouse_path)

SCALE = "small"                 # Task 11 đổi "full"
DAY = "2026-06-02"              # ngày v1 nào cũng được; số verify tính cho ngày này
CONTRACT = "contracts/shopcore_orders_daily.contract.yaml"
#   ^ nằm trong REPO, không phải dưới DATA_ROOT. Contract là mã nguồn: nó được
#     review, được đánh version, đi cùng branch. Dữ liệu thì không.

ORDERS_CSV = (raw_orders_dir(SCALE) / f"orders_{DAY}.csv").as_posix()
MANIFEST = (manifest_dir(SCALE) / f"orders_{DAY}.json").as_posix()
CUSTOMERS = (raw_dims_dir(SCALE) / "customers.csv").as_posix()
PRODUCTS = (raw_dims_dir(SCALE) / "products.csv").as_posix()
STORES = (raw_dims_dir(SCALE) / "stores.csv").as_posix()
QUARANTINE = quarantine_dir(SCALE) / "orders"
# as_posix(): DuckDB nhận dấu / trên mọi HĐH. Đường dẫn Windows dạng D:\... nhét
# vào chuỗi SQL sẽ bị hiểu \n, \t là ký tự thoát.


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
#   memory_limit    trần RAM của DuckDB. Máy 16GB, chừa nửa cho HĐH + trình duyệt.
#   temp_directory  vượt trần thì DuckDB TRÀN ra đĩa thay vì chết. Task 11 cần.
con.execute("CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS core; "
            "CREATE SCHEMA IF NOT EXISTS ops;")
#   staging  bảng nháp một ngày, ghi đè mỗi lần chạy
#   core     bảng đã kiểm, dùng được
#   ops      MỚI TỪ HÔM NAY: siêu dữ liệu vận hành (dq_report, alerts). Dùng tới A15.
print(f"scale={SCALE}  day={DAY}  quarantine={QUARANTINE}")

# %% ── DỌN SẠCH (chỉ chạy khi muốn làm lại từ đầu) ────────────────────
# Mọi thứ dưới raw/ đều là DẪN XUẤT và vứt được — dựng lại luôn được phép.
# DROP chứ không DELETE: bỏ luôn cấu trúc, tránh schema cũ sót cột không khớp.
# Hai bảng ops sẽ được CREATE TABLE IF NOT EXISTS dựng lại ở Task 6 và Task 7.
# PowerShell (con.close() trước): Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\small\quarantine\orders"
con.execute("""
DROP TABLE IF EXISTS core.orders;
DROP TABLE IF EXISTS ops.dq_report;
DROP TABLE IF EXISTS ops.alerts;
DROP TABLE IF EXISTS staging.shopcore_orders_day;
""")

# %% ── Task 1 — luật nằm trong contract, không nằm trong đầu bạn ──────
section("TASK 1 — nạp tolerance từ contract đã ký")

# RÁC CÓ NGÂN SÁCH — VÀ BẠN KHÔNG PHẢI NGƯỜI ĐẶT NÓ.
# Feed này sạch ~97%, và đó là BÌNH THƯỜNG: loại ~1,7% dòng mỗi ngày không phải
# sự cố, đó là thứ Ba. Câu hỏi thật không phải "có rác không?" mà "rác hôm nay có
# BẤT THƯỜNG không?" — và "bất thường" không phải ý kiến của bạn. shopcore và team
# warehouse đã KÝ một data contract; block quality.row_level_tolerances ghi rõ
# từng luật chịu được bao nhiêu rác. Việc của bạn là NẠP các con số đó.

contract = yaml.safe_load(open(CONTRACT, encoding="utf-8"))
#   safe_load: không cho YAML khởi tạo object Python tuỳ ý (chặn code execution).
#   encoding="utf-8" tường minh: Windows mặc định cp1252, gặp ký tự lạ sẽ vỡ.

TOL = {rule: float(str(tol).rstrip('%'))
       for rule, tol in
       contract["quality"]["row_level_tolerances"].items()}
#   YAML KHÔNG CÓ kiểu percent -> '0.5%' về Python là CHUỖI '0.5%'.
#   str(tol)     phòng khi ai đó viết 0.5 (không %) — lúc đó tol là float.
#   .rstrip('%') cắt ký tự % ở đuôi.
#   float(...)   ra số so sánh được.

CHECK_FOR = {                    # tên rule trong CONTRACT -> tên check trong CODE
    "null_or_invalid_status":  "bad_status",
    "unparseable_order_ts":    "bad_order_ts",
    "unparseable_order_total": "null_total",
    "orphan_customer_id":      "orphan_customer",
    "orphan_sku_lines":        "orphan_sku",
    "total_vs_items_mismatch": "total_mismatch",
}
#   Contract đặt tên theo ngôn ngữ NGHIỆP VỤ, code đặt tên theo ngôn ngữ KỸ THUẬT.
#   Bảng ánh xạ là chỗ DUY NHẤT hai thế giới gặp nhau — cùng ý tưởng với bảng
#   ánh xạ quốc gia của A03, thay cho một chuỗi CASE dài.

THRESHOLDS = {CHECK_FOR[rule]: tol for rule, tol in TOL.items()}
print(contract["version"], THRESHOLDS)
# ĐÍCH: in ra 1.3.0 và SÁU ngưỡng —
#   bad_order_ts = 0.5, orphan_customer = 0.5, bốn cái còn lại = 1.0
# và bạn phải CHỈ ĐƯỢC dòng YAML mà mỗi con số đến từ đó.
#
# Vì sao không viết thẳng sáu số? Vì tolerance là QUYẾT ĐỊNH CHUNG, không phải ý
# kiến của bạn — một con số, một chỗ, đã review, đã đánh version. Khi v1.4.0 siết
# orphan_customer_id, gate của MỌI consumer đổi theo ở lần chạy kế, zero code edit.
#
# Hai dòng fine print đáng ghi lại:
#   1. Contract cấp ngân sách theo DÒNG orphan sku ("orphan_sku_LINES"), còn check
#      của bạn gắn cờ cả ĐƠN HÀNG. Ở mức này thì đủ gần; nhận ra những lệch pha
#      kiểu đó là kỹ năng của A06.
#   2. Luật ghim mà KHÔNG có tolerance (order_id là nullable: false, chấm hết)
#      hôm nay chỉ mang tính THÔNG TIN; A06 mới cưỡng chế bằng pre-flight gate.

# %% ── Task 2 — bên gửi khai bao nhiêu dòng? ──────────────────────────
section("TASK 2 — manifest: lời khai của producer")

con.sql(f"SELECT rows, late_rows, dup_rows FROM read_json_auto('{MANIFEST}')").show()
#   read_json_auto: DuckDB đọc file JSON NHƯ MỘT BẢNG, tự suy ra schema.
#
# Mỗi file orders đi kèm một manifest — producer TỰ KHAI đã ghi những gì
# (đúng điều khoản delivery.completeness_signal của contract).
#
# ĐÍCH: rows = 59024 cho small 2026-06-02.
#   late_rows  dòng mang order_ts thuộc ngày TRƯỚC — bản sửa đến trễ.
#              Chính là bất ngờ phân vùng ở A02: một file KHÔNG phải một ngày.
#   dup_rows   order_id xuất hiện HAI LẦN trong CÙNG file — bản sửa trong ngày.
# Cả hai đều KHÔNG PHẢI RÁC. Đừng loại chúng. A08 mới là lúc gấp chúng lại.
#
# Đối chiếu rows với số dòng ta nạp được chính là RECONCILIATION — check rẻ nhất
# và có giá trị cao nhất trong cả bộ suite. Task 7 chỉ formalise nó.

# %% ── Task 3 — dựng staging đã làm sạch ──────────────────────────────
section("TASK 3 — staging.shopcore_orders_day (bản đầy đủ + lineage)")

# PHÁT HIỆN, ĐỪNG SỬA.
# A03 chuẩn hoá CÁCH BIỂU DIỄN (hoa thường, khoảng trắng, định dạng số và ngày);
# hôm nay phán xét GIÁ TRỊ sau khi đã làm sạch. '  CARD  ' KHÔNG phải rác — nó là
# 'card' mặc đồ hoá trang. 'unknown' MỚI là rác: không định dạng nào cứu nó thành
# một status hợp lệ.
# Validation LUÔN chạy trên bảng staging ĐÃ SẠCH, không bao giờ trên text thô —
# nếu không, bạn loại chính những dòng mà pipeline của mình cứu được.
#
# CREATE OR REPLACE đè lên bảng nháp cùng tên của A04 — CÓ CHỦ Ý: hôm nay cần đủ
# cột + 2 cột lineage §7.2, vì dòng bị loại sẽ trở thành BẰNG CHỨNG quarantine.
con.execute(f"""
CREATE OR REPLACE TABLE staging.shopcore_orders_day AS
SELECT
    order_id,
    -- Excel-float: '123456.0'. CAST thẳng sang BIGINT sẽ lỗi vì có dấu chấm,
    -- nên đi vòng qua DOUBLE rồi mới cắt phần thập phân. Đây chính là lý do cột
    -- này được đọc dưới dạng VARCHAR ở khối columns={{...}} bên dưới: đọc thẳng
    -- BIGINT thì DuckDB đã chết ngay lúc parse CSV, trước khi ta kịp cứu.
    CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT)            AS customer_id,
    store_id,
    -- try_strptime nhận một DANH SÁCH định dạng, thử lần lượt, trả NULL nếu
    -- không cái nào khớp. Ngày bất khả thi (2026-06-31) cũng ra NULL.
    -- Có try_ nghĩa là thất bại trả NULL thay vì ném lỗi làm chết cả query.
    try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',
                            '%d/%m/%Y %H:%M:%S',
                            '%Y-%m-%dT%H:%M:%S'])              AS order_ts,
    updated_at,
    -- TRIM cắt khoảng trắng, LOWER hạ hoa, NULLIF biến chuỗi RỖNG thành NULL.
    -- Thứ tự quan trọng: TRIM trước, vì '   ' chỉ thành '' sau khi cắt.
    NULLIF(LOWER(TRIM(status)), '')                            AS status,
    NULLIF(LOWER(TRIM(payment_method)), '')                    AS payment_method,
    -- Bốn dạng tiền bẩn: '$79.98' · 'N/A' · '93,32' · '1.234,56'
    --   LIKE '%,%'  -> kiểu châu Âu: BỎ dấu chấm (phân cách nghìn) rồi ĐỔI
    --                  dấu phẩy thành dấu chấm thập phân. Thứ tự bắt buộc:
    --                  đổi phẩy trước thì '1.234,56' thành '1.234.56', vô nghĩa.
    --   ngược lại   -> chỉ cắt ký hiệu tiền tệ
    -- NULLIF(..., 'N/A') biến sentinel thành NULL TRƯỚC khi cast.
    -- DECIMAL(14,2) chứ không phải DOUBLE: tiền không bao giờ là số thực nhị phân.
    TRY_CAST(NULLIF(
      CASE WHEN order_total LIKE '%,%'
           THEN replace(replace(order_total, '.', ''), ',', '.')
           ELSE replace(TRIM(order_total), '$', '') END,
      'N/A') AS DECIMAL(14,2))                                 AS order_total,
    items,
    meta,
    -- §7.2 LINEAGE — đúng HAI cột, trên mọi dòng warehouse ghi ra từ đây về sau.
    -- KHÔNG có cột tên file: một file một business date, nên _data_date ĐÃ gọi
    -- tên lần giao hàng. Cũng không có cột thời điểm nạp — "nạp lúc nào" là sự
    -- thật về RUN chứ không về DÒNG, và sổ ops.etl_runs của A07 giữ nó, khoá
    -- bằng _run_id (còn NULL cho tới khi A07 phát minh ra danh tính run).
    DATE '{DAY}'                                               AS _data_date,
    CAST(NULL AS BIGINT)                                       AS _run_id
FROM read_csv('{ORDERS_CSV}',
    header=true,
    -- columns={{...}} là SCHEMA TƯỜNG MINH: đọc kiểu này thì file sai hình dạng
    -- sẽ hỏng ỒN ÀO ngay tại đây. Đó chính là "schema check" — loại check thứ
    -- nhất trong năm loại; A06 nâng nó thành pre-flight gate đàng hoàng.
    columns={{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'}});
""")
con.sql("SELECT count(*) AS staged FROM staging.shopcore_orders_day").show()
# ĐÍCH: BẰNG ĐÚNG manifest rows (59024). Lệch một dòng cũng là bug, không phải
# sai số. Đẳng thức này CHÍNH LÀ reconciliation.
#
# §7.4: ba cleaner trên (order_ts / money / id) KHÔNG phải code mới — chúng là
# clean_order_ts, clean_money, clean_id đã tốt nghiệp vào a03_cleaners.py cuối
# A03, được ghim bởi tests/test_cleaners.py. Viết inline ở ĐÂY chỉ để đọc lại
# từng luật cạnh thứ rác nó xử lý; trong a05_validate_day.py phải IMPORT chúng.
# Module là NHÀ DUY NHẤT của chúng.
#
# CHÚ Ý staging KHÔNG LÀM: không map 'unknown' -> NULL, không drop dòng nào.
# Hai cột enum CỐ Ý bỏ qua clean_enum — whitelist của clean_enum đã PHÁN XÉT
# (rác -> NULL) và phán xét là việc của bộ check, không phải của staging.
# Một status rác PHẢI tới quarantine với GIÁ TRỊ CÒN NGUYÊN để làm bằng chứng.

# %% ── Task 4 — items nói đơn hàng này đáng bao nhiêu? ────────────────
section("TASK 4 — items_agg: tổng tiền theo line item + đếm orphan sku")

# Check cross-field cần, với mỗi đơn, sum(qty * unit_price) trên các dòng hàng.
# Tiện thể đang bung mảng, đếm luôn SKU vắng mặt trong bảng chiều (gặp ở A04).
con.execute(f"""
CREATE OR REPLACE TEMP TABLE items_agg AS
SELECT order_id, updated_at,
       round(sum(it.qty * it.unit_price), 2) AS items_total,
       -- FILTER: chỉ đếm dòng thoả điều kiện. Sạch hơn sum(CASE WHEN ... THEN 1)
       -- p.sku IS NULL sau LEFT JOIN = "không tìm thấy trong bảng chiều"
       count(*) FILTER (p.sku IS NULL)       AS n_orphan_sku
FROM (
    -- LUẬT A04: unnest KHÔNG ngồi chung SELECT với GROUP BY (binder error, luôn
    -- luôn). Bung ở truy vấn CON, gộp ở NGOÀI.
    -- ::STRUCT(...)[]  ép JSON array thành mảng struct CÓ KIỂU -> qty ghi dạng
    -- chuỗi "2" được cast ngầm thành 2, và ta có cột thật để nhân.
    SELECT order_id, updated_at,
           unnest(CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER,
                                              unit_price DOUBLE)[]) AS it
    FROM staging.shopcore_orders_day
) LEFT JOIN read_csv('{PRODUCTS}', header=true) p ON it.sku = p.sku
-- LEFT chứ không INNER: INNER sẽ ÂM THẦM loại dòng orphan, tổng tiền co lại,
-- không ai hay. Cùng bài học với A04.
GROUP BY 1, 2;
""")
# VÌ SAO GROUP BY (order_id, updated_at) chứ không riêng order_id?
# Vì ~0,05% order_id xuất hiện HAI LẦN trong một file (bản sửa trong ngày —
# chuyện của A08). CẶP thì duy nhất; id đơn lẻ thì không. Gộp riêng theo id sẽ
# TRỘN dòng hàng của hai phiên bản khác nhau vào một items_total — và check
# total_mismatch sẽ báo sai hàng loạt. Task 7 CHỨNG MINH cặp này duy nhất.

con.sql("""
    SELECT (SELECT count(*) FROM items_agg)                          AS agg_rows,
           (SELECT count(*) FROM staging.shopcore_orders_day)        AS staged
""").show()
# PHÉP KIỂM TRA BẢO TOÀN (kỷ luật của A04): phải BẰNG NHAU. Lệch nghĩa là khoá
# gộp sai — hoặc có đơn không có dòng hàng nào.

con.sql("SELECT count(*) FROM items_agg WHERE n_orphan_sku > 0").show()
# ĐÍCH: ~310 (≈0,5% số đơn)

# %% ── Task 5 — bộ check -> MỘT reject_reason mỗi dòng ────────────────
section("TASK 5 — flagged: gom MỌI lý do một dòng bị loại")

# NĂM LOẠI CHECK, bộ suite nào cũng là một hỗn hợp của chúng:
#   1. SCHEMA        đúng cột, đúng kiểu chưa?      -> columns={{...}} ở Task 3
#   2. ROW           DÒNG NÀY chấp nhận được chưa?  -> chính là cell này
#   3. AGGREGATE     TẬP dòng có hợp lý không?      -> Task 7
#   4. RECONCILIATION khớp lời khai producer chưa?  -> Task 7
#   5. FRESHNESS     bảng có ĐANG cập nhật không?   -> Task 9
#
# Một dòng có thể hỏng NHIỀU luật cùng lúc -> reject_reason là danh sách ngăn
# bằng '; '. Kỹ thuật: mỗi CASE trả TÊN CHECK hoặc NULL; concat_ws BỎ QUA NULL;
# NULLIF(..., '') để dòng sạch hoàn toàn có reject_reason = NULL chứ không phải ''.
con.execute(f"""
CREATE OR REPLACE TEMP TABLE flagged AS
SELECT s.*, i.items_total,
       NULLIF(concat_ws('; ',
         -- ── NOT NULL trên khoá ────────────────────────────────────────────
         CASE WHEN s.order_id IS NULL THEN 'null_order_id' END,
         CASE WHEN s.order_ts IS NULL THEN 'bad_order_ts' END,

         -- ── RANGE: điều khoản của contract trên order_ts ──────────────────
         -- cửa sổ [business_date - 7 ngày, business_date + 1 ngày)
         -- GREATEST(DATE '2026-06-01', ...) tôn trọng mốc bắt đầu kỷ nguyên dữ
         --   liệu: trước 06-01 không tồn tại ngày nào để dòng trễ rơi vào.
         -- s.order_ts IS NOT NULL ở đầu: NULL đã bị bad_order_ts bắt rồi, không
         --   được bắt lần hai (và NULL trong phép so sánh cho ra NULL, không
         --   phải false — CASE sẽ bỏ qua, nhưng viết tường minh cho người đọc).
         CASE WHEN s.order_ts IS NOT NULL AND NOT (
                s.order_ts >= GREATEST(DATE '2026-06-01',
                                       DATE '{DAY}' - INTERVAL 7 DAY)
            AND s.order_ts <  DATE '{DAY}' + INTERVAL 1 DAY)
              THEN 'ts_out_of_range' END,

         -- ── ENUM WHITELIST: allowed_values của contract, NGUYÊN VĂN ────────
         -- Danh sách trắng chứ không phải danh sách đen: giá trị lạ mai kia
         -- xuất hiện sẽ bị bắt, thay vì lọt qua vì chưa ai nghĩ tới nó.
         CASE WHEN s.status IS NULL OR s.status NOT IN
              ('created','paid','shipped','delivered','cancelled','refunded')
              THEN 'bad_status' END,
         -- Cùng khuôn, khác cột và khác danh sách. ĐÍCH: 0 dòng — con số 0 đó
         -- CHỨNG MINH chuẩn hoá LOWER/TRIM của A03 đang chạy đúng.
         CASE WHEN s.payment_method IS NULL OR s.payment_method NOT IN
              ('card','wallet','bank_transfer','cod','paypal')
              THEN 'bad_payment' END,

         -- ── SỐ ────────────────────────────────────────────────────────────
         CASE WHEN s.order_total IS NULL THEN 'null_total' END,
         CASE WHEN s.order_total < 0 THEN 'negative_total' END,

         -- ── CROSS-FIELD: tổng tiền phải khớp tổng dòng hàng ───────────────
         -- Dung sai một xu (0.01) cho sai số làm tròn.
         -- s.order_total >= 0 chặn ở đầu: tổng âm đã bị negative_total bắt, đừng
         --   tính nó hỏng hai luật (nếu không, 52 dòng âm sẽ đội cả hai con số).
         -- i.items_total IS NOT NULL: đơn không có dòng hàng thì KHÔNG PHÁN XÉT,
         --   không mặc định là hỏng.
         CASE WHEN s.order_total >= 0 AND i.items_total IS NOT NULL
               AND abs(s.order_total - i.items_total) > 0.01
              THEN 'total_mismatch' END,

         -- ── REFERENTIAL INTEGRITY (ba join đã có sẵn trong FROM) ──────────
         -- Khuôn chung: sau LEFT JOIN, cột bên phải IS NULL = "không khớp".
         -- null_customer là KHOẢNG TRỐNG chứ không phải VI PHẠM: contract ghim
         --   customer_id nullable: true ("NULL = khách vãng lai"), nên không
         --   điều khoản nào bị phá. Vẫn ghi nhận để đếm. A06 phân loại cả đống.
         CASE WHEN s.customer_id IS NULL THEN 'null_customer' END,
         -- IS NOT NULL ở đầu: tách "khách vãng lai" khỏi "id trỏ vào hư không".
         -- Thiếu vế này thì mọi khách vãng lai đều bị tính là orphan.
         CASE WHEN s.customer_id IS NOT NULL AND c.customer_id IS NULL
              THEN 'orphan_customer' END,
         CASE WHEN st.store_id IS NULL THEN 'orphan_store' END,
         -- n_orphan_sku đếm DÒNG HÀNG; ở đây ta gắn cờ cả ĐƠN. Đúng chỗ lệch
         -- pha so với contract mà Task 1 đã nhắc.
         CASE WHEN i.n_orphan_sku > 0 THEN 'orphan_sku' END
       ), '') AS reject_reason
FROM staging.shopcore_orders_day s
LEFT JOIN items_agg i USING (order_id, updated_at)
-- Ba dim join dưới đây đọc thẳng CSV thô để tài liệu tự đứng được. Kiến trúc
-- ĐÚNG là join vào core.customers / core.stores của A03 — cùng truy vấn, khác
-- mệnh đề FROM.
LEFT JOIN read_csv('{CUSTOMERS}', header=true) c ON s.customer_id = c.customer_id
LEFT JOIN read_csv('{STORES}',    header=true) st ON s.store_id   = st.store_id;
""")
print(con.execute("SELECT count(*), count(reject_reason) FROM flagged").fetchone())
# count(cột) đếm NON-NULL -> count(reject_reason) chính là số dòng vào quarantine.
#
# ĐÍCH small 2026-06-02: 992 dòng (1,68%). DỪNG LẠI ĐỐI CHIẾU TRƯỚC KHI ĐI TIẾP —
# Task 6, 7, 8 đều xây trên con số này.
#   lệch vài chục     -> luật của bạn khác chút, TÌM và GIẢI THÍCH từng khác biệt
#   0 hoặc hàng nghìn -> bug: sai khoá join, hoặc validate raw thay vì staging
#   cao hơn ~30       -> nhiều khả năng bạn đang loại order_id trùng. ĐỪNG. Đó là
#                        bản sửa hợp lệ, không phải rác.

con.sql("""
    SELECT order_id, status, payment_method, order_total, items_total, reject_reason
    FROM flagged WHERE reject_reason IS NOT NULL LIMIT 10
""").show()
# Đọc mười nạn nhân. MỖI lý do phải khiến bạn gật đầu. Không gật được thì luật sai.

# %% ── Task 6 — thứ tự: cách ly -> gate -> nạp ────────────────────────
section("TASK 6 — split load")

# NÓI TO LÊN TRƯỚC KHI CHẠY: đỗ bằng chứng -> kiểm ngưỡng -> nạp.
bad, staged = con.execute(
    "SELECT count(reject_reason), count(*) FROM flagged").fetchone()
pct = 100.0 * bad / staged
print(f"staged={staged} bad={bad} ({pct:.2f}%)")

# %%
# ── 6.1 — BẰNG CHỨNG luôn được ghi, kể cả (NHẤT LÀ) vào ngày tồi tệ ───
# CÁCH LY, ĐỪNG XOÁ. Dòng bị loại là bằng chứng: cho câu hỏi "sao hôm nay nhiều
# hơn thường lệ 300 dòng?", cho việc chỉ tận tay producer dòng nào hỏng, và cho
# việc PHÁT LẠI dòng nếu một luật hoá ra quá chặt.
QUARANTINE.mkdir(parents=True, exist_ok=True)
#   COPY có PARTITION_BY tự tạo các thư mục order_date=..., nhưng KHÔNG tạo thư
#   mục CHA. Thiếu dòng này -> "Failed to create directory", lỗi kinh điển của bài.
con.execute(f"""
COPY (
  SELECT *,
         -- _rejected_at là PAYLOAD (sự thật về DÒNG NÀY), không phải lineage §7.2
         now() AT TIME ZONE 'UTC' AS _rejected_at,
         -- Dòng có timestamp không parse được thì KHÔNG có order_date; nó MƯỢN
         -- ngày của file thay vì trở thành khoá phân vùng NULL.
         COALESCE(CAST(order_ts AS DATE), DATE '{DAY}') AS order_date
  FROM flagged WHERE reject_reason IS NOT NULL
) TO '{QUARANTINE.as_posix()}'
  (FORMAT parquet, PARTITION_BY (order_date), OVERWRITE_OR_IGNORE,
   FILENAME_PATTERN 'from_{DAY}_{{i}}');
""")
#   FILENAME_PATTERN vì sao cần: dòng xấu đến TRỄ rơi vào phân vùng order_date
#   CŨ HƠN, nên các ngày liên tiếp cùng ghi vào MỘT thư mục. Tên mặc định
#   data_0.parquet sẽ để ngày D+1 GHI ĐÈ ngày D — im lặng, không báo lỗi, mất
#   sạch bằng chứng. Tên gắn ngày nguồn thì cùng tồn tại, và chạy lại cùng một
#   ngày chỉ đè CHÍNH NÓ.
#   {{i}} trong f-string là {i} thật — chỗ DuckDB điền số thứ tự file.
#
# DÒNG QUARANTINE MANG GÌ: toàn bộ payload + reject_reason (VÌ SAO) + _rejected_at
# (KHI NÀO) + hai cột lineage §7.2, và KHÔNG GÌ KHÁC. Đủ để điều tra: _data_date
# gọi tên lần giao hàng (một file một business date, nên 2026-06-02 CHÍNH LÀ
# orders_2026-06-02.csv), và từ A07 trở đi _run_id join vào ops.etl_runs để lấy
# tên file, thời điểm nạp và trạng thái run. Dập tên file lên cả 992 dòng là lưu
# MỘT sự thật 992 lần.

# %%
# ── 6.2 — GATE: tolerance của contract, kiểm TRƯỚC KHI core thấy dòng nào ──
con.execute("""
CREATE TABLE IF NOT EXISTS ops.alerts (
  alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
  source VARCHAR, message VARCHAR);
""")
# ops.alerts là PAGER tiêu chuẩn của lab, dùng tiếp tới A15: A07 ghi alert khi một
# run chết hẳn, A11 khi file trễ SLA. run_id còn NULL vì lý do y hệt _run_id —
# và được khai kiểu BIGINT để sẵn sàng nhận id từ sổ run của A07.

rate = dict(con.execute(f"""
    SELECT reason, round(100.0 * count(*) / {staged}, 3) FROM (
      SELECT unnest(string_split(reject_reason, '; ')) AS reason
      FROM flagged WHERE reject_reason IS NOT NULL) GROUP BY 1""").fetchall())
#   string_split('; ') tách chuỗi lý do thành MẢNG; unnest bung mảng thành DÒNG.
#   Một dòng hỏng 2 luật -> đóng góp vào cả hai tỉ lệ. Đó là lý do tổng theo check
#   LỚN HƠN số dòng quarantine.
#   dict(...) biến list tuple thành dict để tra bằng tên check.

breaches = [(c, rate.get(c, 0.0), t)
            for c, t in THRESHOLDS.items() if rate.get(c, 0.0) > t]
#   rate.get(c, 0.0): check không xuất hiện lần nào -> tỉ lệ 0, không phải KeyError.
#   Duyệt theo THRESHOLDS (6 luật có ngân sách), không duyệt theo rate: check
#   không được contract cấp ngân sách thì KHÔNG BAO GIỜ chặn được lần nạp.

if breaches:
    for check, got, limit in breaches:
        msg = (f"DQ HARD FAIL {DAY}: {check} at {got}% "
               f"exceeds contract tolerance {limit}%")
        con.execute("INSERT INTO ops.alerts VALUES "
                    "(current_timestamp, NULL, 'error', 'dq_gate', ?)", [msg])
        #   ? là tham số bind — không nối chuỗi vào SQL. Thói quen chống injection,
        #   và ở đây còn tránh vỡ cú pháp nếu message có dấu nháy.
        print("ALERT:", msg)
    raise RuntimeError(f"DQ HARD FAIL {DAY}: {len(breaches)} contract "
                       f"tolerance(s) breached. Nothing loaded to core.")
    #   RuntimeError kiêm luôn EXIT CODE khác 0 — trong production đó là chỗ pager
    #   réo. Ba tầng cùng lúc: một dòng bảng, một dòng console, một mã thoát.
print("gate: PASS")

# %%
# ── 6.3 — dòng TỐT ───────────────────────────────────────────────────
con.execute("""
-- GRAIN: một dòng = MỘT PHIÊN BẢN ĐƯỢC GIAO của một đơn hàng (duy nhất trên
-- order_id, _data_date). KHÔNG phải một dòng một đơn: ~1,5% mỗi file giao lại
-- đơn của ngày trước như bản sửa; gấp chúng lại là việc của A08.
CREATE TABLE IF NOT EXISTS core.orders AS
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE 1 = 0;
--   WHERE 1=0: sao chép HÌNH DẠNG, không sao chép dòng nào. Mẹo tạo bảng rỗng
--   mà không phải gõ tay cả danh sách cột.
--   EXCLUDE (reject_reason): core không cần cột đó — nó NULL trên mọi dòng ở đây.
""")
con.execute("""
INSERT INTO core.orders
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE reject_reason IS NULL;
""")
con.sql("SELECT count(*) FROM core.orders").show()
# ĐÍCH: 58032 = 59024 - 992
#
# INSERT trần KHÔNG rerun-safe: chạy hai lần thì ngày đó nằm trong core HAI LẦN.
# Sống chung hôm nay (chạy lại = DROP TABLE core.orders rồi nạp lại); A07 sửa tử tế.
# Số đúng bằng 2x manifest thì chắc chắn là lỗi này, không phải lỗi nào khác.
#
# Ngó thư mục: Get-ChildItem "$env:ETL_LAB_DATA\small\quarantine\orders"
# TỰ TRẢ LỜI: vì sao có folder order_date=2026-06-01 dù bạn chỉ nạp 06-02?

# %% ── Task 7 — ops.dq_report: dấu vết giấy tờ ─────────────────────────
section("TASK 7 — ops.dq_report, một dòng mỗi (ngày, check)")

# Check mà không ai xem lại được thì coi như chưa từng chạy. Mỗi lần chạy ghi
# thêm MỘT DÒNG cho MỖI (ngày, check) — KỂ CẢ check 0 lỗi.
# Số 0 CÓ GIÁ TRỊ: bad_payment = 0 CHỨNG MINH chuẩn hoá của A03 đang chạy đúng.
# Không có dòng đó, bạn không phân biệt được "check đã chạy và sạch" với "check
# quên chưa chạy".

# CREATE trước, DELETE sau — thứ tự này bắt buộc, vì lần chạy đầu (hoặc sau khi
# DROP) thì bảng chưa tồn tại để mà xoá.
con.execute("""
CREATE TABLE IF NOT EXISTS ops.dq_report (
    run_at TIMESTAMP, order_date DATE, check_type VARCHAR, check_name VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, pct_failed DOUBLE,
    threshold_pct DOUBLE, passed BOOLEAN);
""")
con.execute(f"DELETE FROM ops.dq_report WHERE order_date = DATE '{DAY}'")
#   Chạy lại cell này nhiều lần mà không nhân đôi báo cáo. (validate_day của
#   Task 8 không có dòng này — ở đó ta dọn cả bảng trước khi chạy ba ngày.)

# ── 7.1 — 12 ROW CHECK ────────────────────────────────────────────────
# Danh sách ĐẦY ĐỦ dựng từ Python rồi LEFT JOIN lên số quan sát được. Chiều join
# này mới là mấu chốt: đi từ DANH SÁCH CHECK ra ngoài, nên check không hỏng lần
# nào vẫn có dòng (rows_failed = 0). Join ngược lại thì chúng biến mất.
check_rows = ", ".join(
    f"('{c}', {THRESHOLDS.get(c, 'NULL')})"
    for c in ['null_order_id', 'bad_order_ts', 'ts_out_of_range', 'bad_status',
              'bad_payment', 'null_total', 'negative_total', 'total_mismatch',
              'null_customer', 'orphan_customer', 'orphan_store', 'orphan_sku'])
#   THRESHOLDS.get(c, 'NULL') -> ngưỡng chảy YAML -> THRESHOLDS -> SQL.
#   Check contract KHÔNG cấp ngân sách thì threshold NULL: chỉ mang tính thông
#   tin, không bao giờ chặn được lần nạp. KHÔNG hard-code số nào ở đây.

con.execute(f"""
CREATE OR REPLACE TEMP TABLE all_row_checks AS
SELECT * FROM (VALUES {check_rows}) AS t(check_name, threshold_pct);
""")

con.execute(f"""
INSERT INTO ops.dq_report
WITH observed AS (
  SELECT reason, count(*) AS n FROM (
    SELECT unnest(string_split(reject_reason, '; ')) AS reason
    FROM flagged WHERE reject_reason IS NOT NULL)
  GROUP BY 1
)
SELECT current_timestamp,
       DATE '{DAY}',
       'row',
       c.check_name,
       {staged},
       coalesce(o.n, 0) AS rows_failed,
       round(100.0 * coalesce(o.n, 0) / {staged}, 3) AS pct_failed,
       c.threshold_pct,
       -- coalesce(..., true): threshold NULL -> phép so sánh ra NULL -> ép thành
       -- true. Check thông tin thì LUÔN pass, theo định nghĩa.
       coalesce(round(100.0 * coalesce(o.n, 0) / {staged}, 3) <= c.threshold_pct,
                true) AS passed
FROM all_row_checks c
LEFT JOIN observed o ON o.reason = c.check_name;
""")

# ── 7.2 — AGGREGATE CHECK: không dòng nào có tội, cả NHÓM mới có ──────
con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'aggregate', 'unique_order_id_updated_at',
       {staged}, count(*), round(100.0 * count(*) / {staged}, 3), 0.0, count(*) = 0
FROM (SELECT order_id, updated_at FROM staging.shopcore_orders_day
      GROUP BY 1, 2 HAVING count(*) > 1);
""")
#   Truy vấn con trả về các CẶP xuất hiện > 1 lần; count(*) bên ngoài đếm số cặp
#   đó. Đây là LỜI KHẲNG ĐỊNH của Task 4, nay được chứng minh: ngưỡng 0.0 nghĩa
#   là một vi phạm duy nhất cũng không chấp nhận được.

con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'aggregate', 'dup_order_id',
       {staged}, count(*), round(100.0 * count(*) / {staged}, 3), NULL, true
FROM (SELECT order_id FROM staging.shopcore_orders_day
      GROUP BY order_id HAVING count(*) > 1);
""")
#   CÙNG khuôn, chỉ gộp theo order_id. Nhưng threshold NULL / passed true:
#   order_id lặp là BẢN SỬA TRONG NGÀY, hoàn toàn bình thường. BÁO CÁO tỉ lệ,
#   KHÔNG loại dòng nào. A08 mới gấp chúng lại.
#   Cặp đôi hai check này chính là bằng chứng cho lựa chọn khoá join ở Task 4:
#   một cái phải bằng 0, cái kia thì không.

# ── 7.3 — RECONCILIATION: số của ta vs lời khai producer ─────────────
con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'reconciliation', 'rowcount_vs_manifest',
       {staged}, abs({staged} - rows), NULL, 0.0, {staged} = rows
FROM read_json_auto('{MANIFEST}');
""")
#   Check RẺ NHẤT và GIÁ TRỊ CAO NHẤT trong cả bộ suite: nạp được phải BẰNG khai
#   báo, mỗi ngày, không lý do. pct_failed NULL vì "lệch bao nhiêu phần trăm" ở
#   đây vô nghĩa — chỉ có khớp hoặc không khớp.

# ── 7.4 — quarantine_rate: tỉ lệ chung cả NGÀY ───────────────────────
con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'aggregate', 'quarantine_rate',
       {staged}, {bad}, round({pct}, 3), NULL, true;
""")
#   threshold NULL vì contract cấp ngân sách theo LUẬT, không theo NGÀY.
#   Informational — và đây là dòng mà biểu đồ xu hướng (stretch 1) dựng lên.
#   SELECT không có FROM: DuckDB cho phép, trả đúng một dòng hằng số.

con.sql(f"""
    SELECT check_type, check_name, rows_failed, pct_failed, threshold_pct, passed
    FROM ops.dq_report WHERE order_date = DATE '{DAY}'
    ORDER BY check_type, rows_failed DESC
""").show(max_rows=20)
# ĐÍCH: 16 dòng = 12 row + 3 aggregate + 1 reconciliation.
#   Mọi passed = true. threshold_pct chỉ có số trên đúng SÁU row check được map.
#
# Số tham chiếu cho 2026-06-02:
#   orphan_sku 310 · total_mismatch 308 · null_total 117 · null_customer 94
#   bad_status 71 · negative_total 52 · orphan_customer 27 · bad_order_ts 18
#   ts_out_of_range, bad_payment, orphan_store, null_order_id: đều 0
# Tổng theo check (997) LỚN HƠN số dòng quarantine (992): năm dòng hỏng hai luật
# cùng lúc. Chứng minh bằng truy vấn cuối file .sql.
#
# VI PHẠM hay KHOẢNG TRỐNG? Gần như mọi lý do hôm nay đều phá một luật CÓ GHI
# trong contract -> VI PHẠM, phải đếm và báo cho shopcore. Trừ null_customer:
# contract ghim customer_id nullable: true ("NULL = khách vãng lai"), nên không
# điều khoản nào bị phá — đó là KHOẢNG TRỐNG. Phân loại cả đống (vi phạm / khoảng
# trống / được dung thứ) là bài mở màn của A06.

# %% ── Task 8 — chạy ba ngày bằng module ───────────────────────────────
section("TASK 8 — work/a05_validation/a05_validate_day.py, ba ngày")

# Task 3-7 giờ đã thành hàm trong module, tham số hoá theo `day`, xếp ĐÚNG thứ tự
# gate. Module này là nấc thang tiếp theo trên bậc thang code §7.4, nên nó chơi
# theo luật của bậc thang: docstring nêu nhiệm vụ và giả định, comment nêu RÀNG
# BUỘC (không phải thuật lại code), cleaner IMPORT từ a03_cleaners.py — và A07
# (pipeline) cùng A11 (runner) sẽ import CHÍNH module này, không bao giờ paste lại.

con.execute("""
DROP TABLE IF EXISTS core.orders;
DROP TABLE IF EXISTS ops.dq_report;
DROP TABLE IF EXISTS ops.alerts;
""")
# PowerShell: Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\small\quarantine\orders"
# Task 6-7 dạy bạn CÁI GÌ đang xảy ra; Task 8 mới là bản chạy thật. Xoá và làm lại.

from work.a05_validation.a05_validate_day import validate_day   # noqa: E402

for day in ["2026-06-02", "2026-06-19", "2026-07-11"]:
    t0 = time.perf_counter()
    validate_day(con, day, SCALE)
    print(f"  {day}: {time.perf_counter() - t0:.1f}s")
#   06-02 ngày thường; 06-19 và 07-11 là HAI ngày flash-sale (~3x sản lượng).

con.sql("SELECT count(*) FROM core.orders").show()      # 414095
con.sql("SELECT count(*) FROM ops.dq_report").show()    # 48 = 16 x 3
# QUAN SÁT CHÍNH — ghi lại: hai ngày cao điểm có ~3x số DÒNG nhưng CÙNG ~1,7% tỉ
# lệ quarantine. Sản lượng đổi, chất lượng không. Đó chính xác là lý do ngưỡng
# phải là PHẦN TRĂM chứ không phải số dòng: ngưỡng tính bằng dòng sẽ báo động giả
# mỗi lần có flash sale.

# %% ── Task 9 — freshness ─────────────────────────────────────────────
section("TASK 9 — core có ĐANG cập nhật không?")
# Mọi check tới giờ đều phán xét NỘI DUNG một ngày; không cái nào nhận ra pipeline
# ĐƠN GIẢN LÀ ĐÃ NGỪNG CHẠY. Core có thể qua hết mọi check trên mà vẫn trễ ba
# ngày — sạch hoàn hảo, âm thầm nói dối mọi dashboard đọc nó.
# Cột lineage _data_date của A03 khiến check này chỉ còn MỘT DÒNG SQL.
#
# DuckDB là embedded db: file .duckdb chỉ cho MỘT process ghi. Chạy con.close()
# trước, nếu không terminal sẽ báo "file is being used by another process".
#
# Chạy từ GỐC REPO (thói quen -m của A03; gọi theo đường dẫn thì lib không import được):
#   python -m work.a05_validation.a05_freshness_check
#   -> fresh: core.orders is at 2026-07-11        (exit 0)
#
# Demo vi phạm: đổi FRESH_EXPECT = "2026-07-12" (ngày chưa từng nạp), chạy lại:
#   python -m work.a05_validation.a05_freshness_check; $LASTEXITCODE
#   -> dòng ALERT, rồi 1
# sys.exit(1) mới là thứ scheduler đọc; nó không đọc stdout. Không có dòng đó thì
# job "thành công" dù dữ liệu đã ôi.
# Rồi trả FRESH_EXPECT về "2026-07-11".

con.sql("SELECT severity, source, message FROM ops.alerts ORDER BY alert_ts").show()
# Phải có ĐÚNG MỘT dòng mới: severity 'error', source 'freshness'.

# %% ── Task 10 — bắt gate nổ ──────────────────────────────────────────
section("TASK 10 — gate chưa từng thấy nổ là tin đồn")

# ĐỪNG sửa contract YAML để dựng cảnh hỏng — đó là thoả thuận ĐÃ KÝ, không phải
# file config của bạn. Ghi đè GIÁ TRỊ ĐÃ NẠP trong code:
THRESHOLDS["total_mismatch"] = 0.1        # giả vờ có bản sửa đổi siết mười lần
#   Tỉ lệ thật của 06-19 là ~0,522% — dưới ngưỡng contract 1.0 nên ngày này vốn
#   xanh. Hạ xuống 0.1 làm nó vỡ.

before = con.execute("SELECT count(*) FROM core.orders").fetchone()[0]
try:
    validate_day(con, "2026-06-19", SCALE, thresholds=THRESHOLDS)
    #   PHẢI truyền thresholds=; thiếu nó thì hàm tự load_thresholds() từ contract
    #   và override bị bỏ qua lặng lẽ.
except RuntimeError as e:
    print("ĐÚNG NHƯ MONG ĐỢI:", e)
    #   try/except để notebook không dừng — ở production thì KHÔNG bắt, cứ để nó
    #   chết và trả exit code khác 0.

after = con.execute("SELECT count(*) FROM core.orders").fetchone()[0]
print(f"core.orders: {before} -> {after}   (phải BẰNG NHAU)")
#   Bằng nhau = bằng chứng cho luật lớn của bài: quarantine -> gate -> load.
#   Lệch nhau = gate của bạn đang chạy SAU load_good; kiểm lại validate_day.

con.sql("""
    SELECT check_name, pct_failed, threshold_pct, passed FROM ops.dq_report
    WHERE order_date = DATE '2026-06-19' AND check_name = 'total_mismatch'
    ORDER BY run_at DESC LIMIT 1
""").show()
# threshold_pct = 0.1, passed = false.
# Dòng report TỒN TẠI DÙ RUN ĐÃ CHẾT — chính là lý do report() gọi TRƯỚC
# check_gate(). Đảo thứ tự thì ngày hỏng chỉ để lại một dòng alert, mất luôn con
# số vượt bao nhiêu và tình hình các check khác hôm đó.

con.sql("SELECT severity, source, message FROM ops.alerts ORDER BY alert_ts").show()
# Hai dòng: freshness (Task 9) rồi dq_gate (Task 10).

del THRESHOLDS["total_mismatch"]
THRESHOLDS.update({CHECK_FOR[r]: t for r, t in TOL.items()})
print(THRESHOLDS)      # total_mismatch phải về 1.0
#   del xoá dấu vết override; update nạp lại TOÀN BỘ từ contract, nên có lỡ ghi
#   đè key nào khác cũng sạch luôn.

# %% ── Task 11 — quy mô đầy đủ ─────────────────────────────────────────
# Đổi SCALE = "full" ở cell 2, restart kernel, chạy lại tới Task 8.
con.execute("DELETE FROM ops.dq_report; DELETE FROM ops.alerts;")
con.close()
# labpaths tự re-root MỌI đường dẫn (warehouse, quarantine, tmp) -> small và full
# KHÔNG BAO GIỜ trộn. Đó chính là lý do bài bắt dùng labpaths thay vì gõ đường dẫn.
#
# Mở Task Manager trong lúc ngày cao điểm chạy: python leo lên ~2-3 GB (unnest
# cộng ba dim join trên 3,6M dòng — run nặng nhất từ đầu lab), mọi core bận, vẫn
# nằm gọn trong memory_limit 8GB.
# Dự kiến trên máy chuẩn: ngày thường ~5-10s, ngày cao điểm ~15-25s.
#
# ĐÍCH: staged khớp CHÍNH XÁC manifest full — 1.180.490 / 3.643.024 / 3.602.310.
# Tỉ lệ quarantine VẪN ~1,7% ở gấp 20 lần sản lượng (06-02 ~19.700 dòng ≈1,67%;
# 06-19 ~60.600 ≈1,66%). Ghi timing cạnh số small.

# %% ── Task 12 — cầu nối công cụ: pandera ─────────────────────────────
section("TASK 12 — cùng bộ luật, viết dưới dạng KHAI BÁO")
# Về SCALE = "small". Cài: pip install pandera
# Chạy: python -m work.a05_validation.a05_pandera_demo
#
# Bạn đã TỰ DỰNG một bộ validation bằng tay, và đó là cách trung thực duy nhất để
# gặp một validation FRAMEWORK: giờ bạn biết chính xác nó đang làm hộ bạn cái gì.
# Luật §7.5: công cụ KHÔNG thay thế bản tự dựng, nó đứng CẠNH. Pipeline vẫn gate
# bằng SQL — vì check phải chạy Ở NƠI 82 triệu dòng đang nằm.
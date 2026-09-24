"""A14 Task 4 — work/fastpipe.py: badpipe sau sáu lần sửa.

  python -m work.a14_performance.a14_fastpipe 2026-06-05
  $env:ETL_LAB_SCALE="full"; python -m work.a14_performance.a14_fastpipe 2026-06-05

Ghi ra tmp/perf_fixed/ chứ không phải tmp/perf/ — GIỮ CẢ HAI để so bằng EXCEPT.

CÁCH DÙNG FILE NÀY CHO ĐÚNG: đừng chép nguyên bản này vào rồi chạy một phát.
Task 4 dạy cái kỷ luật sửa MỘT thứ, chạy lại, ghi số, rồi mới sửa cái tiếp.
Chép badpipe sang fastpipe, rồi gỡ từng cái comment FIX dưới đây theo thứ tự,
mỗi lần một cái. Cái kỷ luật đó CHÍNH LÀ kỹ năng của bài; sửa gộp thì bạn có
một con số tổng và không quy được cho ai.

Thứ tự sửa là thứ tự CHI PHÍ ĐO ĐƯỢC, không phải thứ tự thú vị. Ở một máy dev,
bước validate chiếm 99% thời gian còn strftime với DISTINCT cộng lại chưa tới
nửa giây — nên fix 1 là cái loop, không phải mấy cái bắt mắt.
"""
from __future__ import annotations

import sys
import time

import duckdb

from work.a14_performance.a14_bootstrap import DIMS, LAKE, SCALE, SIX, TMP

DAY = sys.argv[1] if len(sys.argv) > 1 else "2026-06-05"
OUT = TMP / "perf_fixed"
OUT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{TMP.as_posix()}';")

print(f"fastpipe [{SCALE}]  ngày {DAY}  ->  {OUT}")
t_all = t0 = time.perf_counter()


def step(name: str) -> None:
    global t0
    print(f"  {name:10s} {time.perf_counter() - t0:8.2f} s", flush=True)
    t0 = time.perf_counter()


# ── bước 1: cắt một ngày ────────────────────────────────────────────────────
# FIX 2 — lọc trên CỘT PHÂN VÙNG. strftime giấu cột vào trong một hàm chuỗi mà
#   không optimizer nào nhìn xuyên qua được, nên mọi file đều bị mở và mọi dòng
#   đều bị định dạng. order_date là cột phân vùng: cả file không liên quan
#   không bao giờ được mở. EXPLAIN ANALYZE sẽ in "Scanning Files: 1/N".
# FIX 3 — bỏ DISTINCT và ORDER BY, SAU KHI đã chứng minh chúng vô dụng (cell 5
#   của explore đếm hai kiểu rồi so). Không dòng nào trong lake trùng nhau từng
#   byte — kể cả bản sửa của A08 cũng khác updated_at — nên DISTINCT băm 60k
#   dòng rộng để chẳng được gì; còn ORDER BY sắp xếp thứ không ai yêu cầu sắp.
#   ĐỪNG xoá theo niềm tin: nếu số của BẠN lệch thì dừng lại điều tra, một con
#   bug dữ liệu quan trọng hơn mọi phép tăng tốc.
# FIX 4 — gọi tên cột. day_raw đang kéo items và meta, hai cột rộng nhất lake,
#   qua từng bước, rồi gửi luôn cho finance là những người không dùng tới.
#   Hợp đồng cần sáu cột. Đây cũng là chỗ khiến "SELECT *" ở bước 3 thành thật.
con.execute(f"""
    CREATE TABLE day_raw AS
    SELECT {SIX}
    FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)
    WHERE order_date = DATE '{DAY}'""")
step("slice")

# ── bước 2: kiểm tra ────────────────────────────────────────────────────────
# FIX 1 — CÁI LOOP THÀNH MỘT CÂU LỆNH. Đây là phần thắng lớn nhất của ngày.
#   Bản cũ chuyển 60k dòng sang Python rồi chạy 60k lệnh INSERT đơn lẻ, mỗi
#   lệnh khoảng một mili giây chi phí cố định (parse, plan, commit). Còn phần
#   "kiểm tra" thì chỉ là một mệnh đề WHERE đội lốt.
#   Bài học cốt lõi: ở lại phía tập hợp. Đừng bao giờ khiêng từng dòng qua
#   Python để nói một điều mà SQL nói xong trong một mệnh đề.
con.execute("CREATE TABLE day_ok AS "
            "SELECT * FROM day_raw WHERE order_total IS NOT NULL")
step("validate")

# ── bước 3: bản trích cho finance ───────────────────────────────────────────
# FIX 6 — row group về mức lành mạnh. ROW_GROUP_SIZE 5000 chẻ cả một ngày nhỏ
#   thành hơn 10 nhóm (~200 ở full scale). Phía GHI gần như không quan tâm,
#   nhưng phía ĐỌC trả giá mỗi ngày về sau — Task 6 đo đúng cái giá đó.
con.execute(f"""
    COPY (SELECT * FROM day_ok)
    TO '{(OUT / f"orders_{DAY}.parquet").as_posix()}'
    (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
step("extract")

# ── bước 4: mart store_daily ────────────────────────────────────────────────
# FIX 5 — bỏ phép join không ai đặt hàng. Mart join sang customers (60k dòng ở
#   small, 1,2 TRIỆU ở full) rồi không dùng một cột nào của nó — ngày xưa có
#   người định làm mart theo quốc gia. Nó là LEFT JOIN về phía orders nên số
#   dòng không đổi: kết quả y hệt khi bỏ đi, còn cái giá là một bảng băm khổng
#   lồ ở phía build. Nhìn thấy nó trước khi xoá: EXPLAIN ANALYZE cái mart rồi
#   tìm HASH_JOIN được nuôi bởi một lần quét cỡ bảng customers.
#   Optimizer tự chọn phía build và thứ tự join giùm bạn — nhưng nó không thể
#   biết một phép join là VÔ NGHĨA. Xoá việc thừa là phần của con người.
#   (ORDER BY revenue DESC thì GIỮ: 400 dòng, và cái thứ hạng chính là sản phẩm.)
con.execute(f"""
    COPY (
        SELECT o.store_id, s.store_name, s.region,
               count(*) AS orders, sum(o.order_total) AS revenue
        FROM day_ok o
        JOIN read_csv('{(DIMS / "stores.csv").as_posix()}', header=true) s
          ON o.store_id = s.store_id
        GROUP BY 1, 2, 3 ORDER BY revenue DESC
    ) TO '{(OUT / f"store_daily_{DAY}.parquet").as_posix()}' (FORMAT parquet)""")
step("mart")

print(f"  {'TOTAL':10s} {time.perf_counter() - t_all:8.2f} s")
con.close()
print("\nSau MỖI lần sửa, chứng minh mình không làm hỏng gì: cell 6 của explore")
print("chạy EXCEPT theo CẢ HAI chiều, trên cả hai artifact. Nhanh mà sai thì")
print("không tính.")
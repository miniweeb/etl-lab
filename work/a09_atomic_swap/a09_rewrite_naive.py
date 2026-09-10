# work/a09_atomic_swap/a09_rewrite_naive.py -- ghi lại partition kiểu SAI: ngay tại chỗ
#
# usage: python work\a09_atomic_swap\a09_rewrite_naive.py <scale>
#
# ⚠️ SCRIPT NÀY XOÁ FILE THẬT. Nó unlink toàn bộ *.parquet trong thư mục live
# rồi mới ghi lại. Nếu rebuild_sql sai hoặc máy chết giữa chừng, partition
# 2026-06-19 mất và KHÔNG có bản backup nào. Trước lần chạy đầu tiên, copy thư
# mục đó ra chỗ an toàn (xem hướng dẫn Task 3).
#
# Đây là cách viết mà ai cũng viết lần đầu, và nó không hề "cẩu thả" -- nó chỉ
# là hai bước tuần tự hiển nhiên. Vấn đề nằm ở khoảng trống giữa hai bước.

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root, để import lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY, rebuild_sql

scale = sys.argv[1]
part = lake_dir(scale) / "orders" / f"order_date={DAY}"
con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")

t0 = time.time()
for f in part.glob("*.parquet"):         # bước 1: xoá file cũ
    f.unlink()
print(f"old files gone at t={time.time() - t0:.1f}s", flush=True)

# Bước 2: quét tám file CSV, dedupe, ghi ra. Ở small mất 2-3 giây, ở full
# khoảng 10-60 giây tuỳ ổ đĩa. Suốt quãng đó thư mục live đang rỗng, rồi đang
# chứa một file ghi dở.
con.execute(f"COPY ({rebuild_sql(scale)}) "
            f"TO '{(part / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
print(f"new file ready at t={time.time() - t0:.1f}s  <- that was the torn window",
      flush=True)

# Đổi thứ tự hai bước cũng không cứu được: ghi mới trước rồi xoá cũ sau thì
# người đọc thấy cũ + mới cùng lúc, tức đếm trùng -- đúng cái sev-2 mà contract
# shopcore đã gọi tên. Khoảng trống mới là thủ phạm, không phải thứ tự.
#
# Một chi tiết nữa: tám file nhỏ ban đầu bị gộp thành MỘT file data_0.parquet.
# Sau lần chạy này thư mục chỉ còn một file. Đó là hệ quả phụ, không phải mục
# tiêu -- nhưng nó giải thích vì sao chạy lần thứ hai sẽ nhanh hơn ở bước xoá.
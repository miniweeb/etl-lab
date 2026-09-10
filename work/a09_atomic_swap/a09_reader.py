# work/a09_atomic_swap/a09_reader.py -- người đọc vô tội: poll count của một partition
#
# usage: python work\a09_atomic_swap\a09_reader.py <scale> [seconds] [--retry]
#   python work\a09_atomic_swap\a09_reader.py small 10
#   python work\a09_atomic_swap\a09_reader.py small 20 --retry
#
# Script này đóng vai analyst đang mở dashboard trong lúc bạn ghi lại partition.
# Nó chỉ in ra khi con số ĐỔI. Nên: dòng đầu là trạng thái trước, mọi dòng sau
# đều là tin xấu.
#
# Chạy ở TERMINAL 1. Terminal 2 để chạy script ghi.

import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root, để import lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY

scale = sys.argv[1]
seconds = float(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != "--retry" else 30

# Glob `*/*.parquet` + hive_partitioning=true: DuckDB đọc giá trị order_date từ
# TÊN THƯ MỤC. Đây đúng là cách một dashboard thật đọc lake -- và cũng chính là
# lý do Task 4 hỏng: glob này vơ luôn cả thư mục staging nếu bạn để nó bên trong.
glob = f"{(lake_dir(scale) / 'orders').as_posix()}/*/*.parquet"
con = duckdb.connect()                    # in-memory, KHÔNG mở warehouse.duckdb
con.execute("SET memory_limit='2GB';")    # người đọc lịch sự: đếm thì tốn gì đâu
q = (f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true) "
     f"WHERE order_date = DATE '{DAY}'")


def read_once():
    return con.execute(q).fetchone()[0]


def read_with_retry(attempts=15, wait=0.3):
    """Coi 'không có dòng nào' và 'file biến mất' là 'đang swap, thử lại đi'.

    Chỗ này hợp lệ vì ta BIẾT partition này có dữ liệu. Nên 0 không phải là câu
    trả lời, 0 là dấu hiệu đang ở giữa swap. Kiểu xử lý này gọi là retry-on-read:
    rẻ ở phía đọc, và nó biến 'trả lời sai' thành 'trả lời chậm một nhịp'.

    15 lần x 0,3s = tối đa 4,5 giây kiên nhẫn. Khoảng trống giữa hai lần rename
    bình thường chỉ ~4 mili giây, nên biên an toàn rất rộng.
    """
    last = None
    for _ in range(attempts):
        try:
            n = read_once()
            if n > 0:
                return n
            last = n
        except Exception as e:
            last = e
        time.sleep(wait)
    if isinstance(last, Exception):
        raise last
    return last


print(f"watching order_date={DAY} ({scale}) for {seconds:.0f}s", flush=True)
prev, t_end = object(), time.time() + seconds
while time.time() < t_end:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    try:
        n = read_with_retry() if "--retry" in sys.argv else read_once()
        out = f"count={n}"
    except Exception as e:
        out = f"{type(e).__name__}: {str(e)[:90]}"
    if out != prev:                      # chỉ in khi câu trả lời THAY ĐỔI
        print(f"{ts}  {out}", flush=True)
        prev = out
    time.sleep(0.1)

# Kỳ vọng ở Task 2 (lake chưa bị đụng, small): đúng MỘT dòng count=180183.
# Lake ổn định thì người đọc im lặng. Nhiều hơn một dòng nghĩa là có ai đó đang
# ghi -- hoặc bạn quên tắt script ở terminal kia.
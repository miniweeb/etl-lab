# work/a09_atomic_swap/a09_common.py -- những mảnh dùng chung cho A09
#
# Cả năm file A09 nằm trong work/a09_atomic_swap/, cùng khuôn với
# a07_idempotency/ và a08_load_strategies/.
#
# Nhưng A09 chạy script kiểu ĐƯỜNG DẪN, không phải `python -m work....`:
#     python work\a09_atomic_swap\a09_reader.py small
# Chạy kiểu đó, Python đặt sys.path[0] = work/a09_atomic_swap/, nên
# `from a09_common import DAY` tìm thấy file này ngay bên cạnh.
#
# Vì sao không dùng -m như A07/A08? Vì Task 3, 5, 6 cần chạy reader và writer
# SONG SONG ở hai terminal, và lệnh đường dẫn ngắn hơn, gõ lại nhanh hơn khi
# bạn phải lặp nó chục lần. Cách nào cũng được, miễn không trộn: `-m` sẽ đặt
# sys.path[0] = repo root và `from a09_common` sẽ gãy.
#
# Còn `lib` thì không nằm trong work/, nên mỗi script phải tự chèn repo root vào
# sys.path -- dòng sys.path.insert bên dưới, lặp lại y hệt trong cả bốn file.

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root, để import lib
from lib.labpaths import raw_orders_dir

DAY = "2026-06-19"          # ngày cao điểm của tháng 1, kỷ nguyên v1

# Schema v1 khai tường minh. Đây là cùng danh sách cột bạn đã dùng ở A08:
# ép kiểu bằng tay thay vì để read_csv tự đoán, vì tự đoán sẽ đọc mỗi file ra
# một schema khác nhau tuỳ dữ liệu file đó.
V1_COLUMNS = ("{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
              "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
              "'payment_method':'VARCHAR','order_total':'VARCHAR',"
              "'items':'VARCHAR','meta':'VARCHAR'}")


def rebuild_sql(scale: str, day: str = DAY) -> str:
    """Câu SELECT dựng lại đúng MỘT partition order_date từ raw CSV.

    Bản sửa của ngày D về trong các file D..D+7 (SPEC 3.4) -- con số 7 chính là
    late_corrections.window_days của contract A06, cùng con số bạn dùng ở A08.
    Nên: quét tám file, giữ lại dòng có order_date = D, rồi giữ bản updated_at
    mới nhất cho mỗi order_id. Chính là phép dedupe của A08.

    Chi phí: tám file CSV. Ở small là vài giây, ở full là hàng chục giây. Chính
    quãng đó là torn window nếu bạn ghi thẳng vào thư mục live.
    """
    d0 = date.fromisoformat(day)
    files = [raw_orders_dir(scale) / f"orders_{d0 + timedelta(days=k)}.csv"
             for k in range(8)]
    files = [f.as_posix() for f in files if f.exists()]
    return f"""
    WITH src AS (
      SELECT *,
             try_strptime(order_ts,
                  ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                   '%Y-%m-%dT%H:%M:%S']) AS order_ts_clean,
             CAST(order_ts_clean AS DATE) AS order_date
      FROM read_csv({files}, header=true, columns={V1_COLUMNS})
      WHERE order_date = DATE '{day}'
    ),
    ranked AS (
      SELECT *, row_number() OVER (PARTITION BY order_id
                                   ORDER BY updated_at DESC) AS rn
      FROM src
    )
    SELECT * EXCLUDE (order_date, rn) FROM ranked WHERE rn = 1
    """
    # HAI cột bị EXCLUDE, và chúng bị loại vì hai lý do ngược nhau:
    #
    # order_date -- BỎ. Giá trị partition nằm ở TÊN THƯ MỤC
    # (order_date=2026-06-19). File do PARTITION_BY ghi ra không chứa cột đó
    # bên trong. Nếu file bạn dựng lại có cột order_date mà các file khác
    # không có, thì mọi lần đọc nhiều partition sẽ chết vì schema lệch.
    #
    # order_ts_clean -- GIỮ. Loader A02 đã ghi cột này vào MỌI file trong lake.
    # Một lần đọc bằng glob đòi hỏi mọi file cùng schema. Bỏ nó ở đây thì
    # Task 8 và A10 sẽ chết với lỗi schema mismatch.
    #
    # Luật chung: file dựng lại phải khớp CHÍNH XÁC schema của các file lake
    # còn lại. Không thừa cột, không thiếu cột.
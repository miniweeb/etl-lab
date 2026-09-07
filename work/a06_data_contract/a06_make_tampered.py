"""Chế ba bản sao hỏng CÓ CHỦ Ý của một file ngày ở small scale.

Mỗi bản nằm trong thư mục RIÊNG để cả ba giữ được TÊN FILE THẬT — chúng phải đi
QUA được check đặt tên thì mới thử được các tầng sâu hơn.

Không phải tamper ngẫu nhiên: một cột `currency` mới và order_id dạng chuỗi
chính là thứ shopcore sẽ ship dưới tên "month 2" ở A10.
"""
import csv
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
SRC = DATA_ROOT / "small" / "raw" / "orders" / "orders_2026-06-02.csv"
OUT = DATA_ROOT / "tmp" / "tampered"
N = 5000               # vài nghìn dòng là đủ — gate vốn chỉ lấy mẫu

with SRC.open(newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    data = [next(reader) for _ in range(N)]


def write(folder: str, hdr: list[str], rows: list[list[str]]) -> None:
    p = OUT / folder / SRC.name
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        w.writerows(rows)
    print(f"wrote {p}")


i = header.index("payment_method")     # 1. cột BỊ XOÁ: nó biến mất lặng lẽ
write("dropped_col", header[:i] + header[i+1:], [r[:i] + r[i+1:] for r in data])

write("added_col", header + ["currency"],       # 2. cột THÊM ở cuối
      [r + ["USD"] for r in data])

j = header.index("order_id")           # 3. đổi KIỂU: BIGINT -> chuỗi 'ORD-...'
write("type_change", header,
      [r[:j] + [f"ORD-{int(r[j]):010d}"] + r[j+1:] for r in data])

# Tầng nào bắt được tamper nào? (câu hỏi journal của Task 7)
#   dropped_col / added_col -> tầng SCHEMA
#   type_change             -> LỌT QUA schema (cùng tên, cùng thứ tự!) và bị
#                              tầng GIÁ TRỊ bắt ở tỉ lệ cast hỏng 100%
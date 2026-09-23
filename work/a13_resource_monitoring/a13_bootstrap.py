"""A13 bootstrap — path, hằng số, và mấy hàm đo dùng chung.

Giữ nguyên luật của A12: file này KHÔNG mở warehouse.duckdb. A13 còn một lý do
riêng để giữ luật đó — resmon chạy một câu lệnh con, mà câu lệnh con ấy có thể
chính là runner A11 đang giữ warehouse. resmon mà cũng mở file thì nó trở thành
writer thứ hai và giết đúng thứ nó định đo.

Import: from work.a13_resource_monitoring.a13_bootstrap import SCALE, TMP, ...
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

from lib.labpaths import lake_dir, manifest_dir, raw_orders_dir, tmp_dir

# ── SCALE module-level, cùng lý do A12 ──────────────────────────────────────
# A13 còn một lý do nữa: mỗi task nói rõ nó đo ở scale nào. Task 1,2,4b làm ở
# small; Task 3,4,5,6,7 đo ở full. Đổi bằng env rồi mở lại terminal.
SCALE = os.environ.get("ETL_LAB_SCALE", "small")

RAW = raw_orders_dir(SCALE)
MANIFEST = manifest_dir(SCALE)
TMP = tmp_dir(SCALE)
LAKE = lake_dir(SCALE) / "orders_v=*"

ALL_START, ALL_END = "2026-06-01", "2026-08-08"
PROBE_DAY = "2026-06-05"           # ngày dùng cho Task 3 và Task 7

# Máy chuẩn (SPEC §1). A12 dùng mấy số này để tính ngân sách TRÊN GIẤY; hôm nay
# chúng là thứ để ĐỐI CHIẾU với con số đo được.
CORES, RAM_GB = 8, 16

# Sweep của Task 7. Đi xuống tới 64MB có chủ ý: phải thấy đầu gối của đường
# cong thì mới biết giới hạn nào là nhỏ nhất còn dùng được.
SWEEP = ["8GB", "4GB", "2GB", "1GB", "512MB", "256MB", "128MB", "64MB"]

GB = 2 ** 30


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def daterange(start: str, end: str):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def manifest(day: str) -> dict:
    return json.loads((MANIFEST / f"orders_{day}.json").read_text())


def duck(memory_limit: str = "8GB", threads: int = 8):
    """Một connection in-memory, đã đặt sẵn ba SET của lab.

    memory_limit là THAM SỐ chứ không phải hằng số, vì hôm nay nó chính là thí
    nghiệm. Hai SET còn lại thì không bao giờ đổi:
      - temp_directory: spill phải rơi xuống ổ dữ liệu. Task 4b chứng minh.
      - preserve_insertion_order=false: cho phép engine bỏ thứ tự dòng khi
        không ai cần, nhờ vậy sort/copy ở full scale không vỡ RAM (A14).
    """
    import duckdb
    TMP.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'; SET threads={threads};")
    con.execute(f"SET temp_directory='{TMP.as_posix()}'; "
                f"SET preserve_insertion_order=false;")
    return con


def os_drive() -> str:
    return os.environ.get("SystemDrive", "C:").upper()


def gb(n: float) -> str:
    return f"{n / GB:.2f} GB"


if __name__ == "__main__":
    section(f"A13 bootstrap  [scale={SCALE}]")
    print("raw csv  :", len(list(RAW.glob("orders_*.csv"))), "(expect 69)")
    print("lake     :", LAKE)
    print("tmp      :", TMP, " ->", "TRỐNG" if not any(TMP.glob('*')) else "CÒN FILE")
    print("ổ tmp    :", TMP.drive, " vs ổ hệ điều hành", os_drive(),
          "->", "PASS" if TMP.drive.upper() != os_drive() else "WARN: tmp nằm trên ổ OS!")
    print()
    print("Task 4b sẽ chứng minh dòng trên bằng cách hỏi chính engine, chứ không")
    print("phải bằng cách đọc lại giá trị mình vừa gõ vào.")
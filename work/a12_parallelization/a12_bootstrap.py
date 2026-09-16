"""A12 bootstrap — hằng số, đường dẫn, và đo peak RSS.

KHÁC A10 MỘT ĐIỂM CỐT TỬ: file này KHÔNG mở warehouse.duckdb.
A10 boot() trả sẵn một connection ghi được; ở A12 điều đó là thuốc độc — worker
nào lỡ import boot() sẽ giành file với committer. Nên A12 tách đôi:
  - a12_bootstrap : chỉ path + hằng số + đo đạc. Worker import cái này.
  - commit_stage  : nơi DUY NHẤT gọi duckdb.connect(warehouse_path).

Import: from work.a12_parallelization.a12_bootstrap import SCALE, STAGE, ...
"""
from __future__ import annotations

from datetime import date, timedelta
import json
import time

import psutil

from lib.labpaths import data_root

# ── SCALE là MODULE-LEVEL, có chủ ý ─────────────────────────────────────────
# Windows spawn: process con khởi động bằng cách RE-IMPORT file này. Biến
# module-level thì con nhìn thấy; args.scale parse ở __main__ thì KHÔNG.
# Đổi scale = sửa đúng dòng này (hoặc đặt env ETL_LAB_SCALE trước khi chạy).
import os
SCALE = os.environ.get("ETL_LAB_SCALE", "small")

ROOT = data_root(SCALE)
RAW = ROOT / "raw" / "orders"
MANIFEST = ROOT / "raw" / "manifest"
TMP = ROOT / "tmp"
STAGE = TMP / "backfill_stage"

ALL_START, ALL_END = "2026-06-01", "2026-08-08"
BAD_DAY = "2026-07-22"            # A11 đã dạy cách đọc nó: store_rejects=true
SPIKE_DAYS = ("2026-06-19", "2026-07-11")   # ~3x ngày thường -> straggler

# Ngân sách máy chuẩn (SPEC §1): 16 GB RAM, 8 core. Giữ lại ~4 GB cho OS.
CORES, RAM_GB, OS_RESERVE_GB = 8, 16, 4


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def daterange(start: str, end: str):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def manifest(day: str) -> dict:
    return json.loads((MANIFEST / f"orders_{day}.json").read_text())


def biggest_first(days):
    """Task 8 — xếp theo bytes giảm dần.

    Dùng `bytes` của manifest chứ không `rows`: chi phí staging là chi phí ĐỌC
    và PARSE văn bản, mà cái đó tỉ lệ với byte. Một ngày nhiều dòng ngắn rẻ hơn
    một ngày ít dòng nhưng items JSON dài.
    """
    return sorted(days, key=lambda d: manifest(d)["bytes"], reverse=True)


def legal(workers: int, threads: int, mem_gb: float) -> tuple[bool, str]:
    """Task 1 bằng code — để bảng trong journal có thứ đối chiếu.

    Không có gì trong DuckDB hay multiprocessing kiểm hộ bạn hai bất đẳng thức
    này. memory_limit là giới hạn MỖI PROCESS; tổng là việc của bạn.
    """
    t_ok = workers * threads <= CORES
    m_ok = workers * mem_gb + OS_RESERVE_GB <= RAM_GB
    why = []
    if not t_ok:
        why.append(f"{workers}x{threads}={workers * threads} thread > {CORES} core")
    if not m_ok:
        why.append(f"hứa {workers * mem_gb:.1f}GB + {OS_RESERVE_GB}GB OS > {RAM_GB}GB")
    return (t_ok and m_ok), "; ".join(why) or "ok"


# ── đo peak RSS ──────────────────────────────────────────────────────────────
class PeakRSS:
    """Cộng RSS của process này + TẤT CẢ con, mỗi 0.5s, giữ đỉnh.

    Phải cộng cả con: `memory_limit` là lời hứa của từng worker, còn thứ giết
    máy là TỔNG. Chỉ đo process cha sẽ ra một con số nhỏ xinh và vô nghĩa.
    NoSuchProcess là bình thường — worker xong việc và biến mất giữa lúc quét.
    """

    def __init__(self, interval: float = 0.5):
        self.interval, self.peak_gb = interval, 0.0
        self._stop = None

    def _loop(self):
        me = psutil.Process()
        while not self._stop.is_set():
            total = 0
            for p in [me] + me.children(recursive=True):
                try:
                    total += p.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
                except psutil.AccessDenied:
                    pass
            self.peak_gb = max(self.peak_gb, total / 2 ** 30)
            time.sleep(self.interval)

    def __enter__(self):
        import threading
        self._stop = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        time.sleep(self.interval)   # cho vòng quét cuối kịp ghi
        return False


if __name__ == "__main__":
    section(f"A12 bootstrap  [scale={SCALE}]")
    print("raw csv  :", len(list(RAW.glob("orders_*.csv"))), "(expect 69)")
    print("stage dir:", STAGE)
    print()
    for w, t, m in [(1, 8, 8), (2, 4, 5), (4, 2, 3), (8, 1, 1.5),
                    (8, 8, 1.5), (8, 1, 12.8)]:
        ok, why = legal(w, t, m)
        print(f"  {w}x{t}  mem={m}GB  -> {'LEGAL' if ok else 'ILLEGAL'}  {why}")
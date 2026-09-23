"""A13 Task 1 — resmon: chạy một câu lệnh, lấy mẫu tài nguyên, ghi ra CSV.

  python -m work.a13_resource_monitoring.a13_resmon --out work/mon_test.csv -- python -c "import time; x=bytearray(300*2**20); time.sleep(3)"
  python -m work.a13_resource_monitoring.a13_resmon --out work/mon_par.csv --max-rss-gb 12 -- python -m work.a12_parallelization.a12_parallel_runner --workers 4 --threads 2 --mem 3GB

Mọi thứ sau dấu `--` là câu lệnh được chạy và được đo.

Vì sao là lớp bọc chứ không phải hai dòng psutil nhét vào trong pipeline: một
pipeline thường là một tiến trình cha cộng N con. In RSS từ bên trong tiến
trình cha sẽ ra 0.2 GB trong lúc mấy đứa con ôm hàng gigabyte — một con số
trông rất thật thà mà sai hai chục lần. Lớp bọc nhìn thấy cả cây, và nó ghi lại
được, mà cái gì ghi lại được thì đem ra so sánh được.

LUẬT: file này KHÔNG mở warehouse.duckdb trong lúc câu lệnh con đang chạy.
Câu lệnh con có thể chính là runner A11 đang giữ file đó. Phần ghi ops.alerts
(nếu bật --max-rss-gb) chỉ chạy SAU khi con thoát — luật một-writer của A12.
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

import psutil

GB = 2 ** 30


class ProcessTree:
    """Lấy mẫu một tiến trình và toàn bộ con của nó.

    QUAN TRỌNG: giữ ĐÚNG MỘT psutil.Process cho mỗi pid trong suốt lần chạy.
    cpu_percent(interval=None) đo phần CPU tính từ lần gọi trước TRÊN CÙNG MỘT
    đối tượng, nên một đối tượng vừa tạo mới sẽ báo 0 mãi mãi — và pool worker
    của bạn trông như đang ngồi chơi trong lúc tám core đang cháy. Đó là lỗi
    phổ biến số một của bài này, và nó không kêu một tiếng nào.
    """

    def __init__(self, pid: int):
        self.cache: dict[int, psutil.Process] = {pid: psutil.Process(pid)}
        self.root_pid = pid

    def sample(self) -> dict | None:
        root = self.cache.get(self.root_pid)
        if root is None:
            return None
        try:
            for child in root.children(recursive=True):
                self.cache.setdefault(child.pid, child)
                #   setdefault chứ không gán đè: gặp lại một pid đã biết thì
                #   GIỮ đối tượng cũ, vì lịch sử CPU nằm trong đối tượng đó.
        except psutil.NoSuchProcess:
            return None                        # câu lệnh đã thoát
        rss, cpu, read_b, write_b, alive = 0, 0.0, 0, 0, 0
        for pid, p in list(self.cache.items()):
            try:
                with p.oneshot():              # gom syscall của một tiến trình
                    rss += p.memory_info().rss
                    cpu += p.cpu_percent(interval=None)
                    io = p.io_counters()
                    read_b += io.read_bytes
                    write_b += io.write_bytes
                    alive += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                del self.cache[pid]            # tiến trình đó vừa thoát
        return {"rss": rss, "cpu_pct": round(cpu, 1),
                "read_bytes": read_b, "write_bytes": write_b, "nprocs": alive}


def write_alert(cmd: str, peak_gb: float, budget_gb: float) -> None:
    """Task 8 mở rộng (§7.3) — một dòng ops.alerts, severity 'warn'.

    Cảnh báo chứ KHÔNG giết: resmon là cái đồng hồ, không phải cái cầu dao. Một
    monitor tự ý kill tiến trình sẽ bị người ta tắt đi, rồi bạn mất luôn cả số
    liệu lẫn cảnh báo.

    Gọi hàm này SAU khi con đã thoát. Trong lúc con còn sống, nó có thể đang
    giữ warehouse.duckdb, và một writer thứ hai thì hỏng — kể cả read_only,
    đúng như Task 2 của A12 đã chứng minh.
    """
    import duckdb
    from lib.labpaths import warehouse_path
    from work.a13_resource_monitoring.a13_bootstrap import SCALE

    con = duckdb.connect(str(warehouse_path(SCALE)))
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.alerts (
                       alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
                       source VARCHAR, message VARCHAR)""")
    con.execute("INSERT INTO ops.alerts VALUES (now(), NULL, 'warn', 'resmon', ?)",
                [f"peak RSS {peak_gb:.2f} GB vượt ngân sách {budget_gb} GB — {cmd[:200]}"])
    #   source='resmon' được điền đàng hoàng. A07 để sót cột này nên 5 alert cũ
    #   đều source=NULL, và lọc theo source sẽ bỏ sót chúng. Đừng lặp lại.
    con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="monitor.csv")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--max-rss-gb", type=float, default=None,
                    help="vượt ngưỡng thì ghi một dòng warn vào ops.alerts")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        ap.error("đưa câu lệnh cần chạy sau dấu --")

    # child = subprocess.Popen(cmd)
    if cmd[0] in ("python", "python.exe"):
        cmd = [sys.executable] + cmd[1:]
        #   "python" trần được Windows phân giải theo PATH hệ thống, không theo
        #   venv đang active — lệnh con sẽ chạy bằng Python khác và thiếu
        #   duckdb. sys.executable là chính interpreter đang chạy resmon.
    child = subprocess.Popen(cmd)
    tree = ProcessTree(child.pid)
    t0 = time.time()
    rows: list[dict] = []
    breached = False
    while child.poll() is None:
        snap = tree.sample()
        if snap is not None:
            snap["t_s"] = round(time.time() - t0, 2)
            rows.append(snap)
            if args.max_rss_gb and snap["rss"] / GB > args.max_rss_gb:
                breached = True                # nhớ lại, và ĐO TIẾP
        time.sleep(args.interval)
    wall = time.time() - t0

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["t_s", "rss", "cpu_pct",
                                          "read_bytes", "write_bytes", "nprocs"])
        w.writeheader()
        w.writerows(rows)

    if not rows:
        print("resmon: câu lệnh xong trước cả mẫu đầu tiên")
        return child.returncode

    peak_gb = max(r["rss"] for r in rows) / GB
    # Mẫu đầu tiên của cpu_percent LUÔN bằng 0 — bỏ nó khi tính trung bình.
    cpu_rows = rows[1:] or rows
    print("--- resmon summary ---")
    print(f"wall clock : {wall:8.1f} s")
    print(f"peak RSS   : {peak_gb:8.2f} GB")
    print(f"avg CPU    : {sum(r['cpu_pct'] for r in cpu_rows) / len(cpu_rows):8.0f} %"
          "   (100 = một core bận)")
    # max chứ không phải mẫu cuối: lúc một worker thoát, bộ đếm IO của nó RỜI
    # khỏi tổng, nên mẫu cuối cùng nhỏ hơn sự thật.
    print(f"disk read  : {max(r['read_bytes'] for r in rows) / GB:8.2f} GB")
    print(f"disk write : {max(r['write_bytes'] for r in rows) / GB:8.2f} GB")
    print(f"max nprocs : {max(r['nprocs'] for r in rows):8d}   <- cây tiến trình, "
          "1 nghĩa là bạn đang đo mỗi tiến trình cha")
    print(f"samples    : {len(rows):8d}   -> {args.out}")

    if breached:
        print(f"\nBREACH: peak {peak_gb:.2f} GB > ngân sách {args.max_rss_gb} GB")
        try:
            write_alert(" ".join(cmd), peak_gb, args.max_rss_gb)
            print("đã ghi 1 dòng ops.alerts (severity=warn, source=resmon)")
        except Exception as e:
            print(f"không ghi được alert: {type(e).__name__}: {e}")
            #   Nuốt lỗi ở ĐÂY là đúng: warehouse còn bị khoá thì cũng không
            #   được phép làm hỏng con số vừa đo. Số liệu đã nằm trong CSV rồi.
    return child.returncode


if __name__ == "__main__":
    sys.exit(main())
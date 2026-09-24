"""A14 Task 1 — cái đồng hồ bấm giây. Luật của cả bài: không có phép đo nào
được phát biểu mà không đi qua đây.

  from work.a14_performance.a14_bench import bench
  bench(con, "nhãn", "SELECT ...")

Ba thứ làm cho một phép đo trung thực, và cả ba đều nằm trong 15 dòng dưới:

  1. WARM-UP. Lần chạy đầu trả giá cache đĩa lạnh — A13 đã cho bạn thấy đúng
     chuyện này: cùng một lần nạp, 34,4 s rồi 23,0 s, mà số byte đọc y hệt.
     Ở đây ta trả cái giá đó TRƯỚC rồi mới bắt đầu bấm giờ.
  2. MEDIAN của 3, không phải trung bình. Một cú giật của antivirus không được
     phép làm xê dịch con số.
  3. fetchall(). Thiếu nó là bạn chỉ đo phần gửi câu lệnh đi, không đo phần
     engine làm xong việc.
"""
from __future__ import annotations

import statistics
import time


def bench(con, label: str, sql: str, runs: int = 3, warmup: int = 1) -> float:
    """Chạy sql, trả về median (giây). In một dòng tóm tắt."""
    for _ in range(warmup):
        con.execute(sql).fetchall()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    med = statistics.median(times)
    print(f"{label:30s} median {med * 1000:9.1f} ms   "
          f"(min {min(times) * 1000:.1f} / max {max(times) * 1000:.1f})")
    return med


def bench_cold(con, label: str, sql: str) -> float:
    """Một lần chạy DUY NHẤT, không warm-up — để nhìn thấy cái giá cache lạnh.

    Dùng đúng một lần ở Task 1 cho biết nó trông ra sao, rồi thôi. Mọi con số
    mang đi so sánh đều phải là bench() ở trên.
    """
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    dt = time.perf_counter() - t0
    print(f"{label:30s} COLD   {dt * 1000:9.1f} ms")
    return dt


if __name__ == "__main__":
    # Task 1 nghiệm thu: hai lần gọi bench trên CÙNG một query phải gần nhau,
    # và bạn phải thấy lần cold chậm hơn ít nhất một lần.
    from work.a14_performance.a14_bootstrap import LAKE, NORMAL_DAY, duck, section

    section("Task 1 — kiểm cái đồng hồ")
    con = duck()
    q = (f"SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet', "
         f"hive_partitioning=true) WHERE order_date = DATE '{NORMAL_DAY}'")

    bench_cold(con, "lần đầu, cache lạnh", q)
    a = bench(con, "bench lần 1", q)
    b = bench(con, "bench lần 2", q)
    con.close()

    drift = abs(a - b) / max(a, b) * 100
    print(f"\nchênh giữa hai lần bench: {drift:.1f}%")
    print("Dưới ~10% là cái đồng hồ dùng được. Trên đó thì máy đang bận việc")
    print("khác — đóng bớt app rồi đo lại, đừng ghi con số nhiễu vào journal.")
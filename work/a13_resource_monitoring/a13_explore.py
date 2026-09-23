# %% [markdown]
# # A13 — Resource monitoring: đo trước, chỉnh sau
#
# Chạy theo từng cell (`# %%`). Đủ cả 8 task ở đây.
#
# **Chỉ một thứ bắt buộc ra terminal:** `a13_resmon.py`. Nó là lớp bọc — nó phải
# là tiến trình CHA khởi động lệnh con rồi lấy mẫu cả cây, nên không nhét vào
# cell được. Task 1 và Task 3 dùng nó; cell tương ứng chỉ in lệnh ra để copy.
#
# Mấy cell còn lại đều `duckdb.connect()` in-memory, không đụng
# `warehouse.duckdb`, nên không vi phạm luật một-writer của A12.
#
# | Task | Ở đâu | Scale |
# |---|---|---|
# | 1 Dựng monitor | cell 2 in lệnh -> terminal | small |
# | 2 Phân tích CSV của monitor | cell 3, 4 | small |
# | 3 Bọc pipeline thật | cell 5 in lệnh -> terminal | full |
# | 4 Spill lab | cell 6 (+ watcher ở terminal 2) | full |
# | 4b Spill rơi đâu | cell 7 (assert) + cell 8 (số học) | small |
# | 5 Mép vực OOM | cell 9 | full |
# | 6 IO: CSV vs Parquet | cell 10 | full |
# | 7 Right-size | cell 11 | full |
# | 8 Quy ra tiền | cell 12 | - |
#
# Cell 6 và cell 11 ở full scale chạy vài phút và khoá notebook trong lúc đó.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Leo về repo root (mẫu A08/A09/A10/A12).
# ─────────────────────────────────────────────────────────────────────────────
import os
from pathlib import Path

p = Path.cwd()
while not (p / "lib" / "labpaths.py").exists():
    if p.parent == p:
        raise RuntimeError("Không tìm thấy repo root (lib/labpaths.py). "
                           "Mở VS Code ở thư mục ETL/ rồi chạy lại.")
    p = p.parent
os.chdir(p)
print("cwd:", Path.cwd())

import shutil                                                   # noqa: E402
import threading                                                # noqa: E402
import time                                                     # noqa: E402

import duckdb                                                   # noqa: E402
import psutil                                                   # noqa: E402

from work.a13_resource_monitoring.a13_bootstrap import (         # noqa: E402
    GB, LAKE, PROBE_DAY, RAW, SCALE, SWEEP, TMP, duck, os_drive, section)

print("SCALE =", SCALE, " (đổi bằng $env:ETL_LAB_SCALE rồi mở LẠI VS Code)")
print("tmp   =", TMP, "->", "TRỐNG" if not any(TMP.glob("*")) else "CÒN FILE")
print("ổ tmp =", TMP.drive, "vs ổ OS", os_drive())

# Schema era v1, đọc thô. Mấy cell hôm nay không làm sạch gì — mục tiêu là ép
# engine làm việc nặng, không phải dựng lại pipeline.
COLS_V1 = ("{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
           "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
           "'payment_method':'VARCHAR','order_total':'VARCHAR',"
           "'items':'VARCHAR','meta':'VARCHAR'}")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Task 1: smoke test resmon. TERMINAL (nó bọc một lệnh khác).
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
python -m work.a13_resource_monitoring.a13_resmon --out work/mon_test.csv -- python -c "import time; x = bytearray(300*2**20); time.sleep(3)"
""")
print("""Kỳ vọng: peak RSS ~0.30 GB (đúng cái bytearray), 6-7 mẫu, một dòng mỗi
nửa giây. nprocs = 1 ở đây là ĐÚNG — lệnh này không đẻ con; Task 3 mới thấy >1.

Đọc resmon.py trước khi chạy. Hai chỗ đáng đọc kỹ:
  - giữ ĐÚNG MỘT psutil.Process cho mỗi pid: cpu_percent(interval=None) đo phần
    CPU từ lần gọi trước TRÊN CÙNG đối tượng, nên đối tượng mới báo 0 mãi mãi.
  - cộng cả cây: tiến trình cha của pool A12 nằm ở ~0.2 GB trong khi các con ôm
    hàng gigabyte.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Task 2: dogfooding. Monitor nhả CSV, mà bạn sống bằng nghề đọc CSV.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 2 — tổng kết một lần chạy")
MON = "work/mon_test.csv"     # đổi sang mon_seq.csv / mon_par.csv sau Task 3
con = duckdb.connect()
print(con.sql(f"""
    SELECT round(max(rss) / pow(2,30), 2)        AS peak_rss_gb,
           round(avg(cpu_pct), 0)                AS avg_cpu_pct,
           round(max(read_bytes) / pow(2,30), 2) AS disk_read_gb,
           round(max(t_s), 1)                    AS wall_s,
           max(nprocs)                           AS max_procs
    FROM read_csv('{MON}', header=true)"""))
con.close()
# max(read_bytes) chứ không lấy giá trị cuối: lúc một worker thoát, bộ đếm IO
# của nó RỜI khỏi tổng, nên mẫu cuối cùng nhỏ hơn sự thật.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Task 2, hai query tự viết: đỉnh RSS theo cửa sổ 30s, và số giây
# sống trên 4 GB.
# ─────────────────────────────────────────────────────────────────────────────
con = duckdb.connect()
print(con.sql(f"""
    SELECT CAST(t_s / 30 AS INTEGER) * 30 AS bucket_s,
           round(max(rss) / pow(2,30), 2) AS peak_rss_gb,
           round(avg(cpu_pct), 0)         AS avg_cpu
    FROM read_csv('{MON}', header=true)
    GROUP BY 1 ORDER BY 1"""))
print(con.sql(f"""
    SELECT count(*) * 0.5 AS seconds_above_4gb
    FROM read_csv('{MON}', header=true)
    WHERE rss > 4 * pow(2,30)"""))
con.close()
# Mỗi dòng là một mẫu 0.5 giây, nên count(*) * 0.5 ra số giây. Đó là cả mẹo.
# Đạt Task 2 khi trả lời được: đỉnh RSS bao nhiêu, rơi vào cửa sổ 30s nào.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Task 3: bọc pipeline THẬT. TERMINAL, scale full.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
$env:ETL_LAB_SCALE="full"     # rồi mở LẠI terminal cho biến ăn vào

# một ngày qua pipeline A11 (--force để chạy lại ngày đã xanh):
python -m work.a13_resource_monitoring.a13_resmon --out work/mon_seq.csv -- python -m work.runner --scale full --start 2026-06-05 --end 2026-06-05 --force

# một tuần qua pool A12, cấu hình bạn đã chọn:
python -m work.a13_resource_monitoring.a13_resmon --out work/mon_par.csv -- python -m work.a12_parallelization.a12_parallel_runner --start 2026-06-01 --end 2026-06-07 --workers 4 --threads 2 --mem 3GB
""")
print("""Rồi quay lại cell 3, đổi MON, trả lời ba câu vào journal:
  1. peak RSS của lần chạy song song so với ngân sách TRÊN GIẤY của A12 — sát
     tới đâu? Đây là lúc tờ giấy gặp cái máy.
  2. trong 800% CPU có sẵn, mỗi lần chạy dùng bao nhiêu?
  3. lần chạy tuần tự nghẽn ở tính toán (CPU cao) hay ở IO (CPU thấp mà
     read_bytes cứ leo)? Cột nprocs chứng minh pool thật sự có con.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Task 4: SPILL LAB. Full scale, vài phút mỗi giới hạn.
#
# TRƯỚC KHI CHẠY, mở watcher ở terminal thứ hai (Ctrl+C để dừng):
#
#   while ($true) {
#     $f = Get-ChildItem "$env:ETL_LAB_DATA\tmp" -File -ErrorAction SilentlyContinue
#     $mb = (($f | Measure-Object Length -Sum).Sum) / 1MB
#     "{0:HH:mm:ss}  {1} files  {2:N0} MB" -f (Get-Date), $f.Count, $mb
#     Start-Sleep 1
#   }
#
# Cột MB của watcher đọc gần 0 ngay giữa lúc đang spill, và đó KHÔNG phải lỗi
# của bạn: NTFS chỉ cập nhật kích thước file trong danh sách thư mục khi tiến
# trình ghi đóng handle, mà DuckDB giữ file tạm mở tới khi query xong. Watcher
# chứng minh VÒNG ĐỜI (file hiện ra, sinh sôi, biến mất); KÍCH THƯỚC thì phải
# hỏi chính engine bằng duckdb_temporary_files().
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 4 — spill lab [{SCALE}]  sort toàn lịch sử theo (customer_id, order_ts)")
if SCALE != "full":
    print("CẢNH BÁO: small nhét vừa 8GB nên bảng sẽ phẳng lì và bạn đo được đúng")
    print("con số không. Đặt ETL_LAB_SCALE=full.\n")

results = []
for limit in ["8GB", "4GB", "1GB"]:
    con = duck(limit)          # connection MỚI mỗi lần: tái dùng sẽ đo lẫn cache cũ
    out = TMP / "sorted_orders.parquet"
    state = {"peak": 0}        # dict chứ không biến thường: closure ghi vào được
    done = threading.Event()

    def watch(con=con, state=state, done=done):
        poller = con.cursor()  # cursor THỨ HAI trên CÙNG database
        #   Cùng database nên nó thấy file tạm của query kia. Một
        #   duckdb.connect() mới thì không — đó là engine khác, bảng rỗng.
        while not done.is_set():
            try:
                rows = poller.execute(
                    "SELECT size FROM duckdb_temporary_files()").fetchall()
            except duckdb.Error:
                break
            state["peak"] = max(state["peak"], sum(s for (s,) in rows))
            time.sleep(0.5)

    t = threading.Thread(target=watch)
    t.start()
    t0, status = time.time(), "ok"
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{LAKE.as_posix()}/*/*.parquet',
                                             hive_partitioning=true)
                  ORDER BY customer_id, order_ts)
            TO '{out.as_posix()}' (FORMAT parquet)""")
    except duckdb.OutOfMemoryException:
        status = "OOM — giới hạn quá nhỏ cho phép toán này"
        #   KẾT QUẢ HỢP LỆ, không phải thất bại: có phép toán cần một lượng bộ
        #   nhớ tối thiểu mới khởi động nổi. Ghi lại nó xảy ra ở mức nào.
    finally:
        done.set()             # LUÔN dừng watcher, không thì cell treo mãi
        t.join()
        con.close()
        out.unlink(missing_ok=True)
    wall = time.time() - t0
    results.append((limit, wall, state["peak"], status))
    print(f"  {limit:>4}: {wall:6.1f}s   peak spill {state['peak'] / GB:5.2f} GB   {status}")

print(f"\n{'memory_limit':>12} | {'giây':>7} | {'peak spill':>11} | status")
for limit, wall, peak, status in results:
    print(f"{limit:>12} | {wall:7.1f} | {peak / GB:8.2f} GB | {status}")
print("""
Hình dạng cần thấy: giới hạn tụt thì spill tăng và thời gian tăng. Trong Task
Manager, tiến trình Python nằm PHẲNG ĐÚNG ở mức giới hạn chứ không phình — lời
hứa được giữ. Và mấy file duckdb_temp_storage_*.tmp biến mất ngay khi query
xong; chính chuyện đó khiến sự cố đầy ổ đĩa trở nên vô hình.

Ghi journal bảng: memory_limit | wall | peak spill | chạy xong chưa.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Task 4b(a): spill rơi xuống ĐÂU. Small scale — phép chứng minh này
# cần spill, không cần khối lượng.
#
# Hai thứ khiến đây là CHỨNG MINH chứ không phải liếc nhìn:
#   1. đường dẫn lấy từ chính DuckDB (duckdb_temporary_files), không phải từ
#      việc ta đoán nên đứng canh thư mục nào;
#   2. có ASSERT — mọi path dưới tmp/, và ổ tmp khác ổ hệ điều hành. Vế sau
#      chính là preflight check #3 của A07, nhìn thấy ngay lúc chạy.
# Đọc lại giá trị đã SET chỉ chứng minh bạn đã gõ nó vào. Assert chứng minh
# các byte thật sự rơi xuống đâu.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 4b(a) — spill rơi xuống đâu?  [small]")
LIMIT = "1GB"
#   Đủ nhỏ để sort tháng 6 buộc phải spill, đủ lớn để 8 thread khởi động nổi.
#   Dưới ~1GB thì bộ đọc CSV 8 thread còn không bắt đầu được — đúng cái "lượng
#   bộ nhớ tối thiểu" của Task 4, gặp lại lần nữa. Gặp OOM sạch thì nâng một nấc.

TMP.mkdir(parents=True, exist_ok=True)
out = TMP / "spill_where_probe.parquet"
con = duck(LIMIT)
seen: set = set()
state = {"peak": 0}
done = threading.Event()


def watch_paths(con=con, seen=seen, state=state, done=done):
    poller = con.cursor()
    while not done.is_set():
        try:
            rows = poller.execute(
                "SELECT path, size FROM duckdb_temporary_files()").fetchall()
        except duckdb.Error:
            break
        seen.update(Path(pp) for pp, _ in rows)
        state["peak"] = max(state["peak"], sum(s for _, s in rows))
        time.sleep(0.2)
        #   Poll dày hơn cell 6 (0.2 vs 0.5): ở đây cần BẮT ĐƯỢC path, mà file
        #   tạm của một lần sort nhỏ có thể chỉ sống vài giây.


t = threading.Thread(target=watch_paths)
t.start()
status = "ok"
try:
    con.execute(f"""
        COPY (SELECT * FROM read_csv('{(RAW / "orders_2026-06-*.csv").as_posix()}',
                                     header=true, columns={COLS_V1})
              ORDER BY customer_id, order_ts)
        TO '{out.as_posix()}' (FORMAT parquet)""")
except duckdb.OutOfMemoryException:
    status = f"clean OOM ở {LIMIT}"
finally:
    done.set()
    t.join()
    con.close()
    out.unlink(missing_ok=True)

assert seen, f"không quan sát được spill nào ({status}) — nâng LIMIT rồi chạy lại"
outside = sorted(str(x) for x in seen if not x.is_relative_to(TMP))
assert not outside, f"FAIL: spill nằm NGOÀI tmp/: {outside}"
osd = os_drive()
verdict = "PASS" if TMP.drive.upper() != osd else "WARN — tmp/ nằm trên ổ hệ điều hành!"
print(f"query status     : {status}")
print(f"file spill thấy  : {len(seen)}  (đỉnh {state['peak'] / GB:.2f} GB trên đĩa cùng lúc)")
print(f"tất cả đều dưới  : {TMP}")
print(f"ổ đĩa            : {TMP.drive}  vs ổ OS {osd}  -> {verdict}")
print("\nĐể ý tỷ lệ: ~0.27 GB spill cho ~0.5 GB CSV tháng 6. Ngay cả phép sort bé")
print("tí này cũng sống một phần ba trên đĩa.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Task 4b(b): bán kính ảnh hưởng. CHỈ LÀ SỐ HỌC.
#
# TUYỆT ĐỐI đừng trỏ temp_directory vào C: để "xem thử cho thật" — làm đầy ổ hệ
# điều hành đúng là sự cố bài này đang học cách phòng, và nó kéo sập nhiều hơn
# cái thí nghiệm của bạn.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 4b(b) — nếu spill rơi xuống ổ C: thì sao?")
SPILL_GB = 6.0        # <-- đỉnh spill ĐO ĐƯỢC ở cell 6, lần chạy giới hạn 1GB

free_gb = shutil.disk_usage(os_drive() + "\\").free / GB
print(f"{os_drive()} còn trống {free_gb:.1f} GB")
print(f"mỗi lần chạy spill {SPILL_GB} GB")
print(f"-> {int(free_gb // SPILL_GB)} lần chạy ĐỒNG THỜI là lấp đầy ổ hệ điều hành")
print()
print("Giờ tính lại cho hình dáng máy được cấp phát ngoài thực tế: ổ OS 40 GB,")
print(f"còn trống 15 GB -> {int(15 // SPILL_GB)} lần chạy.")
print("Điểm ngọt của bạn ở A12 chạy 4 worker cùng lúc. Nếu mỗi worker spill như")
print("phép sort đó, MỘT phiên backfill song song là đủ giết con máy.")
print("""
Toàn bộ lý do của nếp đặt thư mục tạm, gói trong một phép chia.

Hai câu khép lại cho journal:
  1. spill sống ở đâu trong lab này và cái gì bảo đảm điều đó — khối quy ước
     CỘNG preflight check #3 trước mỗi lần nạp. Quy ước là lời hứa, cái check
     mới là phần thi hành.
  2. vì sao chỗ tạm MẶC ĐỊNH của DuckDB (cạnh file database, hoặc thư mục làm
     việc) tình cờ an toàn ở đây nhưng là cái bẫy trên một máy mà code — hay
     chỉ là thư mục hiện tại của cái shell — nằm trên ổ hệ điều hành.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Task 5: mép vực OOM, CÓ BUỘC DÂY. Full scale.
# ĐÓNG app nặng, LƯU việc lại. ĐỪNG BAO GIỜ bỏ guard đi — với RAM đầy, Windows
# paging và mọi thứ bò như rùa, kể cả editor đang giữ journal chưa lưu của bạn.
# ─────────────────────────────────────────────────────────────────────────────
import pandas as pd                                             # noqa: E402

GUARD_RSS_GB = 12        # dừng trước khi Windows paging chính chúng ta tới chết
GUARD_AVAIL_GB = 2       # ...hoặc trước khi CẢ HỆ THỐNG hết chỗ, cái nào tới trước
#   HAI ngưỡng, trả lời hai câu khác nhau. Cái đầu: "tôi có ăn quá phần mình
#   không". Cái sau: "cả cái máy còn thở không" — và nó bắt được cả trường hợp
#   một tiến trình KHÁC đang ăn RAM trong lúc ta chạy.

section(f"Task 5 — mép vực OOM [{SCALE}]  guard: RSS>{GUARD_RSS_GB}GB "
        f"hoặc available<{GUARD_AVAIL_GB}GB")
proc = psutil.Process()
frames, tripped = [], None
for i, path in enumerate(sorted(RAW.glob("orders_2026-06-*.csv")), 1):
    frames.append(pd.read_csv(path))
    #   append chứ không concat mỗi vòng: concat COPY cả đống cũ mỗi lần, và
    #   bạn sẽ đo tốc độ copy chứ không đo chi phí lưu.
    rss = proc.memory_info().rss / GB
    avail = psutil.virtual_memory().available / GB
    print(f"sau {i:2d} ngày: RSS {rss:5.1f} GB   hệ thống còn {avail:5.1f} GB   "
          f"({rss / i:.2f} GB/ngày)")
    if rss > GUARD_RSS_GB or avail < GUARD_AVAIL_GB:
        tripped = "RSS" if rss > GUARD_RSS_GB else "available"
        print(f"\nGUARD CHẠM ({tripped}) sau {i} ngày — dừng trước khi Windows dừng ta.")
        break

per_day = proc.memory_info().rss / GB / max(len(frames), 1)
print(f"\n{per_day:.2f} GB mỗi ngày x 69 ngày = {per_day * 69:.0f} GB.")
print("Đó là lý do 'cứ nạp hết bằng pandas' chưa bao giờ là một phương án.")
if tripped is None:
    print("Guard không chạm trong 30 ngày — ghi CẢ điều đó. Pandas đời mới lưu")
    print("chuỗi gọn hơn nhiều, nên GB/ngày của BẠN mới là phép đo.")
print("\nĐối chiếu cell 6: DuckDB sort TRỌN 69 ngày trong 1 GB. Khác biệt không")
print("nằm ở ngôn ngữ — một bên có đường out-of-core, bên kia không có khái niệm")
print("ngân sách nên thứ duy nhất chặn được nó là hệ điều hành.")
del frames    # trả RAM lại trước khi chạy cell sau

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — Task 6: IO. Cùng một câu hỏi, hai định dạng, đếm BYTE ĐỌC.
# (Số dòng hai bên lệch chút đỉnh vì late row — A02/A08. Hôm nay so byte.)
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 6 — IO: CSV vs Parquet [{SCALE}]  (tháng 6, đếm đơn theo cửa hàng)")
proc = psutil.Process()
con = duck("8GB")
#   io_counters của TIẾN TRÌNH NÀY. DuckDB chạy bên trong process Python nên
#   mọi lần đọc của nó đều tính vào đây — không cần lớp bọc resmon.

before = proc.io_counters().read_bytes
con.execute(f"""
    SELECT store_id, count(*) AS orders
    FROM read_csv('{(RAW / "orders_2026-06-*.csv").as_posix()}',
                  header=true, columns={COLS_V1})
    GROUP BY 1""").fetchall()
csv_gb = (proc.io_counters().read_bytes - before) / GB

before = proc.io_counters().read_bytes
con.execute(f"""
    SELECT store_id, count(*) AS orders
    FROM read_parquet('{LAKE.as_posix()}/*/*.parquet', hive_partitioning=true)
    WHERE order_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
    GROUP BY 1""").fetchall()
pq_gb = (proc.io_counters().read_bytes - before) / GB
con.close()

print(f"CSV    : đọc {csv_gb:6.2f} GB")
print(f"Parquet: đọc {pq_gb:6.2f} GB")
print(f"tỷ lệ  : ít hơn {csv_gb / max(pq_gb, 1e-9):.0f} lần IO")
print("""
Một câu journal, dùng đúng ba chữ: CỘT (chỉ đọc store_id chứ không đọc items
với meta), NÉN (một cột store_id lặp lại nén gần như bằng không), PHÂN VÙNG
(order_date nằm trong tên thư mục nên tháng 7, 8 không bị mở ra).

Bẫy khi so A/B: Windows có cache đọc file. Lần quét thứ hai trên cùng file CSV
có thể nhanh hơn hẳn trong khi read_bytes y hệt. Chạy mỗi phương án hai lần rồi
so lần-đầu với lần-đầu (A14 nói kỹ chuyện này).""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — Task 7: right-size. Full scale, vài phút.
#
# Câu hỏi thật của production: memory_limit NHỎ NHẤT là bao nhiêu mà lần nạp
# hằng ngày vẫn chạy trong vòng chưa tới GẤP ĐÔI thời gian tốt nhất? Chữ nhỏ
# nhất mới quan trọng: trên đám mây, RAM chính là cái bảng giá.
#
# Tái dùng ALIGN/CANON của A10 chứ không chép SQL sang — chép là mai sửa một
# cleaner phải nhớ có hai bản. Ghi ra tmp/rightsize/ chứ KHÔNG ghi vào lake:
# lake dùng COPY APPEND, không idempotent, chạy tám vòng là nhân tám (A11).
# ─────────────────────────────────────────────────────────────────────────────
from work.a10_schema_evolution.a10_canonical import ALIGN, CANON, era_of   # noqa: E402
from work.a12_parallelization.a12_stage_sql import COLS                    # noqa: E402

day, out_dir = PROBE_DAY, (TMP / "rightsize")
era = era_of(day)
section(f"Task 7 — right-size [{SCALE}]  ngày {day} (era {era})")
if SCALE != "full":
    print("CẢNH BÁO: small một ngày chỉ ~60k dòng, nhét vừa cả 64MB, bảng sẽ phẳng")
    print("và bạn không thấy đầu gối. Đặt ETL_LAB_SCALE=full.\n")

read = (f"read_csv('{(RAW / f'orders_{day}.csv').as_posix()}', header=true, "
        f"columns={COLS[era]}"
        + (", store_rejects=true" if day == "2026-07-22" else "") + ")")
sql = f"""
    COPY (
        WITH aligned AS ({ALIGN[era].replace("FROM src", f"FROM {read}")}),
             canon AS ({CANON})
        SELECT *, DATE '{day}' AS _data_date FROM canon
    ) TO '{out_dir.as_posix()}' (FORMAT parquet, PARTITION_BY (order_date),
                                 OVERWRITE_OR_IGNORE, ROW_GROUP_SIZE 122880)"""

rows = []
for limit in SWEEP:
    shutil.rmtree(out_dir, ignore_errors=True)
    #   Xoá trước MỖI vòng: OVERWRITE_OR_IGNORE gặp thư mục cũ sẽ làm việc khác
    #   đi, và bạn đo nhầm hai thứ mà tưởng là một.
    con = duck(limit)
    t0, status = time.perf_counter(), "ok"
    try:
        con.execute(sql)
    except duckdb.OutOfMemoryException:
        status = "OOM"
    except duckdb.Error as e:
        status = type(e).__name__[:20]
    wall = time.perf_counter() - t0
    con.close()
    rows.append((limit, wall, status))
    print(f"  {limit:>6}  {wall:6.1f}s  {status}")
shutil.rmtree(out_dir, ignore_errors=True)

ok = [(l, w) for l, w, s in rows if s == "ok"]
if ok:
    best = min(w for _, w in ok)
    print(f"\n{'memory_limit':>12} | {'giây':>7} | {'vs best':>8} | status")
    for limit, wall, status in rows:
        ratio = f"{wall / best:.1f}x" if status == "ok" else "-"
        print(f"{limit:>12} | {wall:7.1f} | {ratio:>8} | {status}")
    answer = next((l for l, w in reversed(ok) if w / best < 2.0), None)
    #   reversed: SWEEP đi từ lớn xuống nhỏ, duyệt ngược là đi từ NHỎ lên, nên
    #   cái đầu tiên thoả điều kiện chính là cái nhỏ nhất.
    print(f"\nĐáp án: {answer} — giới hạn nhỏ nhất vẫn dưới 2.0x thời gian tốt "
          f"nhất ({best:.1f}s).")
else:
    print("\nKhông giới hạn nào chạy nổi. Nâng đầu trên của SWEEP lên.")
print("""
Hình dạng: phẳng, phẳng, rồi một ĐẦU GỐI chỗ bắt đầu spill, rồi có thể là cái
sàn OOM sạch. Nếu mọi giới hạn cho cùng một thời gian thì khối lượng công việc
chưa bao giờ vượt giới hạn nhỏ nhất — bạn đo được đúng con số không.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — Task 8: quy ra tiền. Giá chỉ là ước lượng; CÁI THÓI QUEN mới là
# thứ phải giao nộp.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 8 — tiền")
PRICE_HR, RUNS_YEAR = 0.40, 365     # VM 8 core / 16 GB on-demand, giá đứng thay
A11_WALL_S = 3600.0                 # <-- wall clock backfill A11, từ journal
A12_WALL_S = 900.0                  # <-- wall clock (stage + commit) A12
FITS_GB = 4.0                       # <-- đáp án cell 11

saved_hr = (A11_WALL_S - A12_WALL_S) / 3600
print(f"(1) TỐC ĐỘ: {A11_WALL_S / 3600:.2f}h -> {A12_WALL_S / 3600:.2f}h "
      f"= tiết kiệm {saved_hr:.2f}h mỗi lần chạy")
print(f"    {saved_hr:.2f}h x ${PRICE_HR} x {RUNS_YEAR} lần = "
      f"${saved_hr * PRICE_HR * RUNS_YEAR:,.0f}/năm, chỉ riêng từ A12")

daily_hr = A12_WALL_S / 3600
full_year = daily_hr * PRICE_HR * RUNS_YEAR
half_year = daily_hr * (PRICE_HR / 2) * RUNS_YEAR
print(f"\n(2) CỠ MÁY: lần nạp hằng ngày nhét vừa ~{FITS_GB} GB -> tụt xuống bậc VM")
print(f"    nửa cỡ, giá còn ~một nửa: ${full_year:,.0f}/năm -> ${half_year:,.0f}/năm")
print(f"    tiết kiệm thêm ${full_year - half_year:,.0f}/năm")
print("\nGhi journal cả hai con số KÈM phép tính. Giá là ước lượng, còn cái thói")
print("quen ngồi làm phép tính này mới là thứ phải giao nộp.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 13 — kiểm tra sau cùng: tmp/ phải trống trở lại.
# ─────────────────────────────────────────────────────────────────────────────
leftover = sorted(TMP.glob("*"))
print("tmp/:", len(leftover), "mục")
for f in leftover[:10]:
    print("  ", f.name)
print("""
Sau mỗi lần chạy, mấy file duckdb_temp_storage_*.tmp phải biến mất trong vài
giây. Còn sót nghĩa là có query chết mà chưa dọn, hoặc còn tiến trình đang giữ.
Chính chuyện chúng biến mất nhanh như vậy là lý do sự cố đầy ổ đĩa lại vô hình
trong mọi lần chạy thử nhỏ — và là lý do cell 7 phải ASSERT thay vì liếc nhìn.""")
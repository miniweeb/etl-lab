# %% [markdown]
# # A12 — Parallelization: 69 ngày trên 8 core
#
# Chạy theo từng cell (`# %%`). File này là **sổ đo**, không phải chỗ chạy pool:
# pool phải chạy ở **terminal** vì Windows spawn re-import file, và một notebook
# cell không có `__main__` guard theo nghĩa process con hiểu được.
#
# | Task | Chạy ở đâu |
# |---|---|
# | 1 | cell 2 (bảng ngân sách) + journal |
# | 2 | hai terminal, cell 3 chỉ để đối chiếu |
# | 3 | `a12_parallel_runner.py` ở terminal |
# | 4 | `a12_commit_stage.py` ở terminal, rồi cell 5 verify |
# | 5, 6, 8, 9 | terminal + cell 6 ghi bảng |
# | 7 | `a12_single_query.py` ở terminal |
#
# **Luật của cả bài:** notebook này KHÔNG giữ connection tới warehouse.duckdb.
# Cell verify mở → đọc → đóng ngay trong cùng cell. Quên `con.close()` là
# committer của bạn chết bằng IOException ở lần chạy sau.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Leo về repo root (mẫu A08/A09/A10).
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

import duckdb, time                                    # noqa: E402
from work.a12_parallelization.a12_bootstrap import (   # noqa: E402
    SCALE, STAGE, MANIFEST, RAW, SPIKE_DAYS, legal, manifest, section, daterange)
print("SCALE =", SCALE, " (đổi bằng $env:ETL_LAB_SCALE trước khi mở VS Code)")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Task 1: bảng ngân sách.
# Làm bằng tay trên giấy TRƯỚC, rồi mới chạy cell này để đối chiếu.
# Hai dòng cuối phải giải thích được vi phạm ràng buộc NÀO — chúng khác nhau:
# một cái đốt core vào context switch, một cái hứa RAM không tồn tại.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 1 — budget")
for w, t, m in [(1, 8, 8), (2, 4, 5), (4, 2, 3), (8, 1, 1.5), (8, 8, 1.5), (8, 1, 12.8)]:
    ok, why = legal(w, t, m)
    print(f"{w}x{t:<2} mem={m:>5}GB  threads={w*t:<3} RAM={w*m:>6.1f}GB  "
          f"{'yes' if ok else 'NO'}   {why}")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Task 2: single-writer. CHẠY Ở TERMINAL, không ở đây.
# In ra để copy. Dán CẢ HAI lỗi (bản thường và bản read_only) vào journal.
# ─────────────────────────────────────────────────────────────────────────────
from lib.labpaths import data_root   # noqa: E402
wh = (data_root(SCALE) / "warehouse" / "warehouse.duckdb").as_posix()
print(f"""# Terminal 1 — giữ file 60s:
python -c "import duckdb,time; duckdb.connect(r'{wh}'); time.sleep(60)"

# Terminal 2 — trong lúc T1 còn ngủ:
python -c "import duckdb; duckdb.connect(r'{wh}')"
python -c "import duckdb; duckdb.connect(r'{wh}', read_only=True)"
""")
print("read_only CŨNG hỏng. Đó mới là phần đáng nhớ: không phải 'một writer và "
      "nhiều reader' — mà là 'writer giữ file thì không ai vào'.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Task 3: lệnh chạy pool. Chạy Ở TERMINAL.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
# 3.1 month 1, chạy thử:
python -m work.a12_parallelization.a12_parallel_runner --end 2026-07-15 --workers 4 --threads 2 --mem 3GB

# 3.2 cả 69 ngày — vì stage_sql đã era-aware nên KHÔNG có FAIL nào.
#     (Đề gốc bảo bạn chạy bản v1-only để thấy v2/v3 FAIL trước. Muốn thấy cảnh
#      đó thì tạm ép era='v1' trong a12_stage_sql rồi chạy. Đáng xem một lần.)
python -m work.a12_parallelization.a12_parallel_runner --clean --workers 4 --threads 2 --mem 3GB
""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Verify sau Task 3 + Task 4.
# ─────────────────────────────────────────────────────────────────────────────
section("verify stage")
con = duckdb.connect()
print("file parquet:", len(list(STAGE.glob("orders_*.parquet"))), "(expect 69)")
staged, = con.execute(
    f"SELECT count(*) FROM read_parquet('{STAGE.as_posix()}/*.parquet')").fetchone()
claimed, expected_unique = con.execute(f"""
    SELECT sum(rows), sum(rows) - sum(late_rows) - sum(dup_rows)
    FROM read_json_auto('{(MANIFEST / "orders_*.json").as_posix()}')""").fetchone()
print(f"staged          : {staged:,}   (small: 4,116,024)")
print(f"manifest rows   : {claimed:,}   (small: 4,116,840)")
print(f"dropped         : {claimed - staged:,}   (small: 816)")
print(f"expected_unique : {expected_unique:,}   (small: 4,054,002)")
con.close()
# Chẩn đoán khi lệch:
#   staged == 4,116,840  -> SELECT của bạn mất WHERE của A10, giữ cả dòng ts hỏng.
#   dedupe > 4,054,002   -> order_id chưa chuẩn hoá: correction v3 'ORD-...'
#                           không khớp bản gốc v1 dạng số. Quay lại A10.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Task 5/8/9: bảng thực nghiệm. Điền tay sau mỗi lần chạy terminal.
# ─────────────────────────────────────────────────────────────────────────────
runs = [
    # (scale, workers, threads, mem, order, wall_s, peak_gb)
    ("small", 1, 8, "8GB",    "date", None, None),
    ("small", 2, 4, "5GB",    "date", None, None),
    ("small", 4, 2, "3GB",    "date", None, None),
    ("small", 8, 1, "1500MB", "date", None, None),
]
base = next((r[5] for r in runs if r[1] == 1), None)
print(f"{'scale':6}{'cfg':10}{'mem':9}{'order':7}{'wall':>8}{'peak':>8}{'speedup':>9}")
for s, w, t, m, o, wall, peak in runs:
    sp = f"{base / wall:.2f}x" if (base and wall) else "-"
    print(f"{s:6}{f'{w}x{t}':10}{m:9}{o:7}"
          f"{(f'{wall:.1f}s' if wall else '-'):>8}"
          f"{(f'{peak:.1f}G' if peak else '-'):>8}{sp:>9}")
print("\nBaseline TRUNG THỰC là 1x8, không phải 1x1: A11 đã dùng cả 8 core "
      "bên trong mỗi query rồi. So với 1x1 là tự tâng bốc.")
print("Task 9: (stage + commit) so với wall-clock A11 trong journal. Đích >= 3x.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Task 8: skew. Xem hai ngày spike to cỡ nào.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 8 — skew")
days = list(daterange("2026-06-01", "2026-08-08"))
sizes = {d: manifest(d)["bytes"] / 2**20 for d in days}
med = sorted(sizes.values())[len(sizes) // 2]
for d in SPIKE_DAYS:
    print(f"{d}: {sizes[d]:.0f} MB  = {sizes[d]/med:.1f}x ngày trung vị ({med:.0f} MB)")
print("\nTrong pool, ngày chậm nhất định ra vạch đích. Một ngày 3x xếp CUỐI để "
      "7 core ngồi không chờ nó. --order size đẩy nó lên đầu, ngày nhỏ lấp khe.")
print("Chạy hai lần cùng cấu hình, chỉ khác --order date|size, ghi cả hai wall.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Task 6: hai lần phá hoại có kiểm soát. CHỈ MỘT TUẦN, scale full.
# Đóng app khác, lưu việc, mở Task Manager, tay để sẵn Ctrl+C.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
# 6.1 oversubscription — 64 thread trên 8 core
python -m work.a12_parallelization.a12_parallel_runner --start 2026-06-15 --end 2026-06-21 --workers 8 --threads 8 --mem 1500MB

# 6.2 overcommit — quên memory_limit trông như thế này
python -m work.a12_parallelization.a12_parallel_runner --start 2026-06-15 --end 2026-06-21 --workers 8 --threads 1 --mem 12GB
# ABORT bằng Ctrl+C nếu RAM qua ~90% và máy đờ ra.
""")
print("Nếu 6.2 chạy êm: ghi CẢ điều đó. Một tuần có thể quá nhỏ để chạm trần. "
      "Bài học không đổi — KHÔNG AI kiểm tổng; lời hứa là ~96GB trên máy 16GB.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — đóng mọi thứ trước khi ra terminal.
# ─────────────────────────────────────────────────────────────────────────────
try:
    con.close()
except Exception:
    pass
print("closed. Giờ chạy được a12_commit_stage.py.")
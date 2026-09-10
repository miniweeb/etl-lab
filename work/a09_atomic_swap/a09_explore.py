# %% [markdown]
# # A09 — Atomic partition swap
#
# Chạy file này theo từng cell trong VS Code (`# %%`). Đừng chạy cả file một lượt.
#
# **File này KHÔNG chứa toàn bộ bài.** Phần lớn A09 chạy ở terminal, vì bài học
# chỉ hiện ra khi có hai process cùng lúc: một bên đọc, một bên ghi. Một kernel
# Jupyter không mô phỏng được chuyện đó.
#
# | Task | Chạy ở đâu |
# |---|---|
# | 1 | terminal + cell 2 dưới đây |
# | 2, 3, 5, 6, 7 | hai terminal, dùng bốn script `.py` cùng thư mục |
# | 4 | cell 3 dưới đây (PowerShell + đọc) |
# | 8 | cell 4 |
# | 9 | cell 5, 6, 7 — có bước dọn dẹp BẮT BUỘC |
#
# Warehouse: DuckDB chỉ cho MỘT process ghi. Cell 4 và 5 mở
# `warehouse.duckdb`. Nếu sau đó bạn định chạy script gì ở terminal có đụng
# warehouse thì phải `con.close()` trước.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP. Chạy lại cell này mỗi khi kernel mất biến (NameError).
# ─────────────────────────────────────────────────────────────────────────────
import os
from pathlib import Path

# Leo ngược tìm repo root. Có chặn: tới gốc ổ đĩa thì DỪNG và báo lỗi,
# thay vì os.chdir("..") mãi mãi ở D:\ mà không ai biết.
here = Path.cwd()
for cand in [here, *here.parents]:
    if (cand / "lib").is_dir():
        os.chdir(cand)
        break
else:
    raise RuntimeError(f"không tìm thấy repo root (thư mục có lib/) từ {here}")
print("cwd:", os.getcwd())

import shutil
import sys
import time

import duckdb

sys.path.insert(0, "work/a09_atomic_swap")   # để `from a09_common import ...` chạy được
from a09_common import DAY, rebuild_sql
from lib.labpaths import lake_dir, manifest_dir, warehouse_path

SCALE = "small"              # Task 7 mới đổi sang "full", và chỉ cho script terminal
LAKE = lake_dir(SCALE)
PART = LAKE / "orders" / f"order_date={DAY}"

print("scale:", SCALE, "| victim partition:", PART)

# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Biết trạng thái TRƯỚC khi đụng vào bất cứ thứ gì.
# ─────────────────────────────────────────────────────────────────────────────
#
# Hai con số, hai nguồn khác nhau, và chúng KHÔNG bằng nhau — chỗ chênh mới là
# thứ đáng hiểu:
#
#   manifest.rows   = số dòng shopcore giao trong FILE orders_2026-06-19.csv
#   count partition = số dòng đang nằm trong PARTITION order_date=2026-06-19
#
# File 06-19 chứa cả bản sửa cho các ngày trước đó, và những dòng ấy đã được
# loader A02 xếp vào partition cũ hơn. Nên partition nhỏ hơn manifest.
# Đây chính là sợi chỉ "một file không bằng một ngày" từ A00.

man = (manifest_dir(SCALE) / f"orders_{DAY}.json").as_posix()
con = duckdb.connect()      # in-memory, không đụng warehouse
print(con.sql(f"SELECT rows, late_rows, dup_rows FROM read_json_auto('{man}')"))
# Kỳ vọng small: 182151, 2732, 91   (full: 3643024, 54645, 1821)
# Generator seed theo ngày nên deterministic. Lệch là có chuyện.

glob = f"{(LAKE / 'orders').as_posix()}/*/*.parquet"
n_before = con.execute(
    f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true) "
    f"WHERE order_date = DATE '{DAY}'").fetchone()[0]
nfiles = len(list(PART.glob("*.parquet")))
print(f"before: {n_before} rows in {nfiles} files")
# Kỳ vọng small: 180183 dòng trong 8 file.
# 8 file = 1 file to (nạp ngày 06-19) + 7 file nhỏ do các ngày 06-20..06-26 gửi
# bản sửa về. Nếu bạn chỉ thấy 1 file, loader A02 của bạn đã dùng
# OVERWRITE_OR_IGNORE và ăn mất các file late-correction.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Vì sao staging phải khuất: thư mục tmp rò rỉ.
# ─────────────────────────────────────────────────────────────────────────────
#
# Trước khi viết bản sửa, hãy nhìn cái sai kinh điển: để staging NGAY BÊN TRONG
# cây thư mục live, ví dụ order_date=2026-06-19.__tmp__
#
# Chạy ba lệnh này ở TERMINAL (PowerShell), không phải ở đây:
#
#   $lake = "$env:ETL_LAB_DATA\small\lake\orders"
#   Copy-Item -Recurse "$lake\order_date=2026-06-19" "$lake\order_date=2026-06-19.__tmp__"
#   python work\a09_atomic_swap\a09_reader.py small 5
#   Remove-Item -Recurse -Force "$lake\order_date=2026-06-19.__tmp__"
#
# Count sẽ TĂNG GẤP ĐÔI. Hai thứ hỏng cùng lúc:
#   1. glob */*.parquet của người đọc khớp luôn thư mục __tmp__
#   2. filter order_date = DATE '2026-06-19' VẪN khớp, vì phép cast DATE của
#      DuckDB bỏ qua phần đuôi thừa
#
# Chạy cell này để tự nhìn thấy điểm 2 — đây mới là chỗ bất ngờ:

print(con.sql("SELECT CAST('2026-06-19.__tmp__' AS DATE) AS parsed_anyway"))
# → 2026-06-19. Không cảnh báo, không lỗi. Cái đuôi bị nuốt im lặng.
#
# Thư mục có dấu chấm đầu (.tmp...) đặt bên trong cây còn tệ hơn: glob của
# DuckDB vẫn khớp, và tên thư mục không đúng dạng key=value sẽ làm MỌI người
# đọc chết với lỗi Hive partition mismatch.
#
# Kết luận: staging phải nằm nơi glob không với tới — anh em của orders/, không
# phải con của orders/. Đó đúng là chỗ a09_swap.py đặt .staging.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 8 — Table swap trong warehouse.
# ⚠️ Cell này có CREATE OR REPLACE TABLE và DROP TABLE. Đọc trước khi chạy.
# ⚠️ Nó mở warehouse.duckdb bằng HAI connection. Đảm bảo không có script nào
#    khác đang giữ file, nếu không sẽ gặp IOException.
# ─────────────────────────────────────────────────────────────────────────────
#
# Trong warehouse thì không cần mẹo rename file: catalog cho bạn atomic thật.
# Dựng bảng thay thế, rồi đổi tên cả hai bảng trong CÙNG một transaction.

writer = duckdb.connect(str(warehouse_path("small")))
writer.execute("SET memory_limit='8GB'; SET threads=8;")
reader = duckdb.connect(str(warehouse_path("small")))    # connection thứ hai = analyst

lake_glob = (lake_dir("small") / "orders").as_posix() + "/*/*.parquet"
writer.execute(f"""
CREATE OR REPLACE TABLE core.orders_a09 AS
SELECT * FROM read_parquet('{lake_glob}', hive_partitioning=true)
""")
writer.execute("""
CREATE OR REPLACE TABLE core.orders_a09_new AS
SELECT * FROM core.orders_a09 WHERE order_id % 1000 != 0   -- khác đi cho dễ thấy
""")

writer.execute("BEGIN TRANSACTION")
writer.execute("ALTER TABLE core.orders_a09 RENAME TO orders_a09_old")
writer.execute("ALTER TABLE core.orders_a09_new RENAME TO orders_a09")
print("mid-swap  :", reader.execute("SELECT count(*) FROM core.orders_a09").fetchone())
writer.execute("COMMIT")
print("post-commit:", reader.execute("SELECT count(*) FROM core.orders_a09").fetchone())
writer.execute("DROP TABLE core.orders_a09_old")

# Giữa transaction, connection người đọc VẪN thấy bảng CŨ, đầy đủ và đúng. Ngay
# sau COMMIT nó thấy bảng mới. Hai lần rename hiện ra cùng lúc — đó là thứ
# "atomic" mua được khi có catalog quản lý tên.
#
# Hai bảng orders_a09 này là đạo cụ, đọc thẳng từ lake nên KHÔNG mang cặp
# lineage §7.2: file lake không chứa _data_date, và ở đây cũng không có lần chạy
# nào được đăng ký để phát _run_id. Bảng core.orders thật của A07 thì có đủ cả hai.
#
# Hai giới hạn phải nhớ:
#   - Cái này bảo vệ các CONNECTION tới warehouse. Một PROCESS khác thậm chí
#     không mở nổi file khi writer đang giữ — luật một-process-ghi, A12 sẽ đụng thẳng.
#   - Nó không giúp gì cho người đọc Parquet. File nằm ngoài catalog.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 9a — Versioned dirs: biến lake hiện tại thành version 1, trỏ view vào đó.
# ⚠️ Cell này ĐỔI TÊN thư mục orders/ thật. Cell 7 sẽ trả nó về. ĐỪNG bỏ qua cell 7.
# ─────────────────────────────────────────────────────────────────────────────
#
# Kiểu thứ ba không sửa thư mục đã publish, một chữ cũng không. Mỗi lần ghi lại
# thì publish nguyên một thư mục version mới, rồi trỏ view sang bản mới nhất.

root = lake_dir("small")
v1 = int(time.time())
(root / "orders").rename(root / f"orders_v={v1}")        # tức thì, cùng ổ đĩa
writer.execute(f"""
CREATE OR REPLACE VIEW core.lake_orders AS
SELECT * FROM read_parquet('{(root / f"orders_v={v1}").as_posix()}/*/*.parquet',
                           hive_partitioning=true)
""")
print("v1 =", v1)
print(writer.sql("SELECT count(*) FROM core.lake_orders WHERE order_date = DATE '2026-06-19'"))


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 9b — Publish version 2, rồi trỏ view sang.
# ⚠️ copytree nhân đôi cả lake small. Vài trăm MB. Kiểm tra chỗ trống ổ D trước.
# ─────────────────────────────────────────────────────────────────────────────

v2 = v1 + 1
t0 = time.time()
shutil.copytree(root / f"orders_v={v1}", root / f"orders_v={v2}")
print(f"copytree mất {time.time() - t0:.1f}s — nhớ con số này")

# Dựng lại partition 2026-06-19 BÊN TRONG orders_v=<v2>.
#
# Xoá tại chỗ ở ĐÂY là an toàn, khác hẳn Task 3 — chưa ai đọc orders_v=<v2>,
# view vẫn đang trỏ vào v1. Cả thư mục version mới đóng vai trò staging.
part2 = root / f"orders_v={v2}" / f"order_date={DAY}"
n_old = len(list(part2.glob("*.parquet")))
for f in part2.glob("*.parquet"):
    f.unlink()

t1 = time.time()
con2 = duckdb.connect()
con2.execute("SET memory_limit='8GB'; SET threads=8;")
con2.execute(f"COPY ({rebuild_sql('small')}) "
             f"TO '{(part2 / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
con2.close()
print(f"rebuild trong v2: {n_old} file -> 1 file, mất {time.time() - t1:.1f}s")

writer.execute(f"""
CREATE OR REPLACE VIEW core.lake_orders AS
SELECT * FROM read_parquet('{(root / f"orders_v={v2}").as_posix()}/*/*.parquet',
                           hive_partitioning=true)
""")
print(writer.sql("SELECT count(*) FROM core.lake_orders WHERE order_date = DATE '2026-06-19'"))

# Từ giờ consumer query core.lake_orders chứ không bao giờ query một đường dẫn
# thư mục. CREATE OR REPLACE VIEW là thay đổi catalog — atomic, giống Task 8.
#
# Version cũ vẫn nằm nguyên trên đĩa. Muốn rollback thì trỏ view về v1, không
# phải khôi phục dữ liệu từ đâu cả. Và câu hỏi "hôm qua dữ liệu trông thế nào"
# trả lời được bằng cách đọc thẳng orders_v=<v1>.
#
# Cái phải trả là dung lượng đĩa: copytree vừa nhân đôi cả lake small, trong
# khi chỉ MỘT partition trên khoảng ba mươi partition thực sự thay đổi. Ở scale
# full thì mỗi lần publish tốn vài GB cho một partition được ghi lại.
#
# Đây đúng là lý do Iceberg và Delta Lake tồn tại. Chúng giữ ý tưởng con trỏ
# version, nhưng theo dõi từng file riêng lẻ trong metadata thay vì sao chép cả
# cây thư mục. Version N+1 dùng lại mọi file không đổi, chỉ partition được ghi
# lại mới chiếm thêm chỗ.


# %%
# ─────────────────────────────────────────────────────────────────────────────
# TASK 9c — DỌN DẸP. BẮT BUỘC. Các bài sau đòi layout chuẩn.
# ─────────────────────────────────────────────────────────────────────────────

writer.execute("DROP VIEW IF EXISTS core.lake_orders")
shutil.rmtree(root / f"orders_v={v2}")
(root / f"orders_v={v1}").rename(root / "orders")

writer.close()
reader.close()
con.close()

print("lake đã về:", (root / "orders").exists(),
      "| còn sót orders_v=*:", [p.name for p in root.glob("orders_v=*")])
# Kỳ vọng: True | []
# %%

# %% [markdown]
# # A14 — Performance tuning: đo → giải thích → sửa → đo lại
#
# Chạy theo từng cell (`# %%`). Đủ cả 8 task.
#
# Hai thứ phải chạy ở **terminal**: `a14_badpipe.py` và `a14_fastpipe.py`. Chúng
# là sản phẩm của bài (Task 3, 4, 5), nhận tham số ngày, và bản full của badpipe
# chạy ~20 phút — cell tương ứng chỉ in lệnh ra để copy.
#
# | Task | Ở đâu | Scale |
# |---|---|---|
# | 1 Cái đồng hồ | cell 2 | small |
# | 2 Ba lăng kính trên một query | cell 3, 4 | cả hai |
# | 3 Gặp badpipe | cell 5 in lệnh → terminal | small |
# | 4 Sửa, cái đắt nhất trước | terminal + cell 6 (EXCEPT) | small |
# | 5 Đối đầu ở full scale | cell 7 in lệnh → terminal | full |
# | 6 Row group và zone map | cell 8, 9, 10 | cả hai |
# | 7 Cùng query, khác ngày | cell 11 | cả hai |
# | 8 Viết checklist | cell 12 | — |
#
# **Luật của cả bài:** không phát biểu con số nào mà không đi qua `bench()`.

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — SETUP.
#
# ĐỔI SCALE Ở ĐÂY rồi chạy lại cell này. Hai dòng xoá sys.modules bên dưới ép
# Python import lại a14_bootstrap để nó đọc env vừa đặt — không có chúng thì
# `import` chỉ trả về module đã nằm sẵn trong bộ nhớ, kèm SCALE cũ, và bạn mất
# nguyên một lần chạy mới phát hiện (đúng chuyện đã xảy ra ở A13).
# ─────────────────────────────────────────────────────────────────────────────
# WANT_SCALE = "small"
WANT_SCALE = "full"

import os
import sys
from pathlib import Path

os.environ["ETL_LAB_SCALE"] = WANT_SCALE
for _m in [k for k in list(sys.modules) if k.startswith("work.a14")]:
    del sys.modules[_m]

p = Path.cwd()
while not (p / "lib" / "labpaths.py").exists():
    if p.parent == p:
        raise RuntimeError("Không tìm thấy repo root (lib/labpaths.py).")
    p = p.parent
os.chdir(p)

import json                                                     # noqa: E402
import shutil                                                   # noqa: E402
import time                                                     # noqa: E402

import duckdb                                                   # noqa: E402

from work.a14_performance.a14_bench import bench, bench_cold     # noqa: E402
from work.a14_performance.a14_bootstrap import (                 # noqa: E402
    DIMS, LAKE, MANIFEST, NORMAL_DAY, SCALE, SIX, SPIKE_DAY, TMP,
    duck, fix_console, manifest_rows, section)

assert SCALE == WANT_SCALE, f"SCALE={SCALE}, muốn {WANT_SCALE} — Restart kernel"
fix_console()
print("cwd  :", Path.cwd())
print("SCALE:", SCALE)
print("lake :", LAKE)

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Task 1: kiểm cái đồng hồ.
#
# Ba thứ làm một phép đo trung thực: warm-up (trả giá cache lạnh trước khi bấm
# giờ), median của 3 (một cú giật antivirus không được làm xê dịch con số), và
# fetchall() (thiếu nó là chỉ đo phần gửi câu lệnh đi).
# ─────────────────────────────────────────────────────────────────────────────
section("Task 1 — kiểm cái đồng hồ")
con = duck()
q = (f"SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet', "
     f"hive_partitioning=true) WHERE order_date = DATE '{NORMAL_DAY}'")

bench_cold(con, "lần đầu, cache lạnh", q)
a = bench(con, "bench lần 1", q)
b = bench(con, "bench lần 2", q)
con.close()
print(f"\nchênh giữa hai lần bench: {abs(a - b) / max(a, b) * 100:.1f}%")
print("Dưới ~10% là dùng được. Và bạn phải thấy dòng COLD chậm hơn — nếu không,")
print("hãy chạy lại sau khi đã đọc file khác một lúc; cache còn nóng từ cell 1.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Task 2, lăng kính 1: ba cách lọc, cùng một đáp án.
# Chạy ở small, rồi đổi WANT_SCALE ở cell 1 và chạy lại ở full.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 2 — ba cách lọc [{SCALE}]")
con = duck()
BASE = (f"SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet', "
        f"hive_partitioning=true) WHERE ")
filters = {
    "A strftime(ts) = str": f"strftime(order_ts, '%Y-%m-%d') = '{NORMAL_DAY}'",
    "B cast(ts) = date":    f"CAST(order_ts AS DATE) = DATE '{NORMAL_DAY}'",
    "C order_date = date":  f"order_date = DATE '{NORMAL_DAY}'",
}
med = {name: bench(con, name, BASE + f) for name, f in filters.items()}
print(f"\nA chậm hơn C {med['A strftime(ts) = str'] / med['C order_date = date']:.0f} lần"
      "  — cùng một câu trả lời.")
print("""
C cắt trên cột phân vùng nên cả file không liên quan không bao giờ mở ra.
B TRÔNG có vẻ tệ, nhưng optimizer viết lại phép cast thành một phép so khoảng
trên order_ts, và zone map cứu nó.
A giấu cột vào trong một hàm chuỗi mà không optimizer nào nhìn xuyên qua —
mọi file bị mở, mọi dòng bị định dạng.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Task 2, lăng kính 2 và 3: cái plan, rồi cùng sự thật đó dạng dữ liệu.
# ─────────────────────────────────────────────────────────────────────────────
section("Task 2 — lăng kính 2: EXPLAIN ANALYZE")
for key in ("C order_date = date", "A strftime(ts) = str"):
    print(f"\n########## {key} ##########")
    print(con.execute("EXPLAIN ANALYZE " + BASE + filters[key]).fetchall()[0][1])
print("""
Trong hộp scan: C hiện `File Filters: ...` và `Scanning Files: 1/N`; A hiện
`Total Files Read: N` với gần như toàn bộ thời gian nằm ở TABLE_SCAN.
N là tổng số file của lake — vài trăm chứ không phải 69, vì các lần APPEND theo
ngày của A10 để lại nhiều file trong một phân vùng. Con số `1` kia là bằng
chứng pruning, và nó có giá trị mãi mãi.""")

# lăng kính 3 — cùng sự thật, dạng dữ liệu query được
section("Task 2 — lăng kính 3: profile JSON")
prof_path = Path("work/profile.json")
prof_path.parent.mkdir(exist_ok=True)
con.execute("PRAGMA enable_profiling='json';")
con.execute(f"PRAGMA profiling_output='{prof_path.as_posix()}';")
con.execute(BASE + filters["A strftime(ts) = str"]).fetchall()
con.execute("PRAGMA disable_profiling;")


def ops(node):
    if "operator_timing" in node:
        yield (node["operator_timing"], node["operator_type"],
               node["operator_cardinality"])
    for child in node.get("children", []):
        yield from ops(child)


prof = json.loads(prof_path.read_text())
for tm, op, rows in sorted(ops(prof), reverse=True)[:5]:
    print(f"{tm:8.4f} s   {op:20s} rows out: {rows:,}")
con.close()
print("\nBa lăng kính, một sự thật. Cái thứ ba là cái duy nhất bạn ĐỔ VÀO BẢNG")
print("được — xem stretch goal 3: đổ (date, operator_type, operator_timing) vào")
print("ops.query_profile rồi tìm top-3 operator của cả tuần.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Task 3 và 4: chạy ở TERMINAL.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
# Task 3 — baseline. Ghi thời gian TỪNG BƯỚC, đó mới là thứ xếp hạng được:
python -m work.a14_performance.a14_badpipe 2026-06-05

# Task 4 — sau mỗi lần sửa, chạy lại và ghi số:
python -m work.a14_performance.a14_fastpipe 2026-06-05
""")
print("""Task 3 cần ít nhất BẢY cái sai, mỗi cái kèm nó nằm trong bước nào và
bước đó tốn bao nhiêu giây. Chỗ đáng chú ý: mấy cái bắt mắt — SELECT DISTINCT *,
strftime, ORDER BY — nằm gọn trong nửa giây, còn một bước nhạt nhẽo chiếm 99%
thời gian. Xếp hạng vừa nói cho bạn biết bắt đầu từ đâu.

Task 4 sửa MỘT cái một lần, chạy lại, ghi số, rồi mới sang cái tiếp. Sửa gộp
thì bạn có một con số tổng và không quy được cho ai; tệ hơn, nếu một cái làm
CHẬM đi thì cặp đó triệt tiêu nhau và trông như không có gì xảy ra.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Task 4: chứng minh không làm hỏng gì. Chạy sau MỖI lần sửa.
#
# Hai artifact, HAI CHIỀU. EXCEPT một chiều chỉ bắt được dòng mất, không bắt
# được dòng thừa. Nhanh mà sai thì không tính.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 4 — kiểm bằng EXCEPT [{SCALE}]")
con = duck()
old, new = TMP / "perf", TMP / "perf_fixed"


def diff(a: Path, b: Path, cols: str = "*") -> int:
    return con.execute(f"""
        SELECT count(*) FROM (
            (SELECT {cols} FROM read_parquet('{a.as_posix()}'))
            EXCEPT
            (SELECT {cols} FROM read_parquet('{b.as_posix()}')))""").fetchone()[0]


for art, cols in (("store_daily", "*"), ("orders", SIX)):
    #   Bản trích phải so trên SÁU CỘT HỢP ĐỒNG: fastpipe đã bỏ items và meta
    #   (fix 4), nên so "*" sẽ khác nhau vì lý do bạn CỐ Ý tạo ra. Gọi tên cột
    #   ở cả hai phía là so đúng thứ hợp đồng hứa.
    f_old = old / f"{art}_{NORMAL_DAY}.parquet"
    f_new = new / f"{art}_{NORMAL_DAY}.parquet"
    if not (f_old.exists() and f_new.exists()):
        print(f"{art:12s} chưa có đủ hai file — chạy badpipe và fastpipe trước")
        continue
    ab, ba = diff(f_old, f_new, cols), diff(f_new, f_old, cols)
    print(f"{art:12s} old-new {ab:3d}   new-old {ba:3d}   "
          f"{'OK' if ab == ba == 0 else 'KHÁC NHAU — dừng lại điều tra'}")

# Fix 3 cần bằng chứng riêng: DISTINCT có thật sự vô dụng không?
n_plain, n_distinct = con.execute(f"""
    SELECT (SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet',
                hive_partitioning=true) WHERE order_date = DATE '{NORMAL_DAY}'),
           (SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet(
                '{LAKE}/*/*.parquet', hive_partitioning=true)
                WHERE order_date = DATE '{NORMAL_DAY}'))""").fetchone()
print(f"\nDISTINCT: {n_plain:,} thường vs {n_distinct:,} qua DISTINCT -> "
      f"{'vô dụng, xoá được' if n_plain == n_distinct else 'CÓ TÁC DỤNG — điều tra!'}")
print("Không dòng nào trong lake trùng nhau từng byte — kể cả bản sửa của A08")
print("cũng khác updated_at — nên DISTINCT băm mấy chục nghìn dòng rộng để")
print("chẳng được gì. Nếu số của BẠN lệch thì dừng: một bug dữ liệu quan trọng")
print("hơn mọi phép tăng tốc.")
con.close()

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Task 5: đối đầu ở full scale. TERMINAL.
# ─────────────────────────────────────────────────────────────────────────────
print(r"""
$env:ETL_LAB_SCALE="full"    # rồi mở LẠI terminal

# Chạy cái này TRƯỚC rồi đi làm Task 6 trong lúc chờ — thời gian NGỒI XEM,
# không phải thời gian NGỒI ĐỢI. Bước validate mất khoảng hai mươi phút.
# Cứ để nó chạy hết: bạn cần con số đó trong văn bản.
python -m work.a14_performance.a14_badpipe 2026-06-05

# Rồi:
python -m work.a14_performance.a14_fastpipe 2026-06-05
""")
print("""Bọc bằng resmon của A13 thì càng tốt — bạn sẽ thấy ~2 GB RSS bị giữ bởi
một cái fetchall 1,2 triệu dòng:

python -m work.a13_resource_monitoring.a13_resmon --out D:\\ETL\\work\\mon_badpipe.csv -- python -m work.a14_performance.a14_badpipe 2026-06-05

Điền bảng: scale | badpipe TOTAL | fastpipe TOTAL | speedup. Đích tối thiểu 20x.
Rồi chạy LẠI cell 6 ở full scale — một phép tăng tốc không kèm bằng chứng bằng
nhau chỉ là tin đồn.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Task 6: row group x thứ tự sắp xếp. Ghi 4 file thí nghiệm.
# Ở full scale cell này ghi ~8 GB và mất vài phút. Cell 12 dọn.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 6 — ghi 4 biến thể [{SCALE}]")
con = duck()
SIX_SQL = (f"SELECT {SIX} FROM read_parquet('{LAKE}/*/*.parquet', "
           f"hive_partitioning=true)")
EXP = TMP / "rg_exp"
EXP.mkdir(parents=True, exist_ok=True)
variants = {
    "rg5000_unsorted":   (5000,   ""),
    "rg122880_unsorted": (122880, ""),
    "rg122880_sorted":   (122880, "ORDER BY store_id"),
    "rg5000_sorted":     (5000,   "ORDER BY store_id"),
}
for name, (rg, order) in variants.items():
    path = EXP / f"{name}.parquet"
    if path.exists():
        print(f"bỏ qua {name} (đã có)")
        continue
    t0 = time.perf_counter()
    con.execute(f"COPY ({SIX_SQL} {order}) TO '{path.as_posix()}' "
                f"(FORMAT parquet, ROW_GROUP_SIZE {rg})")
    print(f"write {name:20s} {time.perf_counter() - t0:6.1f} s")
print("\nGhi cả THỜI GIAN GHI vào journal: file sắp xếp + nhóm nhỏ là chậm nhất,")
print("vài phút ở full scale. Hai cái dial này trả giá lúc ghi, thu lãi lúc đọc.")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Task 6: đo bốn biến thể trên hai kiểu query.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 6 — đo [{SCALE}]")
for name in variants:
    pq = (EXP / f"{name}.parquet").as_posix()
    groups = con.execute("SELECT count(DISTINCT row_group_id) "
                         f"FROM parquet_metadata('{pq}')").fetchone()[0]
    print(f"--- {name}  ({groups} row group)")
    bench(con, "  quét-hết agg",
          f"SELECT store_id, sum(order_total) FROM read_parquet('{pq}') GROUP BY 1")
    bench(con, "  lọc một cửa hàng",
          f"SELECT count(*), sum(order_total) FROM read_parquet('{pq}') "
          f"WHERE store_id = 42")
print("""
Đọc như một kỹ sư: nhóm nhỏ đánh thuế MỌI query (~4x) vì mỗi nhóm có chi phí
riêng. Sắp xếp không giúp gì cho quét-hết — vẫn phải đọc từng ấy byte — nhưng
với nhóm cỡ lành mạnh thì lọc một cửa hàng nhanh lên hàng chục lần: zone map
bỏ qua gần hết các nhóm.

Số nhóm ~= số dòng / ROW_GROUP_SIZE, nhưng DuckDB làm tròn LÊN theo bội số 2048
của vector, nên 5000 thật ra là 6144.""")

# Nhìn tận mắt cái zone map
pq = (EXP / "rg122880_sorted.parquet").as_posix()
print(con.sql(f"""
    SELECT row_group_id,
           min(stats_min_value) AS min_store,
           max(stats_max_value) AS max_store
    FROM parquet_metadata('{pq}')
    WHERE path_in_schema = 'store_id'
    GROUP BY 1 ORDER BY 1 LIMIT 5"""))
print("Khoảng hẹp = bỏ qua được. File không sắp xếp thì mọi nhóm đều chứa 42.")
con.close()

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — Task 6: cùng cơ chế đó, ÁP VÀO LAKE PHÂN VÙNG. Chỉ có ý nghĩa ở FULL.
#
# Một ngày full ~1,2 triệu dòng ≈ 10 row group, nên còn chỗ để bỏ qua. Một ngày
# small chỉ vừa ĐÚNG MỘT nhóm — sắp xếp trong ngày không thể giúp gì, vì không
# có nhóm nào để bỏ qua. Biết VÌ SAO mới là phần đáng giá.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 6 — viết lại phân vùng ngày spike [{SCALE}]")
if SCALE != "full":
    print("Ở small một ngày là một row group. Đổi WANT_SCALE='full' rồi chạy lại.")
else:
    con = duck()
    src = f"read_parquet('{LAKE}/order_date={SPIKE_DAY}/*.parquet')"
    for tag, order in (("plain", ""), ("sorted", "ORDER BY store_id")):
        out = EXP / f"spike_{tag}.parquet"
        if not out.exists():
            con.execute(f"COPY (SELECT * FROM {src} {order}) TO "
                        f"'{out.as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
        g = con.execute("SELECT count(DISTINCT row_group_id) FROM "
                        f"parquet_metadata('{out.as_posix()}')").fetchone()[0]
        bench(con, f"  spike {tag} ({g} nhóm)",
              f"SELECT count(*) FROM read_parquet('{out.as_posix()}') "
              f"WHERE store_id = 42")
    con.close()
    print("""
Đây là lý do một job viết lại phân vùng (cái swap của A09) SẼ thêm
ORDER BY store_id — nếu query lọc theo cửa hàng là đường nóng. Bạn chỉ sắp xếp
vật lý được MỘT kiểu; hãy tiêu nó cho bộ lọc nóng nhất của mình.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — Task 7: cùng query, khác ngày. Chi phí là hàm của DỮ LIỆU.
# ─────────────────────────────────────────────────────────────────────────────
section(f"Task 7 — skew [{SCALE}]")
con = duck()
res = {}
for day in (NORMAL_DAY, SPIKE_DAY):
    res[day] = bench(con, f"  slice {day}",
                     f"CREATE OR REPLACE TABLE t AS SELECT {SIX} "
                     f"FROM read_parquet('{LAKE}/*/*.parquet', "
                     f"hive_partitioning=true) WHERE order_date = DATE '{day}'")
    #   CREATE OR REPLACE để lần chạy thứ hai của bench không gãy vì bảng đã có.
con.close()
r_time = res[SPIKE_DAY] / res[NORMAL_DAY]
r_rows = manifest_rows(SPIKE_DAY) / manifest_rows(NORMAL_DAY)
print(f"\n{NORMAL_DAY}: {manifest_rows(NORMAL_DAY):>10,} dòng")
print(f"{SPIKE_DAY}: {manifest_rows(SPIKE_DAY):>10,} dòng   ({r_rows:.1f}x)")
print(f"tỷ lệ thời gian: {r_time:.1f}x   so với tỷ lệ dòng {r_rows:.1f}x")
print("""
Ở SMALL hai con số bám sát nhau dù gấp 3 lần dòng — chi phí cố định (liệt kê
vài trăm file lake, lập kế hoạch) nhấn chìm phần việc thật.
Ở FULL sự thật lộ ra: đúng cỡ 3x như manifest dự đoán.

Hai bài học cho journal:
  1. Scale nhỏ XẾP HẠNG được vấn đề nhưng không ĐO CỠ được nó. Xác nhận ở full
     trước khi hứa với ai bất cứ điều gì.
  2. Lập ngân sách cho một job hằng ngày theo ngày p95 của nó, không phải theo
     ngày trung bình — lại là bài straggler của A12, và là lý do mấy cái ticket
     "đêm qua job chạy chậm" bám theo lịch.""")

# %%
# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — Task 8 + dọn dẹp.
# ─────────────────────────────────────────────────────────────────────────────
print("""Task 8 — viết work/perf_checklist.md TỪ JOURNAL, không phải từ trí nhớ.
Bốn mục bắt buộc:

  1. TRƯỚC KHI ĐỘNG VÀO GÌ — có baseline chưa, có sẵn phép kiểm bằng nhau chưa.
  2. NHỮNG KẺ TÌNH NGHI, THEO THỨ TỰ TÔI ĐÃ GẶP — vòng lặp Python theo dòng,
     thiếu bộ lọc phân vùng, SELECT *, DISTINCT/ORDER BY/join thừa, row group sai.
  3. KIỂM CHỨNG THẾ NÀO — bench.py, EXPLAIN ANALYZE, dòng `Scanning Files:`,
     profile JSON.
  4. KHI NÀO THÌ DỪNG — "đủ nhanh" là một con số kinh doanh, không phải con số
     của cái tôi.

Phải có ít nhất TÁM con số bạn tự đo trong đó.""")

# Dọn: ~8 GB file thí nghiệm ở full scale
section("Dọn dẹp")
for d in (TMP / "rg_exp", TMP / "perf", TMP / "perf_fixed"):
    size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) / 2**30 if d.exists() else 0
    print(f"{str(d):55s} {size:6.2f} GB")
print("\nXoá khi đã ghi xong số (chạy cho CẢ hai scale):")
print('  Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\\tmp\\rg_exp",'
      '"$env:ETL_LAB_DATA\\tmp\\perf","$env:ETL_LAB_DATA\\tmp\\perf_fixed"')
print('  Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\\small\\tmp\\rg_exp",'
      '"$env:ETL_LAB_DATA\\small\\tmp\\perf","$env:ETL_LAB_DATA\\small\\tmp\\perf_fixed"')

# %%
con.close()
# %%

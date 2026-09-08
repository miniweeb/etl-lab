# %% work/a07_idempotency/a07_explore.py
"""A07 — Lỗi, retry và tính idempotent.

A06 dời chốt kiểm ra biên giới giữa hai đội. A07 hỏi một câu khác hẳn: khi
pipeline chết GIỮA CHỪNG thì warehouse còn lại gì?

Câu trả lời của A05 là: một mớ hỗn độn. INSERT trần không rerun-safe, và bạn đã
thấy hậu quả ở cuối A06 — ngày 2026-06-02 nằm hai lần trong core.orders. Hôm nay
bạn dựng thứ thay thế nó, rồi cố ý phá nó hàng chục lần để chứng minh nó đứng vững.

Sợi chỉ nối về trước: _run_id mà A03 quyết định giữ lại (và từ chối _source_file,
_loaded_at) hôm nay mới SỐNG DẬY với ops.etl_runs. Task 8 chính là lúc đòi lời hứa
đó.

Scale: small cho Task 1 tới 8, chỉ Task 9 dùng full.
"""

# %%
import os
from datetime import date
from pathlib import Path

import duckdb

os.chdir(r"D:\ETL")          # lib.* và work.* chỉ import được từ repo root

from lib.labpaths import data_root, warehouse_path

SCALE = "small"
DAY = "2026-06-03"


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


con = duckdb.connect(str(warehouse_path(SCALE)))
print(f"scale={SCALE}  day={DAY}  root={data_root(SCALE)}")

# %% ── Task 0 — chuẩn bị ────────────────────────────────────────────────
section("TASK 0 — branch, và core.orders đúng hình dạng")

# Terminal:
#   git switch -c a07-idempotency
#
# Task 3 cần core.orders mang ĐÚNG hai cột lineage: _data_date và _run_id, không
# hơn. Bảng hiện tại của bạn có hình dạng khác — A05 để nó bị nhân đôi, và cuối
# A06 bạn còn load thêm một lần nữa. DROP nó đi.
#
# Đó là dữ liệu DẪN XUẤT: dựng lại được từ raw/, và sau hôm nay bạn sở hữu chính
# công cụ dựng lại nó một cách an toàn. Tâm thế "raw + code = sự thật" là một nửa
# của bài này.

print(con.execute("SELECT count(*) FROM core.orders").fetchone())   # trước khi drop
con.execute("DROP TABLE IF EXISTS core.orders")
con.execute("""
CREATE TABLE core.orders (
    order_id BIGINT, customer_id BIGINT, store_id INTEGER,
    order_ts TIMESTAMP, updated_at TIMESTAMP,
    status VARCHAR, payment_method VARCHAR, order_total DECIMAL(14,2),
    items VARCHAR, meta VARCHAR,
    order_date DATE,
    _data_date DATE, _run_id BIGINT      -- đúng HAI cột lineage, không hơn
)""")
con.sql("DESCRIBE core.orders").show()

# %% ── Task 1 — gặp mặt bug trước khi sửa ───────────────────────────────
section("TASK 1 — bug chạy hai lần")

scratch = duckdb.connect()      # in-memory; ta đang cố ý phá đồ
RAW = (data_root(SCALE) / "raw" / "orders" / f"orders_{DAY}.csv").as_posix()

scratch.execute(f"CREATE TABLE naive_orders AS SELECT * FROM read_csv('{RAW}')")
print("lần 1:", scratch.execute("SELECT count(*) FROM naive_orders").fetchone())

scratch.execute(f"INSERT INTO naive_orders SELECT * FROM read_csv('{RAW}')")
print("lần 2:", scratch.execute("SELECT count(*) FROM naive_orders").fetchone())

# ĐÍCH: 62.707 thành 125.414. Không lỗi nào. Không cảnh báo nào.
# Những lần hỏng tệ nhất của pipeline đều diễn ra TRONG IM LẶNG.

# %%
scratch.sql("""
    SELECT order_id, count(*) FROM naive_orders
    GROUP BY 1 HAVING count(*) > 2 LIMIT 5
""").show()
# Vài id xuất hiện BỐN lần: chúng vốn đã trùng ngay trong file (bản trùng trong
# file bạn gặp ở A03), giờ bị nhân đôi thêm. A08 sẽ xử lý chúng.
scratch.close()

# JOURNAL: một câu — vì sao INSERT thường không bao giờ an toàn khi chạy lại?

# %% ── Task 2 — run ledger ──────────────────────────────────────────────
section("TASK 2 — cho pipeline một trí nhớ")

# Bảng dựng trong ensure_tables() của a07_load_day.py. Chín cột:
#   run_id, run_date, step, status, started_at, finished_at, rows, source_file, error
#
# Mỗi LẦN THỬ một dòng riêng — thất bại là lịch sử đáng giữ, không phải điều
# đáng ghi đè.
#
# HAI CỘT mà người mới sẽ đặt nhầm chỗ: source_file và finished_at.
# Đó chính là "dòng này từ đâu ra / nó về lúc nào" — thứ khiến người ta muốn
# đóng dấu _source_file và _loaded_at lên TỪNG DÒNG dữ liệu. A03 đã nói không,
# và bảng này là lý do: file tạo ra một lần giao và thời điểm nó về là thuộc
# tính của LẦN CHẠY. Lưu một lần ở đây, 62.707 dòng trỏ tới bằng một _run_id.
# Nạp lại ngày đó từ file đã sửa thì MỘT dòng ledger đổi, thay vì ghi lại cùng
# một chuỗi 62.707 lần (1,2 triệu lần ở scale full).

con.execute("CREATE SCHEMA IF NOT EXISTS ops")
con.execute("CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq")
con.execute("""
CREATE TABLE IF NOT EXISTS ops.etl_runs (
    run_id      BIGINT PRIMARY KEY,
    run_date    DATE,
    step        VARCHAR,
    status      VARCHAR,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    rows        BIGINT,
    source_file VARCHAR,
    error       VARCHAR
)""")
con.sql("DESCRIBE ops.etl_runs").show()

# ACCEPT: nói được từng cột dùng làm gì mà không cần nhìn lại — kể cả HAI cột
# nào sống ở đây THAY VÌ trên dòng dữ liệu, và vì sao.

# %% ── Task 3 — loader thật ─────────────────────────────────────────────
section("TASK 3 — stage → validate → publish nguyên tử")

# Mở work/a07_idempotency/a07_load_day.py. BA điều cần bắt được:
#
# 1. UPDATE ghi 'success' nằm BÊN TRONG BEGIN…COMMIT — ledger không bao giờ
#    khai thành công cho dữ liệu không tồn tại.
# 2. DELETE WHERE _data_date = ? chạy ngay cả lần load đầu (xoá 0 dòng, không
#    tốn gì). Lần đầu và lần chạy lại đi CÙNG MỘT NHÁNH CODE — ít nhánh, ít bug.
# 3. INSERT mở dòng ledger ghi source_file NGAY, trước khi có gì hỏng. Một lần
#    thử thất bại vẫn phải nói được nó nghẹn ở file nào.
#
# HAI CHỖ TRỐNG bạn phải điền:
#   - already_succeeded(): query ops.etl_runs, True nếu có ít nhất một dòng với
#     run_date = day AND step = 'load_day' AND status = 'success'
#   - trong ensure_tables(), MỘT dòng CREATE SCHEMA cố ý đặt sai chỗ — nó phải
#     nằm TRƯỚC bảng cần tới nó
#
# Terminal:
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force

# %%
CHECKSUM_SQL = f"""
SELECT count(*) AS n,
       bit_xor(hash(order_id, updated_at, status, order_total)) AS checksum
FROM core.orders WHERE _data_date = DATE '{DAY}'"""
con.sql(CHECKSUM_SQL).show()

# ĐÍCH: cả hai lần chạy đều báo 62707 rows published; n và checksum GIỐNG HỆT
# sau lần thứ hai; thư mục logs/ có một file cho mỗi lần chạy.
#
# Checksum CỐ Ý chỉ hash các cột NGHIỆP VỤ. Cột _run_id ĐÁNG LẼ phải khác nhau
# giữa hai lần chạy, và finished_at trên dòng ledger nó trỏ tới cũng vậy. Đó là
# lineage đang ghi lịch sử, không phải dữ liệu thay đổi.
#
# GHI CHECKSUM VÀO JOURNAL — cả ngày hôm nay bạn sẽ so với nó.
# (Giá trị hash() chỉ so được trong cùng một bản cài DuckDB của bạn.)

# %% ── Task 4 — preflight ───────────────────────────────────────────────
section("TASK 4 — kiểm môi trường trước khi chạm dữ liệu")

# Thứ làm hỏng pipeline lúc 2 giờ sáng hiếm khi là SQL:
#   1. data root writable           — thư mục có thể tồn tại mà vẫn từ chối ghi
#   2. free space on data volume    — chỗ trống ổ C không giúp được gì
#   3. temp dir on data volume      — spill phải nằm cạnh dữ liệu
#   4. source CSV + manifest        — (BẠN TỰ VIẾT) ba cách một delivery hỏng
#   5. warehouse writable, not locked — mở connection CHÍNH LÀ phép dò khoá
#   6. schemas creatable            — CREATE SCHEMA IF NOT EXISTS = dò quyền miễn phí
#   7. contract parseable           — YAML hỏng phải fail ở đây, không phải giữa pipeline
#
# CHECK 4 là phần của bạn: CSV không có / CSV 0 byte / manifest không có.
# Thêm ĐÚNG MỘT dòng results; ok chỉ khi không rơi vào cả ba. Khi FAIL, detail
# phải NÊU ĐÍCH DANH đường dẫn sai, và hint phải nói làm gì.
#
# Terminal — diễn tập đỏ/xanh:
#   python -m work.preflight --scale small --date 2026-06-03; $LASTEXITCODE
#     -> 7 dòng PASS, exit 0
#   python -m work.preflight --scale small --date 2026-06-03 --tmp C:\Windows\Temp; $LASTEXITCODE
#     -> [FAIL] 3. temp dir on data volume, exit 1
#   python -m work.preflight --scale small --date 2026-05-01; $LASTEXITCODE
#     -> check 4 của bạn phải đỏ và nêu đúng đường dẫn thiếu
#
# Để thấy check 5 kích hoạt: mở warehouse từ cửa sổ thứ hai rồi chạy preflight
# trong vòng 30 giây — check 5 FAIL với "being used by another process", và
# check 6 báo trung thực "skipped: no connection".
#
# MÃ THOÁT mới là thứ scheduler tuân theo. Báo cáo đỏ mà vẫn exit 0 là chuông
# báo cháy không lắp pin.

# %% ── Task 5 — savepoint trước bão ─────────────────────────────────────
section("TASK 5 — bộ đồ cứu hộ git")

# Task 6 và 7 bắt bạn cố ý phá một pipeline đang chạy tốt, sửa code vội vàng —
# đúng hoàn cảnh mà một lần sửa hỏng tay nuốt mất file bạn mất hai giờ mới viết đúng.
#
# Terminal:
#   git add work\a07_idempotency\a07_load_day.py work\preflight.py
#   git commit -m "A07 task 5: pre-chaos savepoint"
#   git log --oneline                      # ghi lại sha ngắn
#
#   # dàn dựng thảm hoạ (phá FILE, không phải dữ liệu):
#   Set-Content work\a07_idempotency\a07_load_day.py "# TODO rewrite everything"
#   git status --short                     #  M work/a07_idempotency/a07_load_day.py
#   git diff --stat                        # thiệt hại được ĐỊNH LƯỢNG trước khi hoảng
#
#   # ba cách cứu:
#   git restore work\a07_idempotency\a07_load_day.py    # 1. mess chưa commit
#   git checkout <sha> -- work\a07_idempotency\a07_load_day.py   # 2. lấy 1 file từ commit cũ
#   git reset --hard <sha>                              # 3. vứt mọi thứ từ savepoint
#   git reflog                                          # commit đã reset vẫn còn đây
#
# Kết thúc diễn tập bằng cách chứng minh không có gì giá trị bị cháy:
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03
#     -> already loaded - skipping

# %% ── Task 6 — thả chaos monkey ────────────────────────────────────────
section("TASK 6 — pipeline chưa từng crash giữa chừng là pipeline chưa được kiểm thử")

# Bốn điểm gây lỗi trong load_day():
#   after_read_manifest  — sau bước 1
#   after_stage          — sau bước 2
#   mid_transaction      — GIỮA DELETE và INSERT   <- điểm đáng sợ
#   before_commit        — sau INSERT, trước COMMIT
#
# mid_transaction là cú quyết định: ngày cũ đã bị xoá, ngày mới chưa được ghi.
# Không có transaction thì lần crash này LÀM MẤT MỘT NGÀY DỮ LIỆU.
#
# Terminal, chạy 8-10 lần:
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force --chaos 0.6
#
# LƯU Ý XÁC SUẤT: tới được một điểm sau nghĩa là đã sống sót qua mọi lần tung
# xúc xắc trước đó, nên các điểm sau hiếm dần theo hàm mũ. Với chaos 0.6,
# before_commit chỉ nổ ở ~4% số lần chạy; thấy được nó có thể mất 20-30 lần.
# Đó là xác suất, không phải lỗi trong cách bạn gắn maybe_fail.
# %%
con.sql("""
    SELECT error, count(*) AS n FROM ops.etl_runs
    WHERE status='failed' GROUP BY 1 ORDER BY 2 DESC
""").show()

con.sql("SELECT status, count(*) FROM ops.etl_runs GROUP BY 1").show()
# %%
con.close()  
# %%
# Sau MỖI lần crash, chạy lại checksum:
con.sql(CHECKSUM_SQL).show()
# n vẫn 62.707, checksum KHÔNG ĐỔI — kể cả khi nó chết giữa DELETE và INSERT,
# vì ROLLBACK đã hoàn tác lệnh xoá.

con.sql("SELECT run_id, status, rows, error FROM ops.etl_runs ORDER BY run_id DESC LIMIT 5").show()

# ACCEPT: đã thấy nó chết ở CẢ BỐN điểm, và checksum không hề nhúc nhích.

# %% ── Task 7 — retry với exponential backoff ───────────────────────────
section("TASK 7 — crash sạch là tốt, tự phục hồi không cần người còn tốt hơn")

# with_retry() CHỈ bắt TransientError. Một ValueError từ khâu validate nghĩa là
# DỮ LIỆU sai — retry không sửa được và phải fail NGAY.
# JOURNAL một câu về sự phân biệt này; đó là quy tắc bị bỏ qua nhiều nhất trong
# thiết kế retry.
#
# Thời gian chờ: 1.0s -> 2.0s -> 4.0s -> 8.0s, trần ở 30s.
#
# Terminal:
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force --chaos 0.5
#     -> attempt 1/8 failed ... retrying in 1.0s / 2.0s / 4.0s
#
#   python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force --chaos 1 --max-attempts 3
#     -> bỏ cuộc sau 3 lần

# %%
con.sql("SELECT severity, run_id, message FROM ops.alerts ORDER BY alert_ts DESC LIMIT 1").show()
# ĐÚNG MỘT dòng mới, severity 'error', run_id là lần thử thất bại CUỐI CÙNG.
# Chính sách retry và chính sách cảnh báo là HAI quyết định khác nhau: một lần
# load bỏ cuộc phải được NGHE THẤY.

# %% ── Task 8 — phần chứng minh ─────────────────────────────────────────
section("TASK 8a — bốn checksum")

# 1. Mốc sạch:    --force, không chaos.           Ghi n + checksum.
# 2. Vòng chaos:  --force --chaos 0.3 --max-attempts 10. Để chạy tới thành công.
# 3. Lặp bước 2 thêm HAI lần.
# 4. Kiểm skip:   chạy KHÔNG --force -> "already loaded - skipping", ~1 giây.
#
# Cả bốn checksum phải GIỐNG HỆT. Một lần chạy crash 5 lần trên đường đi KHÔNG
# PHÂN BIỆT ĐƯỢC với một lần chạy thành công ngay từ đầu.
#
# Xác suất ~1/15 rằng cả 10 lần thử đều fail. Cứ chạy lại — và để ý rằng ngay cả
# khi thất bại hoàn toàn thì bảng vẫn NHẤT QUÁN, và nó đã báo động qua ops.alerts.
# Đó chính là tính chất đang hoạt động.

con.sql(CHECKSUM_SQL).show()

# %%
section("TASK 8b — truy vết, phần thu hoạch")

con.sql("DESCRIBE core.orders").show()
# Mười cột nguồn, một order_date suy ra, rồi CHỈ _data_date và _run_id. HAI cột.
# Ít hơn bản năng của bạn muốn — A03 đã thuyết phục bạn bỏ _source_file và
# _loaded_at, và hứa rằng bạn sẽ không thấy thiếu. Giờ là lúc ĐÒI LỜI HỨA.

# %%
con.sql(f"""
    SELECT o.order_id, o._data_date, o._run_id,
           r.source_file, r.finished_at, r.status, r.rows
    FROM core.orders o
    JOIN ops.etl_runs r ON r.run_id = o._run_id
    WHERE o._data_date = DATE '{DAY}'
    LIMIT 1
""").show()
# Bất kỳ dòng nào cũng cho CÙNG câu trả lời, và đó chính là điểm mấu chốt.
#
# Nói to điều được lợi, vì nó là toàn bộ lập luận: một sự thật về LẦN CHẠY được
# lưu MỘT LẦN, ở lần chạy đó, và 62.707 dòng trỏ tới bằng một id 8 byte — thay
# vì 62.707 bản sao giống hệt của một tên file và một dấu thời gian nằm trong
# fact table. Đó là normalization thông thường, cùng quy tắc giữ địa chỉ khách
# hàng không bị chép vào từng dòng đơn hàng.
#
# Cả 62.707 dòng đều ghi tên CÙNG MỘT run — lần thử thành công cuối cùng. Vài
# lần thử thất bại đã đi tới được bước INSERT, nhưng ROLLBACK xoá sạch dòng của
# chúng; chỉ ledger và file log còn nhớ.
#
# Giờ mở logs/run_<id đó>.log: tiểu sử của dòng dữ liệu gói trong một file.
# dòng -> lần chạy -> log là chuỗi audit, và bạn vừa đi hết nó.

# %%
con_full = duckdb.connect(str(warehouse_path("full")))
con_full.sql("""
    SELECT count(*) AS n,
           bit_xor(hash(order_id, updated_at, status, order_total)) AS checksum
    FROM core.orders WHERE _data_date = DATE '2026-06-01'
""").show()
con_full.close()
# %% ── Task 9 — scale full ──────────────────────────────────────────────
section("TASK 9 — cùng code, khối lượng thật")

# Terminal:
#   foreach ($d in "2026-06-01","2026-06-02","2026-06-03") {
#     python -m work.a07_idempotency.a07_load_day --scale full --date $d
#   }
#
# Theo dõi Task Manager: Python + DuckDB nên ở khoảng 1-2 GB. DuckDB đọc CSV
# theo dòng chảy — không giống vách đá pandas ở A01. Mỗi ngày khoảng 10-60 giây.
#
# Rồi:
#   --force chạy lại 2026-06-01  -> cùng thời lượng, cùng checksum
#                                   (thay 1,2 triệu dòng là chuyện rẻ với DuckDB)
#   chạy lại KHÔNG --force        -> skip tức thì
#   đối chiếu ops.etl_runs.rows với manifest -> phải khớp CHÍNH XÁC

# %%
con.sql("""
    SELECT run_date, status, rows,
           finished_at - started_at AS duration
    FROM ops.etl_runs
    WHERE step = 'load_day' AND status = 'success'
    ORDER BY run_date, run_id
""").show()

# %% ── Verify — bảy truy vấn ────────────────────────────────────────────
section("VERIFY — bảy truy vấn chứng minh tính chất")

# 1. n chính xác, checksum không bao giờ đổi
con.sql(CHECKSUM_SQL).show()

# 2. vài dòng failed (mỗi error nêu tên một điểm chaos) cộng các dòng success;
#    KHÔNG dòng nào kẹt ở 'running' sau khi các lần chạy đã kết thúc
con.sql("SELECT status, count(*) FROM ops.etl_runs GROUP BY 1").show()

# 3. ĐÚNG MỘT run sở hữu cả ngày -> kỳ vọng 1
con.sql(f"SELECT count(DISTINCT _run_id) FROM core.orders WHERE _data_date = DATE '{DAY}'").show()

# 4. ngân sách cột lineage: đúng HAI cột do warehouse thêm vào
con.sql(r"""SELECT column_name, column_type FROM (DESCRIBE core.orders)
            WHERE column_name LIKE '\_%' ESCAPE '\'""").show()

# 5. mọi thứ chúng thay thế chỉ cách một phép join — không dòng nào mồ côi
con.sql(f"""
    SELECT count(*) AS rows_traced FROM core.orders o
    JOIN ops.etl_runs r ON r.run_id = o._run_id
    WHERE o._data_date = DATE '{DAY}'
      AND r.source_file = 'orders_{DAY}.csv'
      AND r.finished_at IS NOT NULL
""").show()

# 6. sau demo bỏ cuộc ở Task 7
con.sql("SELECT severity, run_id, message FROM ops.alerts ORDER BY alert_ts DESC LIMIT 1").show()

# 7. kiểm tra ghi dở: mỗi ngày đã load hoặc là 0 (chưa từng thành công) hoặc là
#    đúng con số manifest — giờ bạn KHÔNG THỂ tạo ra trạng thái ở giữa nữa
con.sql("SELECT _data_date, count(*) FROM core.orders GROUP BY 1 ORDER BY 1").show()

#%%
import json
con_full = duckdb.connect(str(warehouse_path("full")))
for d in ["2026-06-01", "2026-06-02", "2026-06-03"]:
    m = json.loads((data_root("full") / "raw" / "manifest" / f"orders_{d}.json").read_text())
    loaded = con_full.execute(
        "SELECT count(*) FROM core.orders WHERE _data_date = ?", [d]).fetchone()[0]
    print(f"{d}: manifest={m['rows']}  loaded={loaded}  {'OK' if m['rows']==loaded else 'MISMATCH'}")
con_full.close()
# %%
con.close()

# %%
con_full = duckdb.connect(str(warehouse_path("full")))

# đúng MỘT run sở hữu mỗi ngày
con_full.sql("""
    SELECT _data_date, count(DISTINCT _run_id) AS runs, count(*) AS rows
    FROM core.orders GROUP BY 1 ORDER BY 1
""").show()

# không dòng nào kẹt ở 'running'
con_full.sql("SELECT status, count(*) FROM ops.etl_runs GROUP BY 1").show()

# ngân sách cột lineage: đúng HAI
con_full.sql(r"""SELECT column_name, column_type FROM (DESCRIBE core.orders)
                 WHERE column_name LIKE '\_%' ESCAPE '\'""").show()

con_full.close()
# %%

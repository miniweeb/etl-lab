# %% work/a06_data_contract/a06_explore.py
"""A06 — Data contracts: dời chốt kiểm soát ra BIÊN GIỚI giữa hai đội.

A05 tìm ra rác SAU KHI nó đã nằm trên đĩa của bạn — lúc đó nó là vấn đề của bạn,
bất kể nguyên nhân là gì. Data contract dời chốt kiểm ra ranh giới giữa producer
và consumer: một lời hứa ĐÃ KÝ, ĐÁNH VERSION, mà một script cưỡng chế được.

KHÁC A05 CHỖ NÀO: bộ check A05 trả lời "dữ liệu này đủ tốt để nạp chưa?" từ phía
BẠN, sau khi file đã về, từng dòng một. Contract trả lời "file này có đúng là thứ
ta đã thoả thuận không?" — và vì lời hứa được viết ra và đánh version, một thất
bại là CÓ NGƯỜI CHỊU TRÁCH NHIỆM: hoặc shopcore phá lời hứa, hoặc lời hứa phải
đổi qua một lần bump version mà cả hai bên đều thấy.

Gate hôm nay KHÔNG thay thế A05. Nó chạy TRƯỚC, như một pre-flight nhanh trên
mẫu; A05 vẫn validate từng dòng sau đó.

Phần lớn A06 là công việc VIẾT (journal, incident note, amendment) và chạy CLI ở
terminal. File này gom những cell cần chạy tương tác.
"""
import os
while not os.path.exists("lib") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print(os.getcwd())

# %%
from pathlib import Path

import duckdb
import yaml

from lib.labpaths import data_root, quarantine_dir, raw_orders_dir, warehouse_path

SCALE = "small"
CONTRACT = Path("contracts/shopcore_orders_daily.contract.yaml")


def section(title: str) -> None:
    print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")


con = duckdb.connect(str(warehouse_path(SCALE)))
print(f"scale={SCALE}  contract={CONTRACT}")

# %% ── Task 1 — đọc contract như một người chuyên nghiệp ──────────────
section("TASK 1 — mười câu hỏi, mỗi câu TRÍCH một điều khoản")

# Mở contracts/shopcore_orders_daily.contract.yaml và contracts/README.md CẠNH
# NHAU. Phần "per-column interrogation" của README là checklist: với MỖI cột —
# kiểu thật là gì, có null được không, có lặp được không, timezone nào, tiền tệ
# nào, và ĐIỀU GÌ KHÔNG ĐƯỢC VIẾT RA?
#
# Đây là việc ĐỌC và VIẾT JOURNAL, không phải việc chạy code. Cell này chỉ giúp
# bạn tra nhanh trong lúc trả lời.

c = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
print(f"{c['contract']} v{c['version']} ({c['status']}, from {c['effective_from']})")
print("blocks:", list(c.keys()))
print("columns:", [col["name"] for col in c["schema"]])

# %%
# Tra một cột bất kỳ trong lúc trả lời mười câu hỏi:
def clause(col_name: str) -> dict:
    return next(x for x in c["schema"] if x["name"] == col_name)


print(yaml.dump(clause("order_total"), allow_unicode=True, sort_keys=False))
print(yaml.dump(c["late_corrections"], allow_unicode=True, sort_keys=False))
print(yaml.dump(c["delivery"], allow_unicode=True, sort_keys=False))

# MƯỜI CÂU HỎI — trả lời MỘT DÒNG mỗi câu vào journal, KÈM TÊN ĐIỀU KHOẢN.
# Hai câu đầu đã làm mẫu trong đề:
#   1. order_total tiền tệ gì, ai làm tròn, làm tròn thế nào?
#      -> USD, đơn tệ theo contract; scale 2, half-up, do PRODUCER áp dụng
#         (schema.order_total.money). Warehouse KHÔNG BAO GIỜ làm tròn lại.
#   2. Một khoản hoàn tiền về. Số tiền mang dấu gì?
#      -> DƯƠNG. Cái "âm" nằm ở status=refunded, không nằm trong con số
#         (money.negative_allowed: false + business_rules.refunds). Một
#         SUM(order_total) ngây thơ vì thế THỔI PHỒNG doanh thu nếu bỏ qua status.
#   3. order_ts timezone gì, ai đã chuyển đổi, ai xử lý DST?
#   4. Finance muốn báo cáo theo giờ địa phương của cửa hàng. Chuyển đổi timezone
#      xảy ra Ở ĐÂU — và vì sao contract CẤM chuyển đổi lúc ingest?
#      (Gợi ý: warehouse_policy — một fact UTC thô phục vụ được MỌI mart khu vực;
#       một fact đã chuyển đổi lúc ingest chỉ phục vụ được đúng MỘT.)
#   5. customer_id về NULL. Mất dữ liệu, bug của producer, hay Ý NGHĨA?
#   6. order_id có duy nhất không? Duy nhất trong PHẠM VI nào — mỗi file, mỗi
#      ngày, hay mỗi đơn hàng ngoài đời thật?
#   7. Hai cột nào quyết định bản sao nào của một đơn gửi lại sẽ THẮNG?
#   8. File của ngày D được với ngược về quá khứ bao xa? Vậy nạp file D có thể
#      chạm vào những phân vùng lake nào? (A08 xây trên con số này.)
#   9. Làm sao biết một lần giao hàng là HOÀN TẤT — và hai con số nào phải đối
#      chiếu ở MỌI lần nạp?
#  10. Vào lúc mấy giờ thì một file thiếu thôi không còn là buổi sáng chậm chạp
#      của bạn mà trở thành SỰ CỐ của shopcore?
#
# ACCEPT: cả mười câu trong journal kèm tên điều khoản, và bạn giải thích được
# câu #4 cho một con vịt cao su nghe mà không cần mở file ra.

# %% ── Task 2 — vi phạm hay khoảng trống? ─────────────────────────────
section("TASK 2 — phân loại phát hiện A05 đối chiếu với lời hứa")

# VI PHẠM (violation) = rác mà contract MINH THỊ CẤM. Bị cách ly, bị đếm, và
#   ĐƯỢC BÁO CÁO CHO PRODUCER — không bao giờ vá âm thầm, vì vá âm thầm giấu một
#   bug của producer mãi mãi.
# KHOẢNG TRỐNG (gap) = rác mà contract IM LẶNG. Bạn không thể đổ lỗi cho shopcore
#   về một lời hứa chưa từng được đưa ra. Gap được xử lý TƯỜNG MINH trong ETL của
#   bạn VÀ biến thành một ĐỀ XUẤT SỬA ĐỔI để sự im lặng khép lại.
# ĐƯỢC DUNG THỨ (tolerated) = contract cho phép MINH THỊ ở mức tỉ lệ đó.
#
# Cẩn thận với loại cuối: các bản gửi lại y hệt TRONG CÙNG FILE mà bạn gặp ở
# A03/A05 KHÔNG PHẢI vi phạm — duplicates.within_file_exact_resends dung thứ tới
# 0,2% như CÁI GIÁ của giao hàng at-least-once.

q = (quarantine_dir(SCALE) / "orders").as_posix()
con.sql(f"""
    SELECT reject_reason, count(*) AS rows
    FROM read_parquet('{q}/order_date=*/*.parquet')
    GROUP BY 1 ORDER BY 2 DESC
    
""").show(max_rows=30)
con.sql("SELECT count(*) FROM core.orders").show()
# %%
# Bung danh sách '; ' ra để phân loại TỪNG lý do riêng lẻ:
con.sql(f"""
    SELECT reason, count(*) AS rows FROM (
      SELECT unnest(string_split(reject_reason, '; ')) AS reason
      FROM read_parquet('{q}/order_date=*/*.parquet'))
    GROUP BY 1 ORDER BY 2 DESC
""").show(max_rows=30)

con.sql("""
    SELECT check_name, sum(rows_failed) AS rows_failed
    FROM ops.dq_report WHERE check_type = 'row'
    GROUP BY 1 ORDER BY 2 DESC
""").show(max_rows=20)

# HAI ARTIFACT phải viết (đội thật viết đúng hai thứ này):
#
# 1. work/incident_note.md — gửi checkout-eng@shopcore.example (contact của
#    producer, lấy TỪ contract). MỖI VI PHẠM kèm số đo của MỘT ngày và điều khoản
#    nó phá. Nửa trang. Producer phải hành động được mà KHÔNG cần đọc code bạn.
#    Đây chính là báo cáo hằng tuần mà block slo_reporting của contract đã hứa
#    với họ. TUYỆT ĐỐI không có chữ "xin hãy sửa" về một GAP.
#
# 2. Danh sách GAP trong journal — đầu vào cho Task 3. Không đổ lỗi, không cần
#    số; chỉ cần "contract không nói X, mà dữ liệu thì làm Y".

# %% ── Task 3 — săn khoảng trống ──────────────────────────────────────
section("TASK 3 — hunting the silences before they hunt you")

# Tác giả contract để lại HAI comment `# NOTE` trong YAML, cố ý đánh dấu hai sự
# im lặng. TÌM CHÚNG — đó là hai mục đầu tiên và là KHUÔN MẪU cho việc "một sự im
# lặng trông như thế nào".
print("\n".join(l for l in CONTRACT.read_text(encoding="utf-8").splitlines()
                if "NOTE" in l))

# Rồi săn ÍT NHẤT BỐN cái tổng cộng (nghĩa là hai cái trở lên do BẠN tìm), dựa
# vào mọi thứ dữ liệu đã làm với bạn từ A03 tới giờ. Mỗi cái ghi ba cột:
#
# | sự im lặng (contract KHÔNG nói gì) | nó cắn thế nào (rác/mơ hồ CỤ THỂ) | một dòng điều khoản khép nó lại |
#
# Gap KHÔNG PHẢI báo cáo bug. Producer không nợ bạn gì ở đây — và đó CHÍNH XÁC là
# thứ khiến gap nguy hiểm: không ai chịu trách nhiệm cho tới khi một điều khoản
# tồn tại. Cột "điều khoản khép lại" là hạt giống của amendment ở Task 11.
#
# Ứng viên: transport locale của tiền (dấu phẩy châu Âu, tiền tố $) · kiểu bên
# trong của items · cách biểu diễn null của utm · timezone và ý nghĩa của
# updated_at · chính sách công bố lại · PII và lưu trữ cho feed customers
# (cái này Task 12 khép lại THẬT).

# %% ── Task 4 — vì sao gate phải test theo kiểu ĐÃ KHAI ────────────────
section("TASK 4 — hỏi sniffer nó NGHĨ file trông thế nào")

f = (raw_orders_dir(SCALE) / "orders_2026-06-01.csv").as_posix()
scratch = duckdb.connect()          # in-memory, không đụng warehouse
for col in scratch.execute("SELECT Columns FROM sniff_csv(?)", [f]).fetchone()[0]:
    print(f"{col['name']:16s} {col['type']}")
scratch.close()

# ĐỌC KỸ ba dòng:
#   customer_id -> DOUBLE       vì một phần nhỏ phần trăm giá trị trông như
#                               "123456.0" (rác Excel-float của A03) và phỏng
#                               đoán NỚI RỘNG ra để vừa với rác
#   order_ts    -> VARCHAR
#   updated_at  -> TIMESTAMP WITH TIME ZONE   <- một timezone CHẲNG AI HỨA.
#                               Đây là nguyên liệu Task 3!
#
# Kiểu đoán được TRÔI theo bất kỳ rác nào rơi vào mẫu. Block `schema` của
# contract là ĐIỂM CỐ ĐỊNH để đối chiếu. Đó là lý do gate chỉ lấy TÊN từ sniffer
# và tự phán xét kiểu bằng TRY_CAST.
#
# Giờ viết work/contract_check.py (khung đã cho, ba TODO trong dict `checks`,
# một TODO ở tầng 5, và main() là của bạn).
#
# ACCEPT: mọi TODO đã điền và một file small pass:
#   [PASS] đi trước bởi MỘT dòng [warn] về payment_method (~0,3% — sự im lặng
#   của contract được phơi ra trên mỗi lần chạy).

# %% ── Task 5 — validate CHÍNH CONTRACT bằng pydantic ─────────────────
section("TASK 5 — không có gì validate cái validator")

# Gate của bạn giờ là thứ nghiêm khắc nhất pipeline — về DỮ LIỆU. Về CONTRACT thì
# nó CẢ TIN: yaml.safe_load trả về một dict trần, và mọi contract["schema"],
# contract["quality"]["row_level_tolerances"], c["allowed_values"] sau đó đều là
# phỏng đoán chưa kiểm về một file mà CON NGƯỜI biên tập.
#
# Hai hình dạng sai lầm trong YAML sửa tay — và contract này sắp bị sửa đổi HAI
# LẦN (Task 11-12), nên cả hai đều đang sống:
#   - KHOÁ GÕ SAI. Ai đó soạn v1.4.0 gõ `nullible: true`. YAML vui vẻ chấp nhận —
#     đó là một khoá mapping hoàn toàn hợp lệ. Không gì đọc tài liệu như một
#     TỔNG THỂ, nên lỗi nằm đó tới khi một dòng code tình cờ chạm vào khoá đó:
#     một KeyError vài ngày sau đổ lỗi cho SCRIPT CỦA BẠN và không hề nhắc tới
#     contract — hoặc, ở chỗ bạn viết .get() hay `if "x" in c`, KHÔNG lỗi gì cả,
#     chỉ là một luật lặng lẽ ngừng tồn tại. Một gate có một luật bị tắt âm thầm
#     TỆ HƠN không có gate, vì bạn vẫn tin nó.
#   - KIỂU SAI. `window_days: seven`. Không ai kêu ca cho tới lúc số học
#     late-corrections chạy — ở file thứ 30 của 45, với một TypeError gọi tên
#     timedelta và không hề nhắc tới contract.
#
# Chạy ở TERMINAL:
#   python -m work.contract_model
#   -> shopcore_orders_daily v1.3.0 (agreed, effective 2026-06-01)
#      10 columns, late window 7 days
#      order_total negative_allowed = False (bool)
#      status tolerance = 1%
#
#   python -m work.contract_typo_demo
#   -> [yaml ] loads fine: ... customer_id.nullable = <<GONE>>, window_days = 'seven'
#      [typed] 3 validation errors for Contract
#              late_corrections.window_days / schema.1.nullable / schema.1.nullible
#
# Cả hai đều cần một work/__init__.py RỖNG (import kiểu package, giống Task 8).
#
# BỐN QUYẾT ĐỊNH đáng tranh luận (đọc trong contract_model.py):
#   - extra="forbid" là TOÀN BỘ vấn đề. Mặc định của pydantic là BỎ QUA khoá lạ.
#     Gõ sai một khoá BẮT BUỘC thì vẫn có lỗi, nhưng CHỈ SAI CHỖ. Gõ sai một khoá
#     TUỲ CHỌN (alowed_values:) thì mặc định nuốt chửng trong im lặng hoàn toàn,
#     y hệt yaml.safe_load.
#   - Mô hình hoá thứ gate ĐỌC, không phải cả tài liệu.
#   - `schema` cần alias vì BaseModel.schema đã bị pydantic chiếm.
#   - Kiểu, không chỉ khoá — negative_allowed: bool là cái sắc nhất.
#
# KHI NÀO ĐỪNG DÙNG: một model kiểu Column cho MỖI DÒNG DỮ LIỆU nghĩa là 82 triệu
# object Python cho feed đầy đủ, mỗi cái một lượt validate — hàng giờ CPU và RAM
# bạn không có, để trả lời những câu DuckDB trả lời trong một lượt quét cột.
# pydantic dành cho CONFIG; SQL và frame schema dành cho FEED.

con.sql("SELECT 1").show()   # giữ cell chạy được; công việc thật ở terminal

# %% ── Task 6 — gate cả tháng 1 (small) ───────────────────────────────
section("TASK 6 — sweep 45 ngày, small")

# Terminal:
#   python work\contract_check.py --scale small --start 2026-06-01 --end 2026-07-15
#
# ĐÍCH: 45 dòng [PASS] (mỗi dòng kèm [warn] payment_method của nó) và
#   45/45 files passed contract shopcore_orders_daily v1.3.0
# trong khoảng 25-30 giây trên máy chuẩn.
#
# Chi phí mỗi file là LẦN ĐỌC MẪU, không phải kích thước file — nhớ điều đó cho
# Task 9. Nếu một file FAIL, ĐỌC ĐIỀU KHOẢN ĐƯỢC TRÍCH trước khi động vào bất cứ
# thứ gì — và nhớ rằng cách sửa KHÔNG BAO GIỜ là "biên tập contract đã ký cho tới
# khi nó xanh".
#
# Ghi thời gian chạy vào journal.

# %% ── Task 7 — phá nó có chủ ý ───────────────────────────────────────
section("TASK 7 — ba file bị làm hỏng")

# Bạn không thể tin một cái gate chưa từng thấy nó hỏng.
# Terminal, chạy MỘT LẦN:
#   python -m work.make_tampered
#
# Rồi chĩa gate vào cả ba:
#   python work\contract_check.py `
#     "$env:ETL_LAB_DATA\tmp\tampered\dropped_col\orders_2026-06-02.csv" `
#     "$env:ETL_LAB_DATA\tmp\tampered\added_col\orders_2026-06-02.csv" `
#     "$env:ETL_LAB_DATA\tmp\tampered\type_change\orders_2026-06-02.csv"
#
# ĐÍCH: 0/3 files passed, mỗi thông điệp gọi tên chính xác vấn đề:
#   missing column(s) ['payment_method']   + trích MAJOR/30 ngày
#   unexpected column(s) ['currency']      + trích MINOR/7 ngày
#   order_id_not_castable: 5000/5000 sampled rows (100.00%) với ví dụ 'ORD-...'
# cộng một [warn] manifest mỗi file (bản copy scratch không có tín hiệu hoàn tất
# — gate NÓI RA thay vì fail các check ad-hoc).
#
# JOURNAL: TẦNG NÀO bắt được tamper nào?
#   dropped/added -> tầng SCHEMA
#   type_change   -> LỌT QUA schema (cùng tên, cùng thứ tự!), bị tầng GIÁ TRỊ bắt
#
# Không phải tamper ngẫu nhiên: cột `currency` mới và order_id dạng chuỗi CHÍNH LÀ
# thứ shopcore ship dưới tên "month 2" ở A10.

# %% ── Task 8 — cắm gate vào pipeline A05 ─────────────────────────────
section("TASK 8 — một gate chạy khi bạn nhớ ra thì không phải gate")

# Đặt nó ở ĐẦU script nạp mỗi ngày của A05, TRƯỚC MỌI LẦN ĐỌC dữ liệu.
# Trong work/a05_validation/a05_validate_day.py, đầu hàm validate_day():
#
#     from work.contract_check import load_contract, check_file
#
#     problems = check_file(csv_path, load_contract())
#     if problems:
#         for msg in problems:
#             print(" -", msg)
#         raise SystemExit(f"[contract gate] {csv_path.name} FAILED - nothing was loaded.")
#     print(f"[contract gate] {csv_path.name} PASS")
#
# Và cho validate_day() một tham số csv_path TUỲ CHỌN (mặc định là đường dẫn raw
# chuẩn) để gate — và CHỈ gate — chĩa được vào bản tampered. TUYỆT ĐỐI KHÔNG chép
# file tampered đè lên một đường dẫn thật dưới raw/.

before = con.execute("SELECT count(*) FROM core.orders").fetchone()[0]
print("core.orders trước:", before)

# %%
# Chạy lại một ngày small trọn vẹn: dòng gate PASS, rồi lần nạp A05 bình thường.
# LƯU Ý: core.orders sẽ chứa ngày chạy lại HAI LẦN — INSERT trần không rerun-safe
# (giới hạn đã biết của A05). ĐỪNG sửa tay; Setup của A07 sẽ drop và dựng lại
# bảng này bằng một loader idempotent đàng hoàng.

from work.a05_validation.a05_validate_day import validate_day   # noqa: E402

validate_day(con, "2026-06-02", SCALE)

# %%
# Rồi chĩa CÙNG pipeline vào file type_change:
tampered = (data_root(SCALE).parent / "tmp" / "tampered" / "type_change"
            / "orders_2026-06-02.csv")
try:
    validate_day(con, "2026-06-02", SCALE, csv_path=tampered)
except SystemExit as e:
    print("ĐÚNG NHƯ MONG ĐỢI:", e)

after = con.execute("SELECT count(*) FROM core.orders").fetchone()[0]
print(f"core.orders: {before} -> {after}")
# Xác nhận run BỎ CUỘC TRƯỚC KHI warehouse bị đụng tới. Cùng bài học với gate của
# A05, nhưng dời sớm hơn một bậc: A05 chặn sau khi ĐỌC, A06 chặn trước khi đọc.

# %% ── Task 9 — sweep quy mô đầy đủ ────────────────────────────────────
section("TASK 9 — 45 file x ~350MB, dưới một phút")

# Terminal:
#   python work\contract_check.py --scale full --start 2026-06-01 --end 2026-07-15
#
# ĐÍCH: lại 45/45 PASS — dưới một phút, khoảng một giây mỗi file.
#
# Các file này ~350 MB mỗi cái, gấp HAI MƯƠI LẦN bản small, vậy mà gate gần như
# không chậm lại, vì nó lấy mẫu 100.000 dòng rồi DỪNG. Mở Task Manager trong lúc
# chạy: bộ nhớ gần như không nhúc nhích.
#
# Một pre-flight check phải RẺ ĐỦ ĐỂ KHÔNG AI BAO GIỜ BỊ CÁM DỖ BỎ QUA NÓ — đó là
# YÊU CẦU THIẾT KẾ, không phải điều tốt nếu có. Bỏ LIMIT thì sweep full đọc ~16GB
# thay vì vài trăm MB, và gate đắt là gate bị bỏ qua.
#
# Ghi tổng thời gian vào journal.

# %% ── Task 10 — catalogue thay đổi (journal) ──────────────────────────
section("TASK 10 — semver cho mỗi kịch bản")

# Với MỖI kịch bản, viết vào journal: breaking hay non-breaking, VỚI AI, và bump
# mà shopcore phải làm (kèm thời hạn báo trước mà change_management đòi hỏi).
#
# | # | Thay đổi thượng nguồn | Breaking? Với ai? | Bump |
# |---|---|---|---|
# | 1 | Thêm cột tuỳ chọn ở CUỐI | Non-breaking với reader đọc theo TÊN; breaking với reader đọc theo VỊ TRÍ (gate exact-order của ta gắn cờ để review) | MINOR, 7 ngày |
# | 2 | Đổi tên payment_method -> payment_type | Breaking — mọi truy vấn gọi tên cột đó | MAJOR, 30 ngày |
# | 3 | order_id thành chuỗi 'ORD-...' | ? | ? |
# | 4 | Chèn cột mới vào GIỮA | ? | ? |
# | 5 | Một giá trị status mới "on_hold" bắt đầu xuất hiện | ? | ? |
# | 6 | Tỉ lệ status rác bò từ 0,3% lên 0,8% trong một tháng | ? | ? |
# | 7 | Đổi cách đặt tên file thành orders-YYYYMMDD.csv | ? | ? |
# | 8 | Producer sửa lỗi chính tả trong phần mô tả của contract | ? | ? |
#
# Nghĩ kỹ #5 và #6, hai cái ranh mãnh:
#   #5 phá MỌI consumer có logic `CASE status ...` liệt kê đầy đủ, dù schema
#      KHÔNG hề đụng tới.
#   #6 CHƯA phá gì cả — nó vẫn qua được tolerance null_or_invalid_status 1% của
#      contract — và đó chính xác là kiểu trôi chậm mà bảng lịch sử ở stretch
#      goal 1 sẽ phơi ra.

# %% ── Task 11 — soạn amendment v1.4.0 ────────────────────────────────
section("TASK 11 — biến gap thành clause")

# Contract đã ký GIỮ NGUYÊN — bạn soạn work/amendment_v1_4_0.md với các đoạn
# status: proposed và một ghi chú rollout, đúng thứ bạn sẽ đăng lên
# #shopcore-data-changes. Phải có:
#
#   1. Điều khoản LOCALE (đề bài cho sẵn phần mở đầu) — biến gap dấu phẩy châu Âu
#      và tiền tố $ thành VI PHẠM từ nay về sau.
#   2. MỘT điều khoản nữa khép một gap Task 3 do bạn chọn. Một gap được DÀNH
#      RIÊNG: PII và lưu trữ — Task 12 soạn nó đầy đủ rồi bắt bạn TRIỂN KHAI, nên
#      chọn một sự im lặng khác.
#   3. Ghi chú ROLLOUT, ba câu: bump version và VÌ SAO NÓ LÀ MINOR (nó thêm ràng
#      buộc khớp với ý định hiện tại; không code consumer nào vỡ; nó nhiều hơn
#      một PATCH sửa câu chữ), thời hạn báo trước theo change_management, và bước
#      trong vòng đời status (proposed cho tới khi ký đối ứng — lúc đó data
#      engineer mới được phép dựa vào nó).
#
# Paste-check: thả từng đoạn vào một file .yaml scratch rồi chạy qua
# yaml.safe_load. Một điều khoản đề xuất có lỗi cú pháp là điều khoản KHÔNG THỂ
# REVIEW.

# snippet = """
#   - name: order_total
#     money:
#       transport_locale:
#         decimal_separator: "."
#         thousands_separator: none
#         currency_symbols: none
# """
# print(yaml.safe_load(snippet))   # parse được thì mới dán vào amendment
snippet1 = """
  - name: order_total
    money:
      transport_locale:
        decimal_separator: "."
        thousands_separator: none
        currency_symbols: none
"""
print(yaml.safe_load(snippet1))

snippet2 = """
  - name: updated_at
    type: TIMESTAMP
    timezone: UTC
    semantics: event_time
    nullable: false
    comment: >
      Thời điểm bản ghi thay đổi tại OLTP nguồn, theo UTC.
      Không phải thời điểm chạy job xuất file.
"""
print(yaml.safe_load(snippet2))

# %% ── Task 12 — PII: điều khoản không thể nằm trên giấy ───────────────
section("TASK 12a — đo NĂNG LỰC trước khi phá huỷ cột")

# Một gap trong danh sách Task 3 KHÁC VỀ BẢN CHẤT với mọi cái còn lại. Feed
# customers mang `email` — PII. Mọi sự im lặng khác gây ra SỐ SAI; cái này gây ra
# THÔNG BÁO VI PHẠM DỮ LIỆU. Và nhìn vào warehouse của chính bạn: core.customers
# đã lưu email thô từ A03. KHÔNG AI QUYẾT ĐỊNH điều đó — không điều khoản nào cho
# phép, không điều khoản nào cấm, nên warehouse trở thành một kho PII DO TAI NẠN.
#
# Điều khoản hứa HAI thứ — phơi nhiễm biến mất, NĂNG LỰC được giữ lại — nên phải
# ĐO năng lực TRƯỚC khi phá huỷ cột, nếu không bạn không chứng minh được là đã giữ.

con.sql("""
    SELECT count(email) - count(DISTINCT email) AS dup_email_rows
    FROM core.customers
""").show()      # small: 180

# %%
section("TASK 12b — dựng lại core.customers")

con.execute("""
CREATE OR REPLACE TABLE core.customers AS
SELECT customer_id, name,
       md5(lower(trim(email))) AS email_hash,   -- CHUẨN HOÁ BÊN TRONG phép hash
       country, city, signup_ts, is_active,
       _data_date, _run_id                      -- hai cột lineage §7.2
FROM core.customers;
""")
# VÌ SAO md5(lower(trim(email))) chứ không phải md5(email)?
# Biểu thức này GIỜ LÀ MỘT ĐIỀU KHOẢN CONTRACT — nó không được phép phụ thuộc vào
# việc A03 mãi mãi làm sạch ở thượng nguồn. Và md5('John@X.com') != md5('john@x.com'):
# một hash chưa chuẩn hoá ÂM THẦM đánh mất chính cái năng lực dedupe mà điều khoản
# hứa hẹn. Hash dạng chuẩn, luôn luôn, TẠI NƠI hash.

# %%
con.sql("DESCRIBE core.customers").show()
# Nửa lời hứa 1 — PHƠI NHIỄM BIẾN MẤT: email_hash VARCHAR ở vị trí thứ ba,
# không có `email` ở bất cứ đâu.

con.sql("""
    SELECT count(email_hash) - count(DISTINCT email_hash) AS dup_hash_rows
    FROM core.customers
""").show()      # small: 180 — Y HỆT
# Nửa lời hứa 2 — NĂNG LỰC ĐƯỢC GIỮ: hash mang ĐÚNG tín hiệu trùng lặp mà cột thô
# đã mang.

con.sql("""
    SELECT email_hash, list(customer_id) AS shares_email
    FROM core.customers
    GROUP BY email_hash HAVING count(*) > 1
    LIMIT 3
""").show()
# Và tín hiệu vẫn DÙNG ĐƯỢC: các cặp chung email mà A03 tìm ra, bạn vẫn tìm ra —
# đúng hai customer_id trong mỗi list.

# %%
# Chạy lại phép dựng lần thứ hai. Nó HỎNG:
#   Binder Error: Referenced column "email" not found
# Và lỗi đó CHÍNH LÀ điều khoản đang HOẠT ĐỘNG: không còn email thô nào để hash
# lại, hoặc để rò rỉ.
#
# HASHING một chiều (md5 ở đây): đẳng thức sống sót — join và dedupe vẫn chạy —
#   nhưng giá trị không đi ngược lại được.
# MASKING giữ HÌNH DẠNG cho mắt người; giá trị đã che không bao giờ so sánh bằng.
# TOKENIZATION thay bằng token mà chỉ một vault khoá chặt đảo ngược được, khi bản
#   gốc PHẢI quay lại.
# Quy tắc ngón tay cái: hash để phân tích, mask để hiển thị, tokenize để đảo ngược.
#
# Lặp lại phần b trên warehouse FULL: 1.200.000 dòng, dup count 3.594 trước và sau.
#
# Điều khoản này còn có TƯƠNG LAI: ở A15, stg_shopcore__customers tính chính
# email_hash đó thẳng từ feed thô và KHÔNG BAO GIỜ select cột thô — điều khoản
# được cưỡng chế BẰNG CÁCH XÂY DỰNG trong mỗi lần dbt rebuild.
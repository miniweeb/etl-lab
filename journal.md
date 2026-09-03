# Lab journal

## A00 — Setup & data generation

### Environment
- repo `D:\ETL_SAMPLE`, data `D:\etl_lab` (ổ D local, không sync)
- Python 3.13.13 · duckdb 1.5.5 · git 2.50.1 · 15.6 GB RAM · 8 cores
- Biến môi trường: `ETL_LAB_DATA` tách rời source code và dữ liệu vật lý
- Ổ C chỉ còn 14 GB trống → bắt buộc chuyển data và spill/temp sang D

### Numbers
- Generate full month 1: ~15 phút, 4 workers
- Raw: 15.7 GB orders (45 files) + ~119 MB dims
- Small: 45 files, ~800 MB
- Selftest PASS cả hai scale: kiểm định đủ 34 tiêu chí schema và ràng buộc

### 10 columns (month 1, era v1)
`order_id · customer_id · store_id · order_ts · updated_at · status · payment_method · order_total · items · meta`

### New knowledge & Core insights
- **Manifest là bản tự khai của producer**: Producer khai trước số dòng lỗi (`dup_rows`, `late_rows`, `corrupt_rows`). Quá trình verify chuyển từ việc phỏng đoán cảm tính sang đối chiếu nhị phân (khớp hoặc không khớp).
- **Phân định bộ nhớ và rủi ro tràn ổ đĩa**: Engine xử lý dữ liệu khi chạy truy vấn lớn sẽ spill dữ liệu tạm ra đĩa. Tách riêng thư mục temp sang ổ đĩa dữ liệu (ổ D) là điều kiện bắt buộc để bảo vệ hệ điều hành.
- **Ràng buộc ngầm định trong Contract**: Triết lý *"that silence is load-bearing"* — những gì Data Contract không đề cập (như timezone của `updated_at` hay tính tùy chọn của `utm`) cũng có giá trị định hình thiết kế tương đương những điều khoản tường minh.

### Observations to carry forward
- `items` escape theo chuẩn RFC 4180: field bọc `"..."`, mỗi `"` bên trong nhân đôi thành `""`. Phải để parser CSV chuẩn tự xử lý, không unescape bằng tay.
- `customer_id` bị bọc ngoặc kép trong khi `order_id` thì không → không nhất quán kiểu dữ liệu ngay trong cùng một file.
- `meta` có bản ghi thiếu hẳn key `utm` → contract quy định `utm` là trường tùy chọn (optional).
- Manifest ghi `"era": "v1"` → báo hiệu sẽ có schema era khác ở các giai đoạn sau.

---

## A01 — First contact: khảo sát CSV, load 1 ngày ra Parquet

### Numbers
- 45 files, 15.7 GB (tháng 1); ngày flash-sale (`2026-06-19`, `2026-07-11`) phình to gấp ~3× ngày thường.
- Ngày khảo sát: `2026-06-02` (manifest: 1,180,490 rows / 359 MB).
- Đối soát count: khớp chính xác 100% manifest ở cả 2 scale và qua cả auto-detect lẫn explicit schema.
- Tỷ lệ dữ liệu bẩn đo bằng `TRY_CAST ... IS NULL`: `order_ts` ~0.4% · `order_total` ~0.7% · `customer_id` dạng float ~0.2%.
- Nén Parquet: 343 MB CSV rút xuống còn 92 MB (~3.7×), tổ chức thành 10 row groups.

### Timings (second run — so sánh warm cache)
| Query | CSV | Parquet | Tối ưu |
|---|---|---|---|
| `count(*)` | ~360 ms | **1 ms** | ~360× (đọc metadata footer, không quét data) |
| Filter `store_id` | ~360 ms | **3 ms** | ~120× (column projection + min/max pushdown) |
| Group by `status` | ~390 ms | **5 ms** | ~78× (chỉ đọc cột status, không parse text) |

- Bộ nhớ: Pandas nạp 1 ngày tốn ~6s và RSS +0.67 GB (~2.2× file size); DuckDB tốn 0.4s và RSS chỉ +0.02 GB.
- Ngoại suy 45 files: Pandas đòi hỏi ~34 GB RAM (vượt ngưỡng 15.6 GB RAM vật lý), DuckDB chỉ giữ mức +20 MB nhờ streaming execution.

### New knowledge & Core insights
- **Cạm bẫy auto-detect từ tỷ lệ mẫu nhỏ**: Chỉ cần 0.2% bản ghi `customer_id` ở dạng float `"47722.0"` (do xuất qua Excel), engine đã ép toàn bộ cột về kiểu DOUBLE. Một tỷ lệ nhiễu cực nhỏ quyết định kiểu của 100% cột dữ liệu.
- **Sức mạnh ba tầng của Parquet so với CSV**:
  1. *Typed*: Schema lưu sẵn trong metadata, parse nhị phân 1 lần duy nhất; CSV phải parse lại toàn bộ ký tự mỗi lần query.
  2. *Columnar (Column Projection)*: Chỉ nạp đúng các cột cần thiết trong truy vấn.
  3. *Metadata & Statistics*: `count(*)` lấy thẳng từ file footer; min/max thống kê ở row group cho phép bỏ qua (skip) toàn bộ trang dữ liệu không khớp.
- **Bản chất nghẽn cổ chai CPU của CSV**: CSV bị giới hạn bởi tốc độ phân tích cú pháp ký tự (CPU parse), không phải do I/O đĩa. Đó là lý do warm cache không cải thiện tốc độ của CSV nhưng lại tăng tốc Parquet vượt trội.
- **Thực tế về Resident Set Size (RSS)**: Bộ nhớ thực tế mà OS cấp phát (`RSS`) mới là nguyên nhân gây sập máy, luôn lớn hơn nhiều so với `df.memory_usage()` do bao gồm chi phí buffer, Python runtime và interpreter.
- **Quy luật của dữ liệu bẩn**: Data bẩn không phải nhiễu ngẫu nhiên mà có cấu trúc lặp lại ổn định (dấu phẩy EU, định dạng ngày đa dạng, timezone ngầm). Tỷ lệ lỗi giữ nguyên khi scale gấp 20 lần.

### Observations to carry forward
- Khai báo explicit schema là yêu cầu sống còn khi đọc dữ liệu thô để ngăn chặn sai lệch kiểu.
- Thận trọng tuyệt đối với dấu phẩy thập phân kiểu châu Âu: xóa nhầm dấu phẩy sẽ khiến giá trị sai lệch 100 lần mà không hề báo lỗi cú pháp.
- Dữ liệu trong file ngày D luôn có đơn hàng của ngày hôm trước do độ trễ hệ sinh thái (`late_rows`).

---

## A02 — Hive partitioning & Late-arriving data

### Numbers
- Layout: Cấu trúc Hive phân vùng theo `order_date=YYYY-MM-DD/`.
- Late-arriving data: Khoảng 1.5% bản ghi trong file ngày D thuộc về các ngày trước đó (Event Time khác Arrival Time).
- Một file nạp có thể phân tán dữ liệu vào tối đa 8 thư mục phân vùng khác nhau.
- EXPLAIN plan thực tế:
  - Lọc trên partition key (`WHERE order_date = '2026-06-19'`): Quét 8/377 files (10.5 ms) nhờ Partition Pruning.
  - Lọc trên cột dữ liệu (`WHERE CAST(order_ts AS DATE) = '2026-06-19'`): Quét toàn bộ 377/377 files (31.6 ms).

### Incident & Bug analysis
- Sự cố mất dữ liệu âm thầm: Chế độ ghi `OVERWRITE_OR_IGNORE` khi nạp file ngày sau đã ghi đè và xóa sạch 60,686 dòng của ngày trước, chỉ giữ lại 885 dòng bổ sung muộn mà không hề báo lỗi.
- Khắc phục: Chuyển toàn bộ sang cơ chế `APPEND` kèm UUID định danh duy nhất cho từng file nạp.

### New knowledge & Core insights
- **Cơ chế Partition Pruning cấp thư mục**: Bằng cách lưu trữ dạng `key=value/`, query engine chỉ cần đọc danh sách đường dẫn thư mục (directory listing) để loại trừ triệt để dữ liệu không liên quan mà không cần giải mã bất kỳ file Parquet nào.
- **Xung đột giữa Event Time và Arrival Time**: Ngày gửi file (Arrival Time) không đồng nghĩa với ngày phát sinh đơn hàng (Event Time). Xử lý dữ liệu buộc phải bóc tách dựa trên mốc nghiệp vụ thực tế.
- **Nguy cơ xóa sổ dữ liệu từ cơ chế Overwrite cục bộ**: Khi ghi đè thư mục theo mốc phân vùng, các bản ghi đến muộn (`late_rows`) sẽ làm biến mất dữ liệu lịch sử đã nạp trước đó nếu không dùng kiến trúc append an toàn.
- **Ghi phân tán đa phân vùng (Dynamic Partitioning)**: Một batch xử lý duy nhất có thể stream và ghi song song vào nhiều thư mục vật lý khác nhau tùy thuộc vào giá trị partition key của từng dòng.

### Observations to carry forward
- Phân vùng theo thư mục giúp loại bỏ đến 98% dung lượng đọc không cần thiết qua Partition Pruning.
- Tuyệt đối không dùng chế độ overwrite thư mục đối với các nguồn dữ liệu có độ trễ hoặc cập nhật muộn.
- Luôn đặt tên file nạp chứa định danh duy nhất (UUID) để tránh ghi đè ngầm định.

---

## A03 — Layered architecture & 13 Cleaning rules

### Numbers
- Mô hình phân tầng: Raw (VARCHAR nguyên bản) → Staging (sạch, 1-1 với raw, khử trùng trong file) → Core (Facts/Dims, mô hình hóa) → Ops (audit, DQ report, alert).
- Tỷ lệ làm sạch thực tế trên 13 rules:
  - `customer_id_float_fixed`: 120 dòng (0.2%) — ép kiểu 2 bước VARCHAR → DOUBLE → BIGINT.
  - `ts_ddmmyyyy_fixed` / `ts_isot_fixed`: 395 dòng (0.7%) — dùng `try_strptime` theo danh sách format ưu tiên.
  - `total_decimal_comma_fixed`: 187 dòng (0.3%) — gỡ dấu chấm nghìn, thay phẩy bằng chấm.
  - `total_negative_kept`: 52 dòng (0.1%) — giữ nguyên giá trị âm (làm sạch không phán xét nghiệp vụ).
  - `status_recased` / `status_junk_to_null`: 145 dòng (0.3%) — chuẩn hóa hoa/thường, rác ngoài whitelist đưa về NULL.
  - `country_conformance`: 548 dòng NULL (~1%) — chuẩn hóa 24 cách viết về 10 mã ISO thông qua mapping table.
  - `dup_order_id_removed`: 31 dòng (0.05%) — khử trùng lặp nội bộ file bằng `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1`.

### Architecture & Lineage
- Quy chuẩn 2 cột lineage bắt buộc:
  - `_data_date` (DATE): ngày nghiệp vụ của lô dữ liệu, dùng làm partition và mốc kiểm tra độ tươi.
  - `_run_id` (BIGINT): định danh phiên chạy để truy vết audit log trong `ops.etl_runs`.

### New knowledge & Core insights
- **Nguyên tắc khoanh vùng trách nhiệm (Isolating Blame)**: Phân chia các tầng kiến trúc để định vị lỗi: sai ở Raw do vendor, sai ở Staging do rules làm sạch, sai ở Core do logic joins/tổng hợp.
- **Ranh giới cốt lõi: Cleansing vs. Validation**:
  - *Cleansing*: Chuẩn hóa hình thức biểu diễn (sửa format ngày, ép kiểu an toàn, xóa ký tự thừa).
  - *Validation*: Phán xét tính đúng đắn nghiệp vụ (tổng tiền âm, trạng thái không hợp lệ). Cleansing không được phép tự ý sửa hoặc xóa dữ liệu nghiệp vụ sai mà phải giữ nguyên để Validation xử lý.
- **Kỹ thuật ép kiểu số thực về nguyên**: VARCHAR không thể ép trực tiếp sang BIGINT nếu chuỗi chứa dấu chấm thập phân (`"123.0"`). Bắt buộc phải qua 2 bước: VARCHAR → DOUBLE → BIGINT.
- **Thiết kế Lineage tinh gọn**: Chỉ gắn `_data_date` và `_run_id` vào từng dòng dữ liệu. Các metadata động như thời gian nạp, tên file nguồn thuộc về bảng quản trị `ops.etl_runs`, không nhồi nhét vào bảng dữ liệu chính.
- **Thay thế biểu thức CASE bằng Dimension Mapping**: Dùng bảng mapping để chuẩn hóa các biến thể văn bản (như mã quốc gia) thay vì lồng hàng chục lệnh CASE WHEN trong SQL.

### Observations to carry forward
- Đóng gói toàn bộ 13 cleaning rules vào module dùng chung `a03_cleaners.py` có unit test đi kèm để bảo đảm tính toàn vẹn khi tái sử dụng.
- Dữ liệu lỗi ngoài Whitelist quy về NULL thay vì tự suy đoán giá trị thay thế.

---

## A04 — Complex types & Grain change

### Numbers
- Cấu trúc phức hợp: Chuyển mảng JSON sang `LIST<STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)>`.
- Strict vs. Lenient: Ép kiểu chặt (Strict) cho `items` để chặn lỗi cấu trúc sớm; ép kiểu lỏng (Lenient) qua `json_transform` cho `meta` do các key UTM là tùy chọn.
- Xử lý 3 trạng thái rỗng trong JSON: key mang `null`, key bị thiếu, và chuỗi sentinel (`"none"`) → quy về 1 giá trị `SQL NULL` chuẩn thông qua `->>` và `NULLIF()`.
- Thay đổi độ hạt (Grain Change) qua `UNNEST()`:
  - Từ cấp Đơn hàng (Order Grain): 59,024 dòng.
  - Sang cấp Mặt hàng (Item Grain): 149,190 dòng trong `core.order_items`.
- Phép kiểm tra bảo toàn (Conservation Check): Đảm bảo số dòng `core.order_items` bằng chính xác `sum(json_array_length(items))`.

### New knowledge & Core insights
- **Kiểu dữ liệu lồng nhau nguyên bản (Native Nested Types)**: DuckDB xử lý `LIST` và `STRUCT` trực tiếp trong engine mà không cần tuần tự hóa (serialize/deserialize) lại thành JSON string, giúp tiết kiệm bộ nhớ và tối ưu I/O.
- **Chiến lược Strict Cast vs. Lenient Cast**:
  - Dữ liệu mang tính bắt buộc và là xương sống nghiệp vụ (`items`): Dùng ép kiểu chặt để bắt lỗi schema ngay lập tức.
  - Dữ liệu metadata tùy chọn, thay đổi tự do (`meta`): Dùng trích xuất lỏng để tránh sập pipeline khi xuất hiện cấu trúc lạ.
- **Ba sắc thái của giá trị rỗng trong dữ liệu bán cấu trúc**: Key chứa JSON null (`{"k": null}`), key hoàn toàn vắng mặt (`{}`), và key chứa chuỗi đại diện sentinel (`{"k": "none"}`). Cần kết hợp toán tử trích xuất văn bản `->>` và `NULLIF()` để đồng nhất về SQL NULL.
- **Cạm bẫy nhân doanh thu (Fan-out trap)**: Khi làm mịn độ hạt từ Order sang Line Item bằng `UNNEST()`, một đơn hàng có $N$ mặt hàng sẽ sinh ra $N$ dòng. Nếu JOIN bảng chi tiết ngược lại bảng cha rồi dùng `SUM(order_total)` thì giá trị doanh thu bị nhân lên gấp nhiều lần.
- **Phép kiểm tra bảo toàn dữ liệu (Conservation Check)**: Khi thay đổi độ hạt dữ liệu, việc so sánh tổng số phần tử mảng ở bảng nguồn và số dòng sau khi bung bảng đích là chốt chặn xác minh tính toàn vẹn.

### Observations to carry forward
- Không bao giờ JOIN ngược bảng con về bảng cha để cộng dồn các chỉ số của bảng cha.
- Luôn kiểm định số dòng bung ra từ mảng bằng hàm kiểm tra bảo toàn trước khi nạp vào kho.

---

## A05 — Data quality gate: split load & quarantine

### Numbers
- Contract tolerances (v1.3.0): `bad_status`: 1.0% · `bad_order_ts`: 0.5% · `null_total`: 1.0% · `orphan_customer`: 0.5% · `orphan_sku`: 1.0% · `total_mismatch`: 1.0%.
- Small (2026-06-02):
  - Manifest: 59,024 rows (`late_rows`: 885, `dup_rows`: 29).
  - Staging: 59,024 rows.
  - Flagged/Bad: 992 rows (1.68%) → Core: 58,032 rows, Quarantined: 992 rows. Gate: **PASS**.
  - Top reject reasons: `orphan_sku`: 310 (0.525%), `total_mismatch`: 308 (0.522%), `null_total`: 117 (0.198%), `null_customer`: 94, `bad_status`: 71.
- Validation 3 ngày (Full scale - A05 Validate Day):
  - `2026-06-02`: staged 1,180,490 → core 1,160,764 | bad 19,726 (1.67%) [15.6s]
  - `2026-06-19`: staged 3,643,024 → core 3,582,451 | bad 60,573 (1.66%) [45.5s]
  - `2026-07-11`: staged 3,602,310 → core 3,542,646 | bad 59,664 (1.66%) [38.4s]
  - Tổng nạp lũy kế: `core.orders` = 8,285,861 rows; `ops.dq_report` = 48 records (16 checks × 3 ngày).
- Pandera profiling (Task 12):
  - Đối soát khớp chính xác: `status` hỏng (35 isin + 36 null = 71 `bad_status`); `order_total` hỏng (52 negative + 117 null = 169).
  - Chi phí bộ nhớ: 3 cột/1 ngày small tốn 1.9 MB trong pandas.

### Gate testing & Freshness alert (Task 9 & 10)
- Freshness breach: Khi nâng kỳ vọng ngày (`FRESH_EXPECT="2026-07-12"` trong khi `max(_data_date)="2026-07-11"`), hệ thống báo lỗi severity `error`, exit code `1`, ghi nhận vào `ops.alerts`.
- Hard fail gate simulation: Giả lập siết tolerance `total_mismatch` xuống 0.1%.
  - Vi phạm: ngày `2026-06-19` có `total_mismatch` là 0.522% (> 0.1%).
  - Cơ chế nguyên tử: chặn toàn bộ đợt load (`Nothing loaded to core`); count `core.orders` giữ nguyên 414,095 → 414,095.

### New knowledge & Core insights
- **Triết lý Quản trị Chất lượng Dữ liệu hiện đại**: Dữ liệu sản xuất luôn có tỷ lệ bẩn nhất định (~1.7%). Giải pháp không phải là dừng toàn bộ hệ thống vì một vài dòng lỗi, mà là: nạp dữ liệu sạch vào Core, cách ly dữ liệu lỗi vào vùng kiểm dịch (Quarantine) và chỉ kích hoạt Hard Fail khi tỷ lệ lỗi vượt ngưỡng thỏa thuận trong Contract.
- **Thứ tự thực thi bất biến của Quality Gate**:
  1. Gắn cờ lỗi trên toàn bộ bản ghi (Flagging).
  2. Đẩy các bản ghi lỗi sang vùng cách ly (Quarantine Parquet) lưu đầy đủ lý do và metadata.
  3. Ghi nhận số liệu vào bảng kiểm toán `ops.dq_report` (kể cả check đạt 0 lỗi).
  4. Đánh giá ngưỡng Tolerance. Nếu vi phạm → Kích hoạt dừng tải khẩn cấp.
  5. Nạp các bản ghi sạch vào `core.orders`.
- **Phân vùng Quarantine theo thuộc tính nghiệp vụ thay vì ngày nạp**: Khi cách ly dữ liệu lỗi từ file nạp ngày D, các đơn lỗi phát sinh từ ngày hôm trước (`late_rows`) sẽ được tự động gom vào thư mục phân vùng ngày hôm trước kèm tiền tố đánh dấu nguồn gốc (ví dụ `from_2026-06-02_0.parquet`).
- **Kiểm định độ tươi mới bằng que thăm (Freshness Probe)**: Đo lường độ trễ dữ liệu thông qua hàm `MAX(_data_date)` trên bảng đích để kịp thời cảnh báo nguy cơ hệ thống ngừng nhận dữ liệu mới (Stale data) so với cam kết SLA.
- **Đối soát đa chiều với Pandera**: Kết hợp kiểm định SQL hàng loạt trong DB với các thư viện schema validation như Pandera ở tầng Python để phân tích chi tiết failure cases và bảo đảm tính đồng nhất giữa hai góc nhìn kiểm thử.

### Observations to carry forward
- Split load bảo vệ Core: Tách dòng lỗi đẩy sang quarantine Parquet theo cơ chế append, không bao giờ để dòng hỏng lọt vào `core`.
- Tỷ lệ vi phạm đồng nhất: Tỷ lệ bản ghi bị quarantine giữ ổn định ở mức ~1.66% - 1.72% xuyên suốt từ Small đến Full và qua các đợt flash-sale.
- Module hóa: Hàm `validate_day` gói gọn logic nạp, kiểm định và split load, sẵn sàng tích hợp trực tiếp vào pipeline và runner tự động.
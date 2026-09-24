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

## A06 — Data contracts

### Task 1 — Per-column contract interrogation (10 câu hỏi nghiệp vụ)

1. **order_total tiền tệ gì, ai làm tròn, làm tròn thế nào?**
   - USD, đơn tệ theo hợp đồng; scale 2, làm tròn half-up do **Producer** thực hiện trước khi export. Warehouse lưu trữ nguyên trạng, **tuyệt đối không làm tròn lại**.
   - *Điều khoản:* `schema.order_total.money`.

2. **Một khoản hoàn tiền (refund) về mang dấu gì?**
   - Số tiền mang **dấu dương**. Bản chất âm thể hiện qua trạng thái `status: refunded`. Phép tính doanh thu `SUM(order_total)` ngây thơ sẽ thổi phồng số liệu nếu bỏ qua status.
   - *Điều khoản:* `schema.order_total.money.negative_allowed` & `business_rules.refunds`.

3. **order_ts timezone gì, ai đã chuyển đổi, ai xử lý DST?**
   - Timezone **UTC** (chuỗi `'YYYY-MM-DD HH:MM:SS'`, không offset). **Producer** chuyển đổi từ giờ địa phương cửa hàng sang UTC và tự xử lý bù/trừ giờ mùa hè (DST) trước khi xuất file.
   - *Điều khoản:* `schema.order_ts.timezone`.

4. **Finance muốn báo cáo theo giờ địa phương của store. Việc chuyển đổi diễn ra ở đâu — và vì sao contract CẤM chuyển đổi lúc ingest?**
   - Diễn ra ở tầng **Marts / Reporting downstream** bằng cách join bảng dimension `stores`.
   - Cấm lúc ingest vì một fact thô UTC phục vụ được **mọi** mart vùng miền; nếu convert ngay lúc ingest thì fact đó chỉ phục vụ được duy nhất một mục đích cục bộ.
   - *Điều khoản:* `schema.order_ts.warehouse_policy`.

5. **customer_id về NULL: mất dữ liệu, bug của producer, hay mang ý nghĩa?**
   - Mang **ý nghĩa nghiệp vụ**: Khách vãng lai mua hàng không đăng nhập tài khoản (Guest checkout). Không phải mất dữ liệu hay lỗi.
   - *Điều khoản:* `schema.customer_id.nullable` & `schema.customer_id.comment`.

6. **order_id có duy nhất không? Duy nhất trong phạm vi nào?**
   - Duy nhất trên **từng đơn hàng ngoài đời thực**. **Không duy nhất trong một file hoặc trong một ngày** do cơ chế bản sửa cập nhật trạng thái (in-file corrections / re-sends).
   - *Điều khoản:* `schema.order_id.unique`.

7. **Hai cột nào quyết định bản sao nào của một đơn gửi lại sẽ THẮNG?**
   - Cặp `(order_id, updated_at)`. Bản ghi có giá trị `updated_at` lớn hơn sẽ thắng (cơ chế latest-wins by value).
   - *Điều khoản:* `late_corrections.semantics` & `quality.duplicates.latest_wins_key`.

8. **File ngày D được với ngược về quá khứ bao xa? Nạp file D có thể chạm vào những phân vùng lake nào?**
   - Về quá khứ tối đa **7 ngày**. Quá trình nạp file D có thể tác động tới các partition lake từ `order_date = D - 7 ngày` đến `D`.
   - *Điều khoản:* `late_corrections.window_days` & `delivery.business_date`.

9. **Làm sao biết một lần giao hàng là HOÀN TẤT — hai con số nào phải đối chiếu ở MỌI lần nạp?**
   - Nhận biết qua file manifest JSON đi kèm (`raw/manifest/orders_YYYY-MM-DD.json`).
   - Hai con số đối chiếu bắt buộc: `loaded_rows == manifest.rows` và `bytes on disk == manifest.bytes`.
   - *Điều khoản:* `delivery.completeness_signal` & `quality.reconciliation`.

10. **Vào lúc mấy giờ thì một file thiếu không còn là buổi sáng chậm chạp của bạn mà trở thành SỰ CỐ của shopcore?**
    - Đúng **04:00 UTC**. (File dự kiến xong lúc 02:00 UTC, trễ quá 04:00 UTC vi phạm cam kết SLA và chính thức thành incident của Producer).
    - *Điều khoản:* `delivery.freshness_sla`.

### Task 2 — Violation vs Gap vs Tolerated Classification

#### Numbers (Quarantine tổng hợp 3 ngày: ~421,293 dòng đã stage)
- Tổng dòng kiểm dịch (Quarantine): 7,198 dòng (~1.71%).
- Phân rã theo từng mã lỗi (một dòng có thể dính nhiều lỗi đồng thời):

| Reject reason | Rows | Tỷ lệ | Ngưỡng Contract | Phân loại | Bản chất & Hướng xử lý |
|---|---|---|---|---|---|
| `negative_total` | 423 | 0.10% | 0.0% (Không có) | **VIOLATION** | Vi phạm `money.negative_allowed: false`. Không có tolerance -> Báo cáo Producer sửa từ gốc. |
| `total_mismatch` | 2,190 | 0.52% | 1.0% | **VIOLATION** | Vi phạm cross-field rule. Dưới ngưỡng chịu đựng -> Ghi bằng chứng, nạp phần sạch, báo cáo định kỳ. |
| `orphan_sku` | 2,172 | 0.52% | 1.0% | **VIOLATION** | Vi phạm tham chiếu SKU sang product dim. Dưới ngưỡng chịu đựng. |
| `null_total` | 901 | 0.21% | 1.0% | **VIOLATION** | Vi phạm `order_total.nullable: false`. Dưới ngưỡng chịu đựng. |
| `bad_status` | 616 | 0.15% | 1.0% | **VIOLATION** | Vi phạm whitelist enum và lowercase. Dưới ngưỡng chịu đựng. |
| `orphan_customer` | 214 | 0.05% | 0.5% | **VIOLATION** | Vi phạm tham chiếu khách hàng có đăng ký tài khoản. Dưới ngưỡng chịu đựng. |
| `bad_order_ts` | 88 | 0.02% | 0.5% | **VIOLATION** | Vi phạm format thời gian (hoàn toàn unparseable). Dưới ngưỡng chịu đựng. |
| `null_customer` | 632 | 0.15% | — | **BUG PHÍA CONSUMER** | `customer_id` có `nullable: true` (Guest checkout). Pipeline A05 cách ly nhầm dữ liệu hợp lệ -> Khắc phục ở A07. |
| `dup_order_id` | ~210 | ~0.05% | 0.2% | **TOLERATED** | Đơn hàng lặp nội bộ file. Hợp đồng cho phép tường minh như cái giá của cơ chế at-least-once delivery. |

#### New knowledge & Core insights (Task 2)
- **Ranh giới khái niệm giữa Tolerated và Violation within tolerance**:
  - *Tolerated*: Là thứ hợp đồng **cho phép xảy ra** như một đặc tính kỹ thuật bình thường (chỉ có duy nhất `within_file_exact_resends` <= 0.2%). Không phải là lỗi.
  - *Violation within tolerance*: Là hành vi **phá vỡ cam kết** (tiền âm, sai enum, lệch tổng). Các mức 0.5% hay 1.0% trong `row_level_tolerances` chỉ là *ngưỡng chịu đựng (hard-fail threshold)* để pipeline không bị sập vì rác thông thường. Chúng vẫn là vi phạm và bắt buộc phải đếm để gửi sang Producer hàng tuần theo điều khoản `slo_reporting`.
- **Trách nhiệm quy kết (Accountability)**:
  - `negative_total` là vi phạm không có ngân sách khoan nhượng (zero-tolerance). 423 dòng này là lỗi thuần túy của Shopcore cần mở incident.
  - `null_customer` chứng minh rằng không phải mọi dòng rơi vào quarantine đều là lỗi của Producer; viết check sai so với hợp đồng sẽ biến Consumer thành bên gây gián đoạn dữ liệu hợp lệ.

---

### Task 3 — Săn khoảng trống hợp đồng (The Gap Hunt)

*Khoảng trống (Gap) là những thứ contract IM LẶNG — không cấm, không cho phép. Không thể đổ lỗi cho Producer về một cam kết họ chưa từng đưa ra. Nếu Consumer không xử lý tường minh, Gap sẽ âm thầm phá hủy pipeline. Bảng dưới đây là hạt giống để soạn thảo Amendment v1.4.0 ở Task 11 & Task 12.*

| Sự im lặng (Contract KHÔNG nói gì) | Nó cắn thế nào (Rủi ro & Hậu quả cụ thể) | Điều khoản sẽ khép nó lại (Seed for v1.4.0) |
|---|---|---|
| **1. Múi giờ & Ngữ nghĩa của `updated_at`** *(Seeded `# NOTE`)* | Không ghi rõ timezone (UTC hay local?), không rõ là thời điểm xảy ra sự kiện (event time) hay lúc quét xuất file batch. Cột này dùng làm khóa dedupe (`latest-wins`), nếu producer đổi server lệch múi giờ thì bản ghi cũ sẽ đè bản ghi mới. CSV sniffer còn tự đoán thành `TIMESTAMP WITH TIME ZONE`. | `schema.updated_at.timezone: UTC` và quy định rõ `semantics: event_time` (mốc thời gian ghi nhận thay đổi tại OLTP DB). |
| **2. Kiểu dữ liệu nội hàm & Độ dài mảng `items`** *(Seeded `# NOTE`)* | Từ khóa `"number"` không ghim rõ là integer hay float, không cấm dạng chuỗi `"qty": "2"`. Không giới hạn số phần tử mảng (nguy cơ mảng khổng lồ gây tràn RAM khi `UNNEST`), không có quy ước giá trị rỗng cho trường tùy chọn. | Bổ sung `schema.items.shape` chi tiết: ghim rõ `qty: integer`, `unit_price: decimal(14,2)`, giới hạn `max_items: 50`. |
| **3. Transport Locale của tiền tệ (`order_total`)** | Contract ghim `currency: USD` nhưng im lặng về ký tự phân cách (transport locale). Producer gửi dấu phẩy châu Âu (`93,32` hoặc `1.234,56`). Nếu ép kiểu trực tiếp sẽ lỗi cú pháp; nếu xóa nhầm dấu phẩy sẽ khiến giá trị sai lệch 100 lần. | `schema.order_total.money.transport_locale`: quy định rõ `decimal_separator: "."`, `thousands_separator: none`, `currency_symbols: none`. |
| **4. Chính sách bảo vệ dữ liệu định danh cá nhân (PII) & Retention** | Feed `customers` mang cột `email` — thông tin định danh cá nhân (PII). Hợp đồng không cho phép cũng không cấm, khiến `core.customers` vô tình trở thành kho lưu email thô suốt từ A03, đối mặt rủi ro vi phạm bảo mật dữ liệu. | Bổ sung khối cấp cao `data_classification.pii`: cấm lưu email thô trong warehouse, bắt buộc băm một chiều `email_hash: md5(lower(trim(email)))` ngay tại ingest. |
| **5. Cơ chế công bố lại dữ liệu quá khứ (Restatement Policy)** | `delivery.freshness_sla` tự thừa nhận v1.3.0 chưa có điều khoản restatement. Nếu Producer âm thầm tạo lại và xuất đè file của 3 ngày trước thì Consumer không nhận được cảnh báo, gây lệch số liệu lịch sử trên các dashboard đã chốt. | Bổ sung điều khoản `delivery.restatement_policy`: yêu cầu thông báo trước qua `#shopcore-data-changes` kèm cờ nhận diện khi xuất lại file cũ hơn 2 ngày. |
| **6. Cách biểu diễn giá trị rỗng của metadata (`meta.utm`)** | `utm` được khai báo là tùy chọn (`string?`) nhưng không quy định hình thức rỗng. Dữ liệu thực tế lúc thì thiếu key, lúc mang `null`, lúc mang chuỗi `"none"`, buộc Consumer phải viết logic xử lý chắp vá ở A04. | `schema.meta.null_representation: absent_key_or_null` (chuẩn hóa cách biểu diễn giá trị rỗng của các trường JSON tùy chọn). |

#### Core insights (Task 3)
- **Sự nguy hiểm của khoảng trống**: Một vi phạm (Violation) thì rõ ràng và có thể phạt bằng cách từ chối/báo cáo. Một khoảng trống (Gap) thì không ai chịu trách nhiệm. Khi dữ liệu gây lỗi pipeline, Consumer không thể bắt đền Producer vì họ chưa từng hứa.
- **Con đường đóng khoảng trống**: Xử lý tường minh trong pipeline của mình (như hàm clean regex tiền tệ ở A03), đồng thời chuyển hóa nó thành đề xuất sửa đổi hợp đồng (Proposed Amendment) để chính thức hóa trách nhiệm giữa hai bên.

### Task 4 — Bốn check tự viết cho gate

| Check | Điều kiện dòng xấu | Tolerance |
|---|---|---|
| `customer_id_not_castable` | `IS NOT NULL AND TRY_CAST(... AS BIGINT) IS NULL` | 0 |
| `store_id_not_castable` | `IS NULL OR TRY_CAST(... AS INTEGER) IS NULL` | 0 |
| `unparseable_order_total` | `IS NULL OR NOT regexp_matches(..., '^[0-9]+\.[0-9]{2}$')` | 1% |
| Tầng 5 — cửa sổ sửa trễ | `min_date < file_date - 7` hoặc `max_date > file_date` | — |

**Bẫy `customer_id`**: cột này `nullable: true`, NULL nghĩa là khách vãng lai chứ
không phải rác. Nên phải viết `IS NOT NULL AND`, ngược hẳn với `order_id`. Cùng
cái bẫy đã gặp ở check orphan_customer của A05.

**Regex tiền**: một dòng gói được cả bốn ràng buộc — USD thuần, hai số lẻ, không
âm, không ký hiệu.

**Vì sao đọc mọi thứ dưới dạng text (`all_varchar=true`)**: nếu để DuckDB tự đoán
kiểu thì kiểu sẽ trôi theo rác. Nó gọi `customer_id` là DOUBLE chỉ vì 0.2% rác
Excel-float của A03. Gate phải tự cast bằng `TRY_CAST` và đối chiếu với block
`schema` trong contract, đó mới là điểm cố định.

**Vì sao `ts_recover` thử cứu trước khi kết luận**: "không parse được" hẹp hơn
"sai định dạng". Có ~0.7% dòng viết sai dạng nhưng vẫn cứu được (DD/MM, ISO-T);
chỉ ~0.02% là không cứu nổi. Tolerance 0.5% dành cho nhóm sau. Nếu hard-fail theo
nhóm trước thì gate đỏ mỗi ngày, mà gate luôn đỏ thì không ai đọc.

**`payment_method` chỉ cảnh báo, không chặn**: enum có ghim nhưng
`row_level_tolerances` không có ngưỡng nào cho nó. Không có ngưỡng đã thoả thuận
thì mình chỉ được đo và báo, chưa được chặn. Đây là nguyên liệu cho amendment T11.

**`SAMPLE_ROWS = 100_000`**: đây là mình chọn, không phải contract quy định. Nó là
lý do sweep full scale vẫn nhanh.

### Task 5 — Validate chính contract bằng pydantic

Gate rất nghiêm với dữ liệu, nhưng với contract thì cả tin. `yaml.safe_load` trả
về dict trần, nên mọi `contract["schema"]` sau đó chỉ là đoán mò về một file do
người sửa tay. Mà contract này sắp bị sửa hai lần ở T11–T12.

Hai kiểu hỏng:
- **Gõ sai khoá** (`nullible:`) — YAML nhận bình thường vì đó vẫn là khoá hợp lệ.
  Chỗ nào mình viết `.get()` thì không báo lỗi gì cả, một luật cứ thế ngừng chạy.
  Gate có luật tắt âm thầm còn tệ hơn không có gate, vì mình vẫn tin nó.
- **Sai kiểu** (`window_days: seven`) — im tới lúc tính late-corrections, ở file
  thứ 30 của 45, báo TypeError về `timedelta` và không nhắc gì tới contract.

Bốn quyết định trong `contract_model.py`:
- `extra="forbid"` — pydantic mặc định bỏ qua khoá lạ. Gõ sai khoá tuỳ chọn
  (`alowed_values:`) sẽ bị nuốt im lặng y hệt yaml.
- Chỉ mô hình hoá phần gate đọc; phần còn lại khai `dict[str, Any]`. Nhưng vẫn
  phải khai, không thì `extra="forbid"` vô nghĩa.
- `columns` + `alias="schema"` vì `BaseModel.schema` bị pydantic chiếm rồi.
- Ghim cả kiểu chứ không riêng khoá. Chuỗi `"false"` là truthy trong Python, nên
  `if money["negative_allowed"]:` sẽ đọc thành "cho phép âm" — luật tiền lật ngược.

Chạy ra: yaml nạp file hỏng không kêu tiếng nào, báo `nullable = <<GONE>>`. Pydantic
bắt 3 lỗi. Chỗ đáng đọc là **đường dẫn lỗi**: `schema.1.nullible` nói rõ cột thứ
hai, khoá nullible, không được phép. Đó là địa chỉ mình đưa lại được cho người
soạn amendment. Một lỗi chính tả sinh ra hai lỗi cùng lúc.

Khi nào đừng dùng: pydantic dành cho config thôi. Dựng model cho từng dòng dữ liệu
là 82 triệu object Python với feed full — hàng giờ CPU để trả lời câu DuckDB làm
xong trong một lượt quét.

### Task 6 — Sweep 45 ngày (small)

`python -m work.a06_data_contract.a06_contract_check --scale small --start 2026-06-01 --end 2026-07-15`

45/45 PASS, 9 giây. Mỗi file kèm một `[warn] payment_method` — đúng thiết kế.
Chi phí mỗi file là lần đọc mẫu, không phải kích thước file. T9 sẽ kiểm điều đó.

### Task 7 — Phá gate có chủ ý

| File hỏng | Tầng bắt được | Thông điệp |
|---|---|---|
| `dropped_col` (xoá `payment_method`) | 3 — schema | missing column + trích MAJOR/30 ngày |
| `added_col` (thêm `currency`) | 3 — schema | unexpected column + trích MINOR/7 ngày |
| `type_change` (`order_id` → `'ORD-...'`) | 4 — giá trị | `order_id_not_castable` 5000/5000 (100%) |

Kết quả: 0/3 PASS.

**Điểm quan trọng nhất**: `type_change` **lọt qua** tầng schema vì tên cột và thứ
tự cột không đổi gì cả. Chỉ tầng giá trị mới bắt được, ở tỉ lệ cast hỏng 100%.
Nhìn output là thấy: chỉ file này mới có dòng `[warn] payment_method`, vì hai file
kia đã bị chặn ở tầng 3 nên gate không bao giờ đọc tới giá trị.

**Thông điệp nêu ví dụ cụ thể** (`'ORD-1520029791'`) là thứ biến nó thành hành
động được — producer nhìn phát biết ngay chuyện gì xảy ra, không cần đọc code
của mình.

**Cả ba đều có `[warn]` manifest**: bản copy scratch không có tín hiệu hoàn tất.
Gate nói ra chỗ nó không kiểm được thay vì im lặng bỏ qua.

Ba kiểu hỏng này không ngẫu nhiên: cột `currency` mới và `order_id` dạng chuỗi
chính là thứ shopcore sẽ ship dưới tên "month 2" ở A10.


### Task 9 — Sweep full scale

`python -m work.a06_data_contract.a06_contract_check --scale full --start 2026-06-01 --end 2026-07-15`

45/45 PASS, **13 giây** (so với 9 giây ở small).

File full ~350 MB mỗi cái, to gấp 20 lần small, nhưng gate chỉ chậm 1.4×. Vì nó
đọc 100k dòng rồi dừng — chi phí mỗi file là một lần đọc mẫu, không phải kích
thước file. Memory gần như không nhúc nhích.

Đây là yêu cầu thiết kế: pre-flight check phải rẻ đủ để không ai bị cám dỗ bỏ
qua. Bỏ `LIMIT` thì sweep full đọc ~16 GB thay vì vài trăm MB, và gate đắt là
gate bị bỏ qua.

**Bug đường dẫn**: lần chạy đầu ra 0/45 vì `main()` ghép `DATA_ROOT / "full" /
raw/orders`, nhưng full scale nằm thẳng ở `D:\etl_lab\raw\orders`, chỉ small mới
có thư mục con. Sửa bằng cách dùng `data_root(scale)` từ `lib.labpaths`.
Đáng chú ý: gate vẫn làm đúng việc — file không tồn tại là FAIL, đó chính là
`freshness_sla`. Chỉ là nguyên nhân nằm ở phía mình.

**payment_method ổn định ~0.30%** ở mọi ngày, cả small lẫn full, kể cả ngày
flash-sale. Rác có cấu trúc, không phải nhiễu ngẫu nhiên. Con số này là bằng
chứng cho amendment ở T11.

### Task 10 — Catalogue thay đổi hợp đồng & SemVer Mapping

Căn cứ quy định tại `change_management.policy`: Mọi thay đổi phá vỡ cam kết (Breaking) yêu cầu thông báo trước 30 ngày kèm MAJOR bump. Mọi thay đổi mang tính tương thích ngược (Additive) yêu cầu thông báo trước 7 ngày kèm MINOR bump, và consumer phải chịu được cột lạ.

| # | Kịch bản thay đổi từ Producer | Breaking hay không? Với ai? | Bump & Thời hạn báo trước |
|---|---|---|---|
| **1** | Thêm cột `currency` vào cuối file | **Non-breaking** với reader đọc theo tên cột. **Breaking** với reader đọc theo vị trí, hoặc gate đối chiếu danh sách cột chính xác. | **MINOR**, 7 ngày (`notice_additive`) |
| **2** | `order_id` BIGINT $\rightarrow$ `'ORD-...'` | **Breaking toàn diện**: phá schema lưu trữ, hỏng join, 100% rớt quarantine. | **MAJOR**, 30 ngày (`notice_breaking`) |
| **3** | Đổi tên `order_total` $\rightarrow$ `total_amount` | **Breaking toàn diện** với downstream: mọi SQL, mart, script gọi tên cũ sập ngay. | **MAJOR**, 30 ngày (`notice_breaking`) |
| **4** | Thêm `crypto` vào enum `payment_method` | **Breaking với consumer**, không với producer. Producer thấy chỉ thêm text; consumer có whitelist cứng đẩy hết bản ghi mới vào quarantine. | **MAJOR (đề xuất)** — 30 ngày |
| **5** | `order_ts` UTC $\rightarrow$ giờ store-local | **Breaking âm thầm**. Tên cột, thứ tự, kiểu đều không đổi nên gate mù hoàn toàn. Hỏng tận số liệu: doanh thu theo ngày lệch, mart theo vùng sai hết. | **MAJOR** (phá `order_ts.timezone`), 30 ngày |
| **6** | `null_or_invalid_status` bò 0.15% $\rightarrow$ 0.9% | **Không phải schema change**. Gate vẫn PASS vì dưới 1%. Nhưng chất lượng đang suy thoái, chỉ thấy được nếu lưu lịch sử gate. | **Không bump**. Cảnh báo xu hướng qua SLO hằng tuần |

#### Ba điểm rút ra:
1. **Gate của mình nghiêm hơn contract**: Kịch bản 1 — contract ghi rõ *consumer must tolerate unknown columns*, nhưng gate lại fail khi gặp cột thừa. Đây là quyết định thiết kế chủ quan của consumer, không phải điều khoản hai bên ký. Hai hướng xử lý: nới lỏng gate cho đúng cam kết, hoặc đàm phán bắt buộc producer khai báo cột mới trước hạn — nội dung đưa vào amendment Task 11.
2. **Contract im lặng về việc mở rộng enum**: `change_management` chỉ định nghĩa MAJOR cho rename/removal/type change, hoàn toàn bỏ quên việc thêm giá trị vào `allowed_values`. Do đó câu trả lời MAJOR ở kịch bản 4 thuần túy là đề xuất đơn phương từ phía consumer, chưa có cơ sở pháp lý. Đây là một gap lớn cần khép lại ở Task 11.
3. **Ba tầng nghiêm trọng với độ phơi nhiễm khác nhau**:
   - Kịch bản 2–3: Gate bắt được ngay lập tức tại tầng schema/kiểu dữ liệu.
   - Kịch bản 5: Gate mù hoàn toàn vì schema không đổi — cần bổ sung semantic check (như so sánh phân bố giờ trong ngày với baseline lịch sử).
   - Kịch bản 6: Gate vẫn xanh nhưng rác đang bò sát trần dung sai — cần quan sát theo chuỗi thời gian. Đây là lý do kiến trúc cần bảng `ops.contract_checks`: nếu không lưu lịch sử, hệ thống chỉ thấy được lát cắt của ngày hôm nay.

### Task 11 — Soạn amendment v1.4.0

Sản phẩm: `work/amendment_v1_4_0.md`, hai clause + rollout note.
Contract đã ký giữ nguyên từng byte.

| Clause | Đặt ở | Khép gap nào |
|---|---|---|
| transport_locale | `schema.order_total.money` | Dấu phẩy châu Âu và tiền tố `$` từ gap thành violation |
| updated_at semantics | `schema.updated_at` | Cột dedupe mà không ai biết nó đo gì |

**Vì sao MINOR**: hai clause chỉ viết ra thành lời cái ý định vốn đã có —
`money` đã ghim USD và scale 2, `latest_wins_key` đã dựa vào `updated_at`.
Không code consumer nào vỡ, không cột nào bị xoá/đổi tên/đổi kiểu. Nhưng nhiều
hơn PATCH: PATCH sửa câu chữ, đây là thêm ràng buộc gate cưỡng chế được.
Báo trước 7 ngày qua `#shopcore-data-changes`. Trạng thái `proposed` cho tới khi
shopcore ký đối ứng — trước đó consumer không được dựa vào.

**Paste-check**: cả hai đoạn qua `yaml.safe_load` sạch. Clause sai cú pháp là
clause không review được.

Một chi tiết bắt được lúc paste-check: `nullable: false` ra bool thật, còn
`thousands_separator: none` ra **chuỗi** `'none'`. Đúng cái bẫy T5 — chuỗi khác
rỗng là truthy, `if locale["thousands_separator"]:` sẽ đọc thành "có dấu phân
cách nghìn".

### Task 12 — PII: clause phải triển khai được

Gap này khác mọi gap còn lại. Các gap kia gây số sai; gap này gây nghĩa vụ thông
báo rò rỉ dữ liệu. Và `core.customers` đã lưu email thô từ A03 — không ai quyết
định điều đó, không clause nào cho phép cũng không clause nào cấm.

Clause hứa hai thứ, nên phải đo trước khi phá cột:

| | Trước | Sau |
|---|---|---|
| dup rows theo email | 180 | — |
| dup rows theo email_hash | — | **180** |

- Nửa 1 — phơi nhiễm biến mất: `DESCRIBE` cho thấy `email_hash VARCHAR` ở vị trí
  thứ ba, không còn `email` ở đâu.
- Nửa 2 — khả năng được giữ: 180 y hệt. Và tín hiệu vẫn dùng được, mỗi
  `email_hash` trùng vẫn ra đúng hai `customer_id`: `[256, 30256]`,
  `[7984, 37984]`, `[16265, 46265]`.

**Vì sao `md5(lower(trim(email)))` chứ không `md5(email)`**: biểu thức này giờ
là một clause, nó không được phép dựa vào chuyện A03 mãi mãi làm sạch ở thượng
nguồn. `md5('John@X.com') != md5('john@x.com')` — hash chưa chuẩn hoá sẽ âm thầm
đánh mất đúng cái khả năng mà clause hứa. Chuẩn hoá tại chỗ hash, luôn luôn.

**Chạy lần hai thì hỏng**: `Binder Error: Referenced column "email" not found`,
kèm gợi ý `Candidate bindings: "email_hash"`. Lỗi đó chính là clause đang hoạt
động — không còn email thô để hash lại, cũng không còn để rò rỉ.

**Ba cách che PII**: hash để phân tích (một chiều, quan hệ bằng nhau vẫn còn),
mask để hiển thị (giữ hình dạng, không so bằng được), tokenize khi cần lấy lại
giá trị gốc (chỉ vault mới đảo ngược).

**Clause này còn có tương lai**: ở A15, `stg_shopcore__customers` sẽ tính chính
`email_hash` đó thẳng từ feed thô và không bao giờ select cột thô — clause được
cưỡng chế bằng cách xây dựng, mỗi lần dbt rebuild.

## A07 — Lỗi, retry và tính idempotent

### Numbers

| | Small (2026-06-03) | Full (2026-06-01) |
|---|---|---|
| Rows | 62,707 | 1,214,064 |
| Checksum | 15477265932102261913 | 9351036678762254023 |
| File CSV | 18.1 MB | 352.3 MB |
| Thời gian một lần load | ~1.5s | ~3s |

- Full scale ba ngày: 1,214,064 + 1,180,490 + 1,254,150 = **3,648,704 dòng, 13 giây**,
  từ ~1 GB CSV. RAM giữ ở mức 1–2 GB nhờ streaming — so với ngoại suy 34 GB của
  pandas ở A01.
- Ledger small: **7 success / 34 failed** trên 41 run_id. Không dòng nào kẹt `running`.
- Ledger full: **5 success / 3 failed** trên 8 run_id.
- Đối soát manifest ba ngày full: khớp chính xác 100%.

---

### Task 0 — core.orders đúng hình dạng

`core.orders` trước khi drop: **472,127 dòng** — di sản của A05 (INSERT trần, nhân
đôi) cộng một lần load thừa cuối A06. Dựng lại còn 13 cột: 10 cột nguồn +
`order_date` + `_data_date` + `_run_id`.

Đó là dữ liệu dẫn xuất — dựng lại được từ `raw/`, và sau hôm nay mình sở hữu chính
công cụ dựng lại nó an toàn. Tâm thế **raw + code = sự thật**.

Warehouse full cũng phải làm y vậy, nhưng mãi tới Task 9 mới phát hiện: bảng cũ ở
đó có **14 cột**, nên INSERT nổ `Binder Error: table orders has 14 columns but 13
values were supplied`. Đáng chú ý — đó là `BinderException`, không phải
`TransientError`, nên `with_retry` **không** thử lại. Đúng thiết kế: schema sai thì
chạy lại một triệu lần vẫn sai.

---

### Task 1 — Bug chạy hai lần

62,707 → **125,414** sau lần INSERT thứ hai. Không exception, không cảnh báo.

**Vì sao INSERT trần không bao giờ an toàn khi chạy lại**: nó chỉ biết THÊM, không
biết trạng thái đích hiện có gì — nên kết quả phụ thuộc số lần chạy chứ không phụ
thuộc dữ liệu nguồn. Một job retry sau timeout, một lần chạy tay để "chắc ăn", đều
nhân đôi dữ liệu mà không để lại dấu vết.

Vài `order_id` ra **4 lần**: chúng vốn đã trùng trong file (in-file dup của A03),
giờ nhân đôi thêm. Hai vấn đề khác nhau — A08 xử lý loại thứ nhất, A07 xử lý loại
thứ hai.

---

### Task 2 — Run ledger `ops.etl_runs`

Chín cột: `run_id · run_date · step · status · started_at · finished_at · rows ·
source_file · error`.

**Mỗi LẦN THỬ một dòng riêng**, không phải mỗi ngày một dòng. Nếu mỗi ngày một dòng
thì lần chạy thứ hai ghi đè lần thứ nhất và mất sạch lịch sử thất bại. Task 6 crash
34 lần — ledger nhớ hết. Thất bại là lịch sử đáng giữ, không phải điều đáng che.

**Hai cột `source_file` và `finished_at` sống ở đây, không sống trên dòng dữ liệu.**
Đó chính là `_source_file` và `_loaded_at` mà A03 từ chối. Chúng là thuộc tính của
LẦN CHẠY, không phải của từng đơn hàng — đơn hàng không "có" một tên file, cái lần
chạy nạp nó mới có.

Đo được: đóng lên mỗi dòng thì 62,707 bản sao giống hệt của cùng một chuỗi (1.2
triệu ở full). Lưu ở ledger thì một dòng, và mọi dòng trỏ tới bằng `_run_id` 8 byte.
Nạp lại ngày đó từ file đã sửa → **một** dòng ledger đổi, thay vì ghi lại cùng chuỗi
1.2 triệu lần. Đây là normalization thông thường, cùng quy tắc giữ địa chỉ khách
hàng không bị chép vào từng dòng đơn hàng.

---

### Task 3 — stage → validate → publish nguyên tử

Ba chi tiết trong bộ khung:

1. `UPDATE ... status='success'` nằm **bên trong** `BEGIN…COMMIT` — ledger không bao
   giờ khai thành công cho dữ liệu không tồn tại.
2. `DELETE WHERE _data_date = ?` chạy cả ở lần load **đầu tiên** (xoá 0 dòng, không
   tốn gì). Lần đầu và lần chạy lại đi **cùng một nhánh code** — ít nhánh, ít bug.
3. `INSERT` mở dòng ledger ghi `source_file` **ngay**, trước khi có gì hỏng. Một lần
   thử thất bại vẫn phải nói được nó nghẹn ở file nào.

**Hai chỗ tự viết:**
- `already_succeeded()` — `SELECT count(*) > 0 FROM ops.etl_runs WHERE run_date = ?
  AND step = 'load_day' AND status = 'success'`. Chỉ tính `success`: một ngày có
  mười dòng `failed` vẫn là chưa load, và phải load lại được mà không cần `--force`.
- `CREATE SCHEMA` đặt sai chỗ trong `ensure_tables()` — nó nằm **sau** `CREATE TABLE
  ops.etl_runs`. Trên warehouse đã có schema `ops` từ A05 thì vẫn chạy, nên bug này
  chỉ nổ trên máy sạch. Đó là lý do nó nguy hiểm.

Chạy hai lần, cả hai `published 62707 rows`, checksum giống hệt.

**Checksum cố ý chỉ hash cột nghiệp vụ** (`order_id, updated_at, status,
order_total`). `_run_id` **phải** khác giữa hai lần chạy — đó là lineage ghi lịch
sử, không phải dữ liệu thay đổi. Giá trị `hash()` chỉ so được trong cùng một bản cài
DuckDB.

---

### Task 4 — Preflight: bảy check trước khi chạm dữ liệu

Thứ làm hỏng pipeline lúc 2 giờ sáng hiếm khi là SQL:

| # | Check | Điểm đáng chú ý |
|---|---|---|
| 1 | data root writable | Thư mục có thể tồn tại mà vẫn từ chối ghi — phải THỬ GHI, không phải `.exists()` |
| 2 | free space on data volume | Chỗ trống ổ C không giúp gì nếu dữ liệu ở ổ D |
| 3 | temp dir on data volume | Spill sang ổ hệ thống làm đầy ổ C và treo máy |
| 4 | source CSV + manifest | *(tự viết)* ba cách delivery hỏng |
| 5 | warehouse writable, not locked | **Mở connection chính là phép dò khoá** |
| 6 | schemas creatable | `CREATE SCHEMA IF NOT EXISTS` = dò quyền miễn phí |
| 7 | contract parseable | YAML hỏng fail ở đây, không phải giữa pipeline |

**Check 4 tự viết** — ba nhánh, gộp thành đúng một dòng kết quả:
- CSV không tồn tại → gõ nhầm `--date`, hoặc chưa chạy generator
- CSV tồn tại nhưng 0 byte → **sự cố thượng nguồn**, báo shopcore
- manifest JSON không tồn tại → thiếu completeness signal

Khi FAIL, `detail` nêu **đích danh đường dẫn** (`D:\etl_lab\small\raw\orders\
orders_2026-05-01.csv not found`), không viết "file bị thiếu". Người trực cần địa
chỉ, không cần lời than.

**Diễn tập đỏ/xanh — mã thoát là phần quan trọng nhất:**

| Lệnh | Kết quả | Exit |
|---|---|---|
| bình thường | 7 PASS | 0 |
| `--tmp C:\Windows\Temp` | check 3 FAIL | 1 |
| `--date 2026-05-01` | check 4 FAIL, nêu đúng path | 1 |

Mã thoát là thứ scheduler tuân theo; báo cáo đỏ mà vẫn exit 0 là chuông báo cháy
không lắp pin.

**Preflight không dừng sớm** như contract gate của A06 — sáu check kia vẫn chạy khi
một check đỏ, vì lúc 2 giờ sáng cần thấy toàn bộ tình trạng trong một lượt chứ không
phải sửa từng cái rồi chạy lại bảy lần.

**Check 6 báo `skipped: no connection` khi check 5 fail** — không in PASS giả cho
một check chưa chạy.

Check 5 kích hoạt thật nhiều lần trong buổi: kernel Jupyter giữ `warehouse.duckdb`.
Nhịp làm việc cho cả A07: `con.close()` → chạy script ở terminal → `connect` lại.
Ngoài ra còn 4 process python treo từ 10 ngày trước, dọn bằng
`Get-Process python | Stop-Process -Force`.

---

### Task 5 — Git rescue kit

**Ba lệnh:**

| Khi nào | Lệnh |
|---|---|
| Sửa hỏng, chưa commit | `git restore <file>` |
| Cần file từ một commit cũ | `git checkout <sha> -- <file>` |
| Quay cả repo về một commit | `git reset --hard <sha>` |

Hai lệnh đầu chỉ đụng file nêu tên. Lệnh thứ ba đụng tất cả — nó xoá luôn
`a07_explore.py` khỏi đĩa.

**Bốn điều rút ra:**

1. **Git chỉ cứu được thứ đã giao cho nó.** `git status` in `M` là file đang theo
   dõi (cứu được), `??` là git không biết (xoá là mất). `a07_explore.py` mang `??`
   suốt buổi — phá nó thay vì phá loader là mất 387 dòng.
2. **Định lượng thiệt hại trước khi hoảng.** `git diff --stat` cho `313 deletions`.
   Có con số thì mới chọn được cách xử lý.
3. **`reset --hard` không xoá hẳn.** Commit `a43176b` biến khỏi `git log` nhưng còn
   trong `git reflog`, và explore được lấy lại từ chính commit "đã chết" đó. Git giữ
   khoảng 30 ngày.
4. **Làm trước Task 6–7** vì hai task đó bắt sửa code liên tục trong lúc pipeline
   crash. Savepoint `07673bd`.

**Liên hệ với chủ đề bài:** loader idempotent = chạy lại thì dữ liệu về đúng một
trạng thái. Git savepoint = `reset --hard` thì code về đúng một trạng thái. Cùng
nguyên tắc, một cho dữ liệu, một cho code.

---

### Task 6 — Chaos monkey

Bốn điểm gây lỗi, phân bố sau 34 lần chết:

| Điểm | Số lần | Crash ở đây thì warehouse ra sao |
|---|---|---|
| `after_read_manifest` | 18 | Chưa đụng gì |
| `after_stage` | 9 | Bảng tạm mất, warehouse không đổi |
| `mid_transaction` | 5 | DELETE đã chạy, INSERT chưa |
| `before_commit` | 2 | Đã INSERT nhưng chưa COMMIT |

**Phân bố hình thang là đúng, không phải bug.** Tới được điểm sau nghĩa là đã sống
sót qua mọi lần tung xúc xắc trước. Với chaos 0.6: 60% / 24% / ~10% / ~4%.

**Checksum không đổi qua tất cả**: 62707 / 15477265932102261913. Năm lần
`mid_transaction` là bằng chứng mạnh nhất — không có transaction thì đó là năm lần
mất trắng một ngày dữ liệu, im lặng. Có transaction thì `ROLLBACK` hoàn tác DELETE.

**Không dòng nào kẹt ở `running`**: 7 + 34 = 41, khớp đúng số run_id đã cấp. Vì
`fail_run()` nằm trong `except` và `UPDATE ... success` nằm trong transaction.

---

### Task 7 — Retry với exponential backoff

Backoff quan sát được: **1.0s → 2.0s → 4.0s → 8.0s → 16.0s → 30.0s → 30.0s**, chạm
trần `max_delay`.

**`with_retry` chỉ bắt `TransientError`.** Trong `load_day()`, `if staged == 0: raise
ValueError(...)` là **dữ liệu sai** — file rỗng thì thử lại một triệu lần vẫn rỗng.
Retry không sửa được, và mỗi lần thử vô ích lại chôn sâu thêm nguyên nhân thật. Lỗi
`BinderException` ở Task 9 chứng minh điều này trong thực tế: nó fail ngay, không
retry.

Đây là quy tắc bị bỏ qua nhiều nhất trong thiết kế retry: `except Exception` rồi
retry tất.

**Alert:** mỗi lần bỏ cuộc ghi đúng một dòng `ops.alerts`, severity `error`, `run_id`
là lần thử **cuối cùng**. Chính sách retry và chính sách cảnh báo là hai quyết định
khác nhau — một load bỏ cuộc trong im lặng thì không ai biết dữ liệu hôm nay thiếu.

**Đánh đổi `max_attempts`:** đặt 1 thì mọi lỗi thoáng qua đều thành alert, người
trực học cách bỏ qua alert. Đặt quá cao thì lỗi thật bị giấu hàng chục phút. Không
có số đúng.

---

### Task 8 — Phần chứng minh

**8a — bốn checksum.** Ba đường đi khác nhau tới cùng một kết quả:

| run_id | Đường đi | Checksum |
|---|---|---|
| 42 | chạy sạch, thành công ngay | 15477265932102261913 |
| 54 | fail 1 lần rồi thành công | y hệt |
| 57 | fail 2 lần rồi thành công | y hệt |
| — | skip check (không `--force`) | ~1 giây, không cấp run_id |

Một lần chạy crash 5 lần trên đường đi **không phân biệt được** với một lần chạy
trơn tru. Và lần `--max-attempts 10` bỏ cuộc hoàn toàn (run 43–52) cũng không làm
hỏng gì: bảng vẫn nhất quán, và đã báo động qua `ops.alerts`.

**8b — truy vết, đòi lời hứa của A03.**

`DESCRIBE core.orders`: 13 cột, trong đó đúng **hai** cột lineage. Ít hơn bản năng
muốn.

```
order_id  _data_date  _run_id | source_file            finished_at    status   rows
1530041035 2026-06-03      57 | orders_2026-06-03.csv  09:57:57.336   success  62707
```

Bất kỳ dòng nào cũng cho cùng câu trả lời — đó là điểm mấu chốt. Mọi dòng mang
`_run_id = 57`, lần thử thành công cuối. Những lần thất bại trước có cái đã đi tới
INSERT (`before_commit` fail 2 lần), nhưng ROLLBACK xoá sạch; chỉ ledger và file log
còn nhớ.

**Điểm yếu phát hiện được:** docstring nói `logs/run_<run_id>.log` nhưng thực tế tên
file dùng timestamp, vì một lần gọi script sinh nhiều `run_id` (mỗi attempt một cái).
File `run_2026-06-03_1788836273.log` chứa run 55, 56, 57. Tra ngược phải grep:
`Select-String -Path logs\*.log -Pattern "run_id=57 "`.

Cũng có file log **0 byte** — lần bị preflight chặn vì warehouse khoá. Logger tạo
file rồi script thoát trước khi ghi gì. Không đẹp, nhưng nói đúng sự thật: lần chạy
đó không làm gì cả.

---

### Task 9 — Full scale

Ba ngày, 3.65 triệu dòng, **13 giây**. RAM 1–2 GB nhờ streaming.

**Force chạy lại 1.2 triệu dòng: ~3 giây, checksum không đổi** (9351036678762254023).
Chạy lại một ngày không phải chuyện phải cân nhắc.

**Đối soát manifest — khớp chính xác cả ba ngày:**

| Ngày | manifest.rows | loaded |
|---|---|---|
| 2026-06-01 | 1,214,064 | 1,214,064 |
| 2026-06-02 | 1,180,490 | 1,180,490 |
| 2026-06-03 | 1,254,150 | 1,254,150 |

Đó là `quality.reconciliation` của contract A06 — chuỗi từ A06 sang A07 khép lại ở
đây.

**`runs = 1` cho mọi ngày.** Ngày 06-01 load ba lần (run 4, 7, 8) nhưng chỉ run 8 sở
hữu dữ liệu — hai lần trước bị DELETE xoá sạch. Không dòng nào mồ côi từ lần chạy cũ.

---

### New knowledge & Core insights

- **Idempotency là thuộc tính của KẾT QUẢ, không phải của số lần chạy.** Chạy một
  lần hay mười lần, crash giữa chừng hay trơn tru, trạng thái đích phải giống hệt.
  Cách đo là checksum, không phải cảm giác.
- **Delete-then-insert trong một transaction** là cách đạt idempotency đơn giản nhất
  cho ETL theo ngày. DELETE chạy cả lần đầu để lần đầu và lần chạy lại đi cùng một
  nhánh code.
- **Checksum phải độc lập thứ tự.** `bit_xor(hash(...))` gom bằng phép giao hoán nên
  thứ tự dòng không ảnh hưởng — điều bắt buộc khi engine chạy song song. `count(*)`
  quá thô (đổi nội dung mà giữ số dòng thì không thấy); `string_agg` rồi hash thì đỏ
  ngẫu nhiên.
- **Chỉ retry lỗi tạm thời.** Phân loại lỗi TRƯỚC khi quyết định retry là quyết định
  thiết kế, không phải chi tiết cài đặt. `except Exception` rồi retry tất sẽ lặp lại
  lỗi tất định tới hết số lần thử và chôn stack trace thật.
- **Ledger ghi mọi LẦN THỬ, không ghi mọi NGÀY.** Lịch sử thất bại là dữ liệu vận
  hành: nó cho biết pipeline hỏng ở đâu, bao lâu một lần, và lần cuối thành công là
  khi nào.
- **Thuộc tính của lần chạy thì lưu ở lần chạy.** Đây là normalization áp cho
  metadata, không riêng cho dữ liệu nghiệp vụ.
- **Preflight là shift-left của A06 dời thêm một bậc.** A06 kiểm dữ liệu trước khi
  nạp; A07 kiểm môi trường trước khi đọc. Cả hai đều rẻ, và cả hai đều trả về mã
  thoát.

### Observations to carry forward

- Một file DuckDB, một process ghi. Notebook và terminal không dùng chung được —
  `con.close()` trước khi chạy script.
- `--force` là công tắc an toàn: mặc định skip ngày đã thành công, muốn nạp lại phải
  nói rõ.
- Kernel restart làm mất `con`, `section`, `DAY`, `CHECKSUM_SQL`. Cell nào có `DROP`
  hay `CREATE TABLE` không kèm `IF NOT EXISTS` thì đọc kỹ trước khi chạy lại.
- Warehouse small và full là hai file riêng, mỗi cái có `ops.run_id_seq` riêng — nên
  run_id đánh số độc lập.
- `preflight.py` nằm thẳng trong `work/` chứ không trong thư mục con, vì A11 sẽ
  import nó trước từng ngày backfill.


# A08 — Append vs Upsert vs Partition Refresh

**Ngày:** 2026-09-08 · **Branch:** `a08-load-strategies` · **Cửa sổ:** 2026-06-01 → 2026-06-10 (10 file)

Ba cách nạp cùng một tập dữ liệu, chứng minh chúng cho ra cùng một trạng thái hiện hành tới từng dòng, rồi chọn một cái để dùng.

---

## 1. Số đo

### Dữ liệu đầu vào (từ manifest)

| | small | full | tỉ lệ |
|---|---|---|---|
| rows | 577.673 | 11.553.524 | 20,0× |
| late_rows | 7.750 (1,34%) | 155.088 (1,34%) | — |
| dup_rows | 284 (0,05%) | 5.773 (0,05%) | — |

`dup_rows` nằm dưới ngưỡng contract `within_file_exact_resends: 0,2%` → nhiễu at-least-once bình thường, không phải sự cố.

### Ba chiến lược

| | dòng ghi (small / full) | thời gian (small / full) | ghi/nhận | chi phí đọc | lịch sử | file bất biến |
|---|---|---|---|---|---|---|
| append + view | 577.673 / 11.553.524 | 2,3s / 24,7s | 1,0× | **0,040s / 2,131s** | đầy đủ | được |
| upsert (MERGE) | 575.494 / 11.509.661 | 3,8s / 59,1s | 1,0× | 0 | mất | **không** |
| partition refresh | 2.980.603 / 59.611.485 | 3,4s / 66,7s | **5,2×** | 0 | mất | **được — duy nhất** |

Refresh còn xoá 2.410.964 (small) / 48.218.822 (full) dòng.

### Kết quả reconcile

| | small | full |
|---|---|---|
| ba chiến lược | 569.639 | 11.392.663 |
| bốn phép diff EXCEPT | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |

### Số phụ

| | small | full |
|---|---|---|
| order_id ở nhiều hơn một file | 7.684 (1,35%) | 153.678 (1,35%) |
| chồng lấn 06-01 ∩ 06-02 | 883 đơn (late_rows ghi 885 dòng) | — |
| dòng bị MERGE từ chối | ~1.895 | 38.090 |
| `null_dates` sau COALESCE | 0 | 0 |

---

## 2. Những chỗ suýt sai

**Chồng lấn 883 vs late_rows 885.** `INTERSECT` đếm **đơn hàng phân biệt**, `late_rows` đếm **dòng**. Hai đơn được sửa hai lần trong cùng file 06-02 → góp 2 dòng nhưng 1 đơn. Cùng lời giải thích cho 7.684 vs 7.750 ở quy mô 10 file.

**`order_date` không được phép NULL.** `try_strptime` trả NULL cho timestamp hỏng (~0,02% dòng). `NULL BETWEEN x AND y` cho ra NULL chứ không phải TRUE → dòng đó không lọt qua `WHERE` nào của chiến lược C, không được insert, không bị delete. Mất dòng, không lỗi, không cảnh báo. `COALESCE(..., DATE '{d}')` là cái lưới hứng. Dòng `null_dates = 0` trong output là bằng chứng lưới hoạt động.

**MERGE không báo lỗi khi nguồn có key trùng.** Nó chọn bừa một dòng rồi báo thành công. Phải dedupe staging trước — đó là `TEMP TABLE stg_day`.

**Điều kiện chặn `s.updated_at > t.updated_at`.** Bỏ nó đi thì MERGE vẫn chạy, count cuối vẫn đúng 11.392.663, nhưng 38.090 đơn bị kéo ngược về phiên bản lỗi thời. Không test count-based nào bắt được.

**Hình dạng query của chiến lược C.** Dedupe **toàn cục** trên log as-of trước, **rồi** mới lọc theo cửa sổ. Lọc trước rồi dedupe là sai: bản thắng thật của một đơn có thể nằm ngoài cửa sổ, dedupe cục bộ sẽ làm sống lại phiên bản cũ. Ở small hai cách cho kết quả giống hệt; ở full cách sai để lại 7 đơn trùng.

---

## 3. Cửa sổ sai — bài học đắt nhất

Chạy lại chiến lược C với cửa sổ ngây thơ `[D, D]` thay vì `[D−7, D]`:

| | count | diff nội dung |
|---|---|---|
| cửa sổ đúng | 569.639 | 0 |
| cửa sổ sai | 569.640 | **5.824** |

Count lệch **1** (0,0002%). Nội dung sai **5.824 dòng** (1,02%).

Mọi đơn có phiên bản hiện hành là bản sửa đều kẹt ở bản gốc cũ, vì bản sửa rơi vào partition cũ hơn mà cửa sổ hẹp không chạm tới.

**Đếm không phải là reconcile.** Count trả lời "có bao nhiêu dòng", không trả lời "có đúng những dòng đó không". Phải diff nội dung, và phải cả hai chiều — `A − B = 0` chỉ nói A là tập con của B.

Con số 7 trong `[D−7, D]` đến từ `late_corrections.window_days` của contract A06. Không có cam kết bằng văn bản thì cửa sổ chỉ là phỏng đoán.

---

## 4. Truy vết đơn `1510000111`

| _data_date | status | order_total | updated_at | |
|---|---|---|---|---|
| 2026-06-02 | refunded | 435,92 | 2026-06-02 00:10:35 | ← thua |
| 2026-06-01 | delivered | 84,48 | 2026-06-02 20:27:19 | ← bản gốc |
| 2026-06-06 | refunded | 230,65 | 2026-06-06 07:53:51 | ← thắng |

Bản trong file 06-02 mang `updated_at` **sớm hơn** bản trong file 06-01. File về sau, nội dung cũ hơn. Xếp hạng theo `_data_date` sẽ chọn nhầm `435,92` — sai gần gấp đôi giá trị, count không nhúc nhích.

**Thứ tự file không có ý nghĩa gì. Chỉ `updated_at` mới xếp hạng được.**

Cả ba chiến lược đều chọn `refunded / 230,65`. Ba cơ chế khác nhau (`row_number()`, điều kiện chặn của MERGE, dedupe as-of) cùng thực thi một điều khoản contract: *latest updated_at wins*.

Nhìn từ nghiệp vụ: đơn ngày 01/06 chỉ được biết là đã hoàn tiền vào 06/06 — trễ năm ngày. Báo cáo doanh thu chốt ngày 02/06 ghi nhận đơn này là bán được, và con số đó **không sai** ở thời điểm chốt, chỉ lỗi thời.

---

## 5. Quyết định

**Dùng partition refresh cho `core.orders`, giữ append log làm nguồn bền vững.**

Chi phí dedupe không mất đi, chỉ đổi chỗ: view trả 2,13s ở *mỗi lượt đọc*, refresh trả một lần lúc ghi. Đọc xảy ra hàng nghìn lần mỗi ngày → dời chi phí sang phía ghi là đúng chiều.

Giá 5,2× dòng ghi rẻ hơn vẻ ngoài: 66,7s so với 59,1s của upsert, chỉ chậm 13%, vì ghi tuần tự theo lô rẻ hơn 11,5 triệu lần dò tìm key rời rạc. **Nhưng đây là đánh đổi có điều kiện** — rẻ *vì cửa sổ chỉ 7 ngày*. Contract cho 30 ngày thì tỉ lệ ghi vọt lên và upsert thắng ngược.

Điểm quyết định là tương thích: Parquet bất biến, không update tại chỗ. Refresh là chiến lược **duy nhất** dùng được cho file lake. Upsert khoá pipeline vào warehouse vĩnh viễn.

Giữ log vì nó là thứ duy nhất trả lời được "ta biết điều này từ khi nào", và là bảo hiểm nếu logic dedupe hôm nay hoá ra sai.

**Nhược điểm phải thừa nhận:** refresh chỉ idempotent ở ngày mới nhất. Chạy lại ngày D cũ hơn sẽ tua `[D−7, D]` về trạng thái as-of D, xoá mất bản sửa về sau. Rerun không phải "chạy lại một ngày" mà là backfill từ D cuốn tới — A11 dựng runner đó.

**Lỗ hổng còn nguyên:** trong lúc `DELETE` xong mà `INSERT` chưa xong, người đọc thấy gì? Transaction A07 che được người đọc trong warehouse. Nếu `core.orders` nằm ở lake dạng Parquet thì không có transaction nào cả → **A09**.

---

## 6. Ghi chú hiệu năng

- append: small 2,3s → full 24,7s, tức **10,7× thời gian cho 20× dữ liệu**. Tốt hơn tuyến tính vì chi phí cố định bị nuốt gọn và 8 luồng được dùng hiệu quả hơn.
- Khoản thuế của view **phình ra** theo scale: 13× ở small → **426×** ở full (0,005s vs 2,131s). `count(*)` trên log gần như không đổi (đọc metadata), còn view phải chạy `row_number()` trên toàn bộ 11,5 triệu dòng.
- 2,131s mới chỉ là `count(*)` — phép đọc rẻ nhất tồn tại. Query thật có join/filter/group by còn tệ hơn.
- Refresh ghi gấp 5 lần upsert nhưng chỉ chậm 13%: máy tính thích việc lớn và đơn giản hơn việc nhỏ và nhiều.

## 7. Nguyên tắc rút ra

> **Phát triển trên small, đừng bao giờ nghiệm thu trên small.**

Cái bẫy hình dạng query ở Task 5 hoàn toàn im lặng ở scale small và chỉ lộ ra ở full. Không có bước chạy full thì lỗi đó đã lên production.

---

# A09 — Atomic partition swap

Câu hỏi của bài: ghi lại một partition đã publish, trong lúc có người đang đọc
nó. Ba cách, đo cùng một thứ — người đọc thấy gì trong lúc ghi.

Nạn nhân: `order_date=2026-06-19`, ngày cao điểm.

---

## 1. Số đo

### Trạng thái ban đầu

| | small | full |
|---|---|---|
| manifest `rows` | 182.151 | 3.643.024 |
| manifest `late_rows` | 2.732 | 54.645 |
| manifest `dup_rows` | 91 | 1.821 |
| dòng thật trong partition | 180.183 | 3.603.703 |
| số file Parquet | 8 | 8 |

**Manifest 182.151 nhưng partition chỉ 180.183.** Chênh 1.968. Không phải mất
dữ liệu — file `orders_2026-06-19.csv` chở 2.732 dòng sửa cho các ngày CŨ HƠN,
loader A02 đã xếp chúng vào partition khác. Đổi lại, partition 06-19 nhận 764
dòng sửa từ các file 06-20..06-26. 2.732 − 764 = 1.968.

**8 file** = 1 file gốc + 7 lần các ngày sau gửi late correction về. Một file
14,5 MB và bảy file 6–17 KB. Bảy file nhỏ tổng 91 KB, tức 0,6% của file chính —
đúng hình dạng vật lý của con số late 1,34% đo ở A08. Sửa thì ít, nhưng rải ra
thành nhiều file, và mỗi lần rebuild phải gom hết.

Nếu chỉ thấy 1 file thì loader A02 đã dùng `OVERWRITE_OR_IGNORE` và ăn mất các
file late correction. Kiểm chỗ này trước khi làm gì khác.

### Cửa sổ đọc sai — con số trung tâm

| scale | naive rewrite | swap |
|---|---|---|
| small (180 k dòng) | **1,9 s** | **0,004 s** |
| full (3,6 M dòng) | **28,7 s** | **0,002 s** |

Dữ liệu gấp 20 lần → naive chậm gấp 15 lần, swap không đổi.

Đây mới là câu kết luận, không phải "swap nhanh hơn": **cửa sổ sai của naive
tăng theo dữ liệu, của swap thì không.** Naive bắt người đọc chịu thời gian
*ghi dữ liệu* — tăng theo số byte. Swap bắt họ chịu thời gian *đổi tên thư mục*
— không tăng, vì filesystem chỉ sửa một mục trong bảng thư mục.

### Sau dedupe

180.183 → **179.294** (small, mất 889 dòng), 3.603.703 → **3.585.850** (full,
mất 17.853).

Không phải mất dữ liệu. Loader A02 append bản sửa thành file mới mà không xoá
bản cũ, nên partition đang chứa 889 đơn hàng ở hai phiên bản. `rebuild_sql` giữ
`updated_at` mới nhất nên gộp còn một. Partition sau khi rebuild ĐÚNG HƠN trước
— nhưng đúng theo cách khiến mọi người đọc trong 1,9 giây đó nhận số rác.

---

## 2. Ba kiểu người đọc gặp phải

Task 3, naive rewrite, small:

```
15:54:23.503  count=180183
15:54:29.890  count=0            ← trả lời sai, không báo lỗi
15:54:30.759  IOException        ← mở trúng file đang ghi dở
15:54:31.367  count=179294
```

`IOException` làm job đỏ, có người bị gọi dậy, sửa. `count=0` chảy thẳng vào
dashboard, vào email báo cáo, vào quyết định — không ai biết.

Ở Task 6 (`--nap 2`) chỉ còn `count=0`, không có `IOException` nào: thư mục
biến mất hẳn nên glob không tìm thấy gì, DuckDB trả 0 dòng và không coi đó là
lỗi. Loại bỏ được lỗi ồn ào không có nghĩa là an toàn hơn.

---

## 3. Chỗ suýt sai đắt nhất — staging đặt trong `orders/`

Task 4. Copy partition thành `order_date=2026-06-19.__tmp__` NGAY TRONG
`orders/`. Lake **đứng hoàn toàn yên** — không xoá, không ghi, không process nào
chạy. Reader vẫn đọc ra **358.588 = 179.294 × 2**.

Hai thứ hỏng chồng lên nhau:

1. Glob `orders/*/*.parquet` — `*` không kiểm tra tên, thư mục thừa bị vơ vào.
2. Filter `WHERE order_date = DATE '2026-06-19'` **vẫn khớp**:

| Tên thư mục | Chuỗi giá trị | Cast thành DATE |
|---|---|---|
| `order_date=2026-06-19` | `2026-06-19` | 2026-06-19 |
| `order_date=2026-06-19.__tmp__` | `2026-06-19.__tmp__` | **2026-06-19** |

Bộ parse DATE của DuckDB đọc được `2026-06-19` ở đầu chuỗi thì dừng, phần
`.__tmp__` bị nuốt. Không lỗi, không cảnh báo.

**Luật rút ra: staging phải nằm ngoài tầm với của glob mà người đọc dùng.** Đổi
tên thành `.staging_...` không cứu được — dấu chấm đầu tên chỉ giấu file khỏi
`ls` trên Linux, `*` của DuckDB vẫn khớp. Nên layout đúng là:

```
lake/
  orders/          ← người đọc glob ở đây
  .staging/        ← anh em, ngoài tầm glob
  .trash/          ← anh em, ngoài tầm glob
```

Không phải `lake/orders/.staging/`.

Ghi chú thêm: `order_date=__HIVE_DEFAULT_PARTITION__` có sẵn trong lake từ A02
thì KHÔNG gây chuyện này — DuckDB nhận diện tên đặc biệt và trả `NULL`, mà
`NULL = DATE '...'` không bao giờ đúng. Chính partition đó là lý do A08 viết
`COALESCE(..., DATE '{d}')`.

---

## 4. Swap không atomic — nói thẳng ra

Thay một thư mục đang sống cần **hai** lần rename:

```
rename 1: live    → .trash
rename 2: .staging → live
```

Giữa hai dòng đó partition không tồn tại. `--nap 2` phóng to quãng này và đo
được **2,05 giây** `count=0` — khớp chính xác.

Swap rút cửa sổ từ 1,9 s xuống 4 ms. Nó **không** đưa về 0.

### Phía đọc bù phần còn lại

`read_with_retry()`: coi `0` và lỗi IO là "đang swap, đọc lại", 15 lần × 0,3 s.
Chạy lại `--nap 2 --retry` → reader in **một dòng duy nhất**, khoảng trống 2
giây vẫn còn nguyên ở phía ghi.

Cách này dùng được vì ta BIẾT partition có dữ liệu. Số 0 không phải câu trả
lời, nó là dấu hiệu đang ở giữa swap.

**Phải làm cả hai bên:**

| Phía ghi | Phía đọc | Kết quả |
|---|---|---|
| naive 1,9 s | không retry | thấy `0` — Task 3 |
| naive 1,9 s | có retry | cứu được, nhưng người đọc chờ gần 2 giây |
| swap 4 ms | không retry | gần như không trúng — "gần như" không phải "không bao giờ" |
| swap 4 ms | có retry | trúng cũng đọc lại là xong |

Dòng ba dễ chủ quan. 4 ms nghe an toàn, nhưng 50 dashboard poll liên tục × 30
partition swap mỗi ngày thì xác suất cộng dồn không còn nhỏ, và khi trúng thì
reader không có cách nào biết mình đang cầm số sai.

Dòng hai cho thấy retry một mình cũng không đủ: rebuild 30 giây ở full sẽ vượt
hạn mức 4,5 giây, retry hết lượt và vẫn trả 0.

---

## 5. Crash giữa hai lần rename

`--crash` gọi `os._exit(1)` ngay sau rename 1 — giết process lập tức, không
chạy `finally`, không dọn dẹp. Hậu quả:

| Thư mục | Trạng thái |
|---|---|
| `orders/order_date=2026-06-19` | **không tồn tại** |
| `.staging/order_date=2026-06-19` | bản mới, đầy đủ |
| `.trash/order_date=2026-06-19.1788946831` | bản cũ, đầy đủ |

Partition biến mất và **không tự khỏi** — đứng nguyên vậy cho tới khi có người
sửa.

Sửa được vì **bản mới và bản cũ không bao giờ mất cùng lúc**. Tại mọi thời điểm
trong `swap()`, ít nhất một trong hai giữ một partition đầy đủ.

`recover()` xét theo thứ tự: live có chưa → staging còn không → trash còn
không. Ưu tiên staging vì đó là dữ liệu mới hơn — đi tiếp tốt hơn lùi lại. Chạy
thật: `finished the swap: staging -> live`, số về đúng 179.294.

---

## 6. Warehouse thì atomic thật

Task 8. Trong một transaction: `orders_a09` → `orders_a09_old`, rồi
`orders_a09_new` → `orders_a09`. Connection thứ hai đếm giữa chừng:

```
mid-swap  : 2772677     ← bảng CŨ, đầy đủ và đúng
post-commit: 2769890    ← bảng mới
```

| | Lake (`--nap 2`) | Warehouse |
|---|---|---|
| Người đọc giữa lúc swap | `count=0` suốt 2 giây | số cũ, đúng |
| Hai lần rename | hai bước tách rời | hiện ra cùng lúc sau COMMIT |
| Có khoảng trống không | có | không |

Khác biệt: rename bảng chỉ sửa **catalog** — sổ ghi tên nào trỏ tới dữ liệu
nào. Sổ đó nằm trong transaction nên hai lần đổi tên hoặc cùng hiện ra hoặc
cùng không. Filesystem không có sổ nào bao trùm cả hai thao tác rename thư mục.

**Hai giới hạn:**
- Bảo vệ các CONNECTION tới warehouse. Một PROCESS khác thậm chí không mở nổi
  file khi writer đang giữ — luật một-process-ghi.
- Không giúp gì cho người đọc Parquet. File nằm ngoài catalog.

Hai bảng `orders_a09` là đạo cụ, đọc thẳng từ lake nên không mang cặp lineage
§7.2: file lake không chứa `_data_date`, và không có lần chạy nào được đăng ký
để phát `_run_id`. Bảng `core.orders` thật của A07 thì có đủ.

---

## 7. Versioned directories — cách thứ ba

Task 9. Không sửa thư mục đã publish, một chữ cũng không. Mỗi lần ghi lại thì
publish nguyên một thư mục version mới, rồi `CREATE OR REPLACE VIEW` trỏ sang.

```
copytree mất 0,5s
rebuild trong v2: 1 file -> 1 file, mất 1,3s
count = 179294
```

Từ đây consumer query `core.lake_orders`, **không bao giờ query một đường dẫn
thư mục**. Đổi view là đổi catalog — atomic, giống Task 8.

Trong v2, **xoá tại chỗ là an toàn** — trái ngược hẳn Task 3. Vì chưa ai đọc
`orders_v=<v2>`, view vẫn trỏ v1. Cả thư mục version mới đóng vai trò staging.

Version cũ nằm nguyên trên đĩa: rollback là trỏ view về v1, không phải khôi
phục dữ liệu từ đâu cả. Và câu "hôm qua dữ liệu trông thế nào" trả lời được
bằng cách đọc thẳng `orders_v=<v1>`.

**Cái phải trả là dung lượng.** `copytree` nhân đôi cả lake trong khi chỉ MỘT
partition trên khoảng ba mươi thực sự đổi. Ở small chép (0,5 s) còn nhanh hơn
dựng lại (1,3 s) nên chi phí chưa lộ. Ở full lake nặng gấp 20 lần thì chép
thành hàng chục giây, vẫn chỉ để thay một partition.

Đây là lý do Iceberg và Delta Lake tồn tại: giữ ý tưởng con trỏ version, nhưng
theo dõi từng file trong metadata thay vì sao chép cả cây thư mục. Version N+1
dùng lại mọi file không đổi, chỉ partition được ghi lại mới chiếm thêm chỗ.

---

## 8. `.trash` chỉ lớn dần

Sau buổi: 4 thế hệ ở small, 1 ở full — đúng bằng số lần swap chạy trọn.

Script **không có** bước dọn, và cố ý vậy: xoá bản cũ ngay lúc publish là mất
đường rollback. Nhưng giữ mãi thì mỗi lần ghi lại một partition tốn thêm một
bản đầy đủ trên đĩa.

Cần một chính sách — giữ N thế hệ gần nhất, hoặc xoá thế hệ cũ hơn X ngày.
Không ai làm hộ. Cùng câu hỏi với `orders_v=*`: giữ bao nhiêu version. Iceberg
gọi việc này là expire snapshots, và nó là công việc phải chạy định kỳ chứ
không tự động.

---

## 9. Quyết định

| Tình huống | Chọn | Vì |
|---|---|---|
| Ghi lại partition trong lake, có người đọc | swap thư mục + retry phía đọc | cửa sổ 4 ms không tăng theo dữ liệu |
| Thay bảng trong warehouse | rename trong transaction | catalog cho atomic thật, không hở |
| Cần rollback / đọc lại lịch sử | versioned dir + view | trỏ view, không khôi phục dữ liệu |
| Lake lớn, cần cả hai thứ trên | table format (Iceberg/Delta) | con trỏ version mà không chép cây thư mục |

Naive rewrite: không có tình huống nào. Nó chỉ đúng khi chắc chắn không ai đọc,
mà điều đó thì không chứng minh được.

---

## 10. Nguyên tắc rút ra

1. **Đừng sửa thứ người đọc đang nhìn.** Dựng ở chỗ khuất, publish bằng một
   thao tác không di chuyển dữ liệu.
2. **Staging phải nằm ngoài tầm glob của người đọc.** Anh em của `orders/`,
   không phải con.
3. **`0` không phải câu trả lời khi biết dữ liệu phải có.** Phía đọc từ chối
   tin vào kết quả rỗng là một phòng thủ hợp lệ.
4. **Hai bên cùng chịu trách nhiệm.** Phía ghi thu cửa sổ, phía đọc retry. Một
   mình bên nào cũng còn hở.
5. **Nói thẳng cái mình không làm được.** Swap không atomic. Gọi nó là atomic
   thì lần crash đầu tiên sẽ dạy lại.
6. **An toàn khi crash đến từ việc không bao giờ mất cả hai bản cùng lúc**, chứ
   không phải từ việc thao tác nhanh hơn.
7. **Đo bằng cái người đọc thấy**, không phải bằng thời gian job chạy. Job xanh
   trong khi dashboard trả số 0 vẫn là hỏng.
8. **Rollback phải rẻ hơn khôi phục.** Giữ bản cũ có chi phí đĩa; chi phí đó
   nhỏ hơn dựng lại từ raw lúc 2 giờ sáng.

---

## Observations to carry forward

- Naive rewrite hỏng **dần theo kích thước** — 1,9 s ở 180 k dòng, 28,7 s ở 3,6
  M. Mọi cách hỏng theo kiểu này đều trông ổn trên máy dev.
- `CAST('<date>.<gì đó>' AS DATE)` của DuckDB nuốt phần đuôi im lặng. Đây là
  bẫy chung của hive partitioning, không riêng bài này.
- File dựng lại phải khớp **chính xác** schema các file lake còn lại. `EXCLUDE
  order_date` (giá trị nằm ở tên thư mục) nhưng **GIỮ** `order_ts_clean` (loader
  A02 đã ghi vào mọi file). Thừa hay thiếu cột đều làm mọi lần đọc bằng glob
  chết vì schema mismatch.
- Rename trên Windows thất bại với `PermissionError` khi bất kỳ process nào
  đang mở file bên trong. Cần vòng retry ngắn ở phía ghi, nếu không lỗi đầu
  tiên sẽ giết script đúng vào lúc tệ nhất — sau rename 1, trước rename 2.
- `os._exit()` khác `sys.exit()`: không chạy `finally`, không dọn dẹp. Đó là
  thứ mô phỏng đúng mất điện.
- A09 chạy script kiểu đường dẫn (`python work\a09_atomic_swap\a09_reader.py`)
  chứ không `-m`, vì `from a09_common import` cần `sys.path[0]` là thư mục con.
  Trộn hai kiểu là gãy import.

---

# A10 — Schema evolution: month 2 arrives

shopcore ship export v2 (16/07) và v3 (01/08), báo sau khi đã ship, kèm câu
"should be backwards compatible". Không hề. Bài này: phát hiện drift, phân loại
mức nguy hiểm, gộp ba era vào MỘT canonical schema để hạ nguồn không bao giờ
phải biết dòng đến từ era nào.

---

## 1. Đã làm gì

| Task | Việc | Kết quả |
|---|---|---|
| 0 | `a10_restore_0619.py` — trả partition 06-19 về trạng thái A02 | tiền đề, xem §2 |
| 1 | Sinh month 2 cả hai scale | 69 file + 69 manifest |
| 2 | Cố tình chạy loader A02 vào file v2/v3 | hỏng, thông báo lỗi đánh lạc hướng |
| 3 | Dò era bằng header, phân loại drift | 45/16/8 file, 3 header |
| 4 | Thiết kế canonical schema | 16 cột + cặp lineage |
| 5 | `V3_COLS` + ba aligner | 15 cột giống hệt nhau cho mọi era |
| 6 | `staging.shopcore_orders_canon` + ba phép kiểm | đều đạt |
| 7 | Dựng lại toàn bộ lake theo canonical | small và full, identity đóng |
| 8 | Amendment 2.0.0 + 3.0.0, gate theo era, sửa check A05 | 68 green, 1 unreadable |
| 9 | Viết lại | §8 |

Code: `work/a10_schema_evolution/` — `a10_bootstrap.py`, `a10_canonical.py`,
`a10_restore_0619.py`, `a10_rebuild_lake.py`, `a10_gate_eras.py`,
`a10_explore.py`. Contract: `contracts/shopcore_orders_daily.v2.contract.yaml`,
`.v3.contract.yaml`.

---

## 2. Tiền đề: A09 để lại một cái bẫy

A09 dedupe lấy 889 dòng (small) / 17.853 (full) ra khỏi partition 06-19. Đó là
kết quả ĐÚNG của A09. Nhưng Task 7d đối soát lake với manifest thô, mà manifest
không biết gì về dedupe — migrate từ lake hiện tại sẽ ra 4.056.748 thay vì
4.057.637, và mất thời gian truy một sai lệch do chính mình tạo ra ở bài trước.

`a10_restore_0619.py` dựng lại partition đó từ tám file raw (06-19..06-26, vì
late window là D+7), dời bản cũ sang `.trash` chứ không xoá.

**Luật rút ra: kết quả đúng của một bài có thể là tiền đề sai của bài sau.**
Trước khi đối soát với nguồn ngoài, kiểm xem chính mình đã sửa gì.

---

## 3. Ba era

| Era | Ngày | File | Đổi gì |
|---|---|---|---|
| v1 | 06-01 → 07-15 | 45 | 10 cột quen thuộc |
| v2 | 07-16 → 07-31 | 16 | thêm `currency`, `discount_amount`, `channel` ở CUỐI + đổi nghĩa `order_total` |
| v3 | 08-01 → 08-08 | 8 | `order_id` thành VARCHAR, `payment_method` → `payment_type`, thêm `loyalty_tier`, `items` thêm `disc`, ĐỔI THỨ TỰ CỘT |

Dò era bằng **header**, không bằng kiểu dữ liệu sniff được: kiểu dao động theo
rác lọt vào mẫu, header thì ổn định và rẻ.

### Bảng phân loại

| Thay đổi | Loại | Ồn hay lặng | Vì sao |
|---|---|---|---|
| 3 cột thêm ở cuối (v2) | additive | ồn — nhưng chỉ vì loader ghim 10 cột | sniffer bó tay, báo lỗi dialect |
| `order_total` đổi nghĩa (v2) | semantic | **lặng** | không cột nào đổi; consumer vẫn chạy, vẫn ra số, và sai |
| `payment_method` → `payment_type` | rename | **lặng** dưới `union_by_name` | thành hai cột, mỗi cột nửa NULL, không lỗi |
| `order_id` BIGINT → VARCHAR | type | **lặng** khi join | `'ORD-...'` không bao giờ bằng số |
| `items` thêm `disc` | nested add | **lặng** | struct cast thiếu field thì rơi giá trị, không kêu |
| `loyalty_tier` thêm | additive | ồn (cùng lý do v2) | |
| Đổi thứ tự cột (v3) | reorder | **lặng** với reader theo vị trí | `columns=` map theo VỊ TRÍ; nạp `'USD'` vào `items`, không lỗi |

Sáu trên bảy là lặng.

### Đây còn là sự cố quy trình

`change_management.notice_breaking`: rename hoặc đổi kiểu là MAJOR, phải báo
trước **30 ngày** qua `#shopcore-data-changes`, kèm semver MAJOR và ghi chú
migration. Ngay cả phần thêm cột của v2 cũng cần **7 ngày**
(`notice_additive`). shopcore ship cả hai era rồi mới gửi email.

Trích được điều khoản là thứ biến "dữ liệu hỏng" thành sự cố có chủ sở hữu.

---

## 4. Task 2 — thông báo lỗi đánh lạc hướng

Loader A02 chạy vào file v2 và v3 đều chết với:

> *It was not possible to automatically detect the CSV parsing dialect*

Không một chữ nào về "thừa 3 cột". Ta đưa `read_csv` 10 cột, file có 13, và
sniffer bỏ cuộc bằng một lỗi **nghe như vấn đề delimiter**.

Gate A06 trên cùng file thì nói thẳng:

```
[FAIL] orders_2026-07-16.csv
   - unexpected column(s) ['currency','discount_amount','channel'] - new export
     version? change_management: additive needs a MINOR bump, 7 days notice.
```

Gate so header với contract; loader chỉ đọc số cột nó được bảo đọc và không có
cách nào biết file có bao nhiêu.

Một chi tiết gate còn chưa đúng: với 08-01 nó xếp `payment_type` vào nhóm "cột
thêm, MINOR". Thực ra đó là nửa còn lại của một rename. Gate thấy hai sự kiện
rời rạc (một cột mất, một cột xuất hiện) chứ không thấy quan hệ giữa chúng.

---

## 5. `union_by_name` không phải lời giải

Đọc ba file một ngày mỗi era với `union_by_name=true`:

- `order_id` ra **VARCHAR** — chuỗi của v3 ép cả cột lên. Lịch sử BIGINT hết join
- `payment_method` **và** `payment_type` cùng tồn tại, mỗi cột nửa NULL
- `customer_id` ra DOUBLE, `updated_at` ra TIMESTAMP WITH TIME ZONE — kiểu trôi theo rác

Nó che được phần thêm cột và **giấu mất** rename. Là công cụ thăm dò, không phải
công cụ nạp.

---

## 6. Canonical schema — hai luật thiết kế

### Luật 1: backfill bằng đúng loại "không có"

| Cột | v1 nhận | Vì sao |
|---|---|---|
| `currency` | `'USD'` | **biết**: v1 là USD theo contract |
| `discount_amount` | `0` | **biết**: chưa có cơ chế discount |
| `channel` | `NULL` | **không biết**: không ai đo |
| `loyalty_tier` | `NULL` | **không biết** |

Đo được hậu quả ngay: ngày v1 (06-02) qua được công thức của era v2
(`order_total = items_sum − discount_amount`) với **99,39%**, ngang ngày v2
(99,43%) và v3 (99,39%). Chỉ vì `discount_amount = 0`.

Nếu để `NULL`: phép trừ ra NULL, `abs(NULL) <= 0.01` ra NULL, `FILTER` loại
sạch, cột `ok` của ngày v1 ra **0**. Không lỗi, không cảnh báo.

**Known value thì điền giá trị, unknown mới để NULL.** Hai cái nghe giống nhau,
hậu quả ngược nhau.

### Luật 2: giữ cả hai dạng khoá

Late correction vượt biên giới era: file v3 mang bản sửa cho đơn đang nằm trong
lake dưới dạng BIGINT. Đo trên file 08-01 (small):

```
join thô  (v.order_id = CAST(h.order_id_num AS VARCHAR))  ->   0 / 705
canonical (replace(v.order_id,'ORD-','')::BIGINT = ...)   -> 705 / 705
```

**0 trên 705.** Mỗi bản sửa đó, qua `MERGE` của A08, sẽ được chèn vào như một
đơn hàng MỚI. Đếm trùng, không lỗi ở đâu cả. Đó là lý do `order_id_num` tồn tại.

### Khoá này là loại gì

`order_id` là **natural key** — đến cùng dữ liệu, do producer đúc. Contract gọi
nó "shopcore surrogate key", nhưng surrogate là *đối với shopcore*; từ chỗ ta
ngồi nó là khoá thừa kế, có nghĩa, miễn phí, và **ngoài tầm kiểm soát**. Ta vừa
chứng kiến nó đổi kiểu dưới chân mình.

Cặp `order_id` VARCHAR + `order_id_num` BIGINT không phải bản vá. Đó là
**warehouse-controlled canonical key**: shopcore vẫn giữ phần ý nghĩa (đơn nào),
warehouse sở hữu phần định dạng.

Ba nấc, và chọn nấc nào phụ thuộc **mức tin tưởng khoá của producer sẽ đứng yên**:

- **(a) dùng thẳng khoá producer** — không code, mọi join đọc giống hệt nguồn.
  Vừa vỡ trên hai mặt: đổi kiểu, và late correction hết khớp (0/705).
- **(b) canonical hoá** — đang làm. Một biểu thức deterministic cho mỗi era,
  không thêm state, không lookup, kỹ sư nào cũng suy lại được bằng mắt.
- **(c) surrogate key thật** — warehouse tự đúc id (sequence hoặc hash), giữ
  natural key làm thuộc tính thường. Hạ nguồn tách rời hoàn toàn. Giá phải trả:
  **một lần tra khoá cho mỗi lần nạp**, mà late row khiến lần tra đó phải đụng
  lịch sử chứ không chỉ batch hôm nay; và thêm một chặng mỗi khi debug đối
  chiếu với nguồn. Lưu ý nó KHÔNG mua được gì: lần tra vẫn phải khớp
  `'ORD-0000000123'` với `123`, nên phần canonical hoá không biến mất — nó
  chuyển vào trong bước gán khoá.

Chọn (b). Bằng chứng là drift vừa phân loại ở §3, không phải sở thích.

---

## 7. Task 7 — dựng lại lake

Kế hoạch: migrate month 1 từ **Parquet lake đã có** (không parse lại 45 CSV) →
nạp month 2 từ raw CSV theo ngày → tất cả vào MỘT thư mục versioned mới →
verify → mới đổi view. Pattern A09 số 3.

| | small | full |
|---|---|---|
| manifest (trừ 07-22) | 4.058.440 | 81.169.464 |
| lake rows | 4.057.637 | 81.153.228 |
| dropped | 803 | 16.236 |
| identity đóng | ✓ | ✓ |
| partition | 69 | 69 |
| wall-clock | 37 s | 331 s |

**`APPEND` chứ không `OVERWRITE_OR_IGNORE`.** Mỗi lần COPY đặt tên file lại từ
`data_0.parquet`. Late row khiến ngày D ghi vào partition D−7..D — đúng những
thư mục ngày trước đã ghi. Với `OVERWRITE_OR_IGNORE`, `data_0.parquet` của ngày
D đè lên file ngày trước trong partition dùng chung, im lặng. Đề nói đã thử:
lake 4 triệu dòng ra còn 45 nghìn, không lỗi ở đâu.

Hệ quả: **script dùng `APPEND` thì chạy lại KHÔNG idempotent.** A07 giải quyết
bằng DELETE trước INSERT; ở đây thư mục version mới đóng vai trò đó — mỗi lần
dựng lại phải bắt đầu từ thư mục rỗng.

**69 partition dù chỉ 68 file nạp được.** Các file 07-23..07-29 mang late
correction cho đơn ngày 07-22, nên thư mục `order_date=2026-07-22` vẫn có dữ
liệu — chỉ thiếu phần đơn gốc của chính ngày đó.

**07-22 ghi `failed` vào `ops.etl_runs`, KHÔNG sửa.** Chẩn đoán và sửa file đó
là trọng tâm A11.

**Dropped lệch giữa hai nửa:** month 1 bỏ 553, month 2 bỏ 250 (small). Month 1
nhiều gấp đôi dù chỉ nhiều hơn 45/24 ngày — vì lake A02 giữ
`__HIVE_DEFAULT_PARTITION__`, chỗ chứa những dòng timestamp hỏng. `CANON` parse
lại và loại chúng, nên **lake canonical không còn partition NULL nào**. Đó là
thay đổi hình dạng, không chỉ là một con số.

Lake không nhận cặp lineage §7.2 — nó ở lại `staging`. Parquet dưới `lake/` là
kho suy lại được, không phải bảng warehouse; và nửa month 1 là migration hàng
loạt các file A02 đã ghi, ngày giao của chúng mất rồi. `order_date` không thay
thế được (late row chính là những dòng có ngày file khác ngày đơn). Một lake
sạch lineage đồng đều tốt hơn một lake chỉ đóng dấu nửa mình biết. Cặp đó hạ
cánh xuống `core.orders` ở A11.

---

## 8. Task 8 — tiến hoá contract

### Hai amendment

**2.0.0** (hiệu lực 07-16) — ba cột thêm, VÀ luật reconciliation mới. Thêm cột
thôi thì minor; **viết lại nghĩa của một business rule đang có mới là thứ phá
mọi consumer**: không dòng code nào đổi, consumer vẫn chạy, vẫn ra số, và sai.
Đó là MAJOR, và đúng là lý do điều khoản 30 ngày áp dụng.

Gộp luôn hai clause của amendment v1.4.0 (transport locale, `updated_at`
semantics) — nó kẹt ở `proposed` từ A06 vì shopcore chưa bao giờ ký. Đề xuất lỡ
một chuyến thì đi chuyến sau.

**3.0.0** (hiệu lực 08-01) — breaking trên bốn mặt: rename, đổi kiểu id,
`loyalty_tier`, `items.disc`, đổi thứ tự cột.

Mỗi bản mang `effective_from` riêng, nên gate giữ được cả ba và kiểm mỗi file
theo bản có hiệu lực vào ngày của nó.

Pydantic model chặn ngay ba khoá mới (`extra="forbid"`): phải khai
`transport_locale` trong `Money`, `semantics` trong `Column`, `supersedes` trong
`Contract`. Model làm đúng việc — đó là lý do nó tồn tại.

### Gate phân biệt "drift" với "không đọc nổi"

Gate A06 chạy trên file 07-22 báo:

```
- missing column(s) [tất cả 10 cột]
- unexpected column(s) ['"order_id","customer_id",...,"channel"']
```

Cái cột "thừa" đó là **cả dòng header 13 cột gộp thành tên một cột duy nhất**.
`sniff_csv` không ném lỗi — nó thoái hoá: dòng gãy làm hỏng việc dò dialect,
sniffer chốt một delimiter không có trong dữ liệu.

Gate kết luận "schema drift" cho một file **không drift chút nào** — nó có đúng
13 cột như mọi file v2 khác, chỉ gãy ở mức byte.

Hai chuyện cần hai phản ứng khác nhau. Drift → đòi producer bump version.
Không đọc được → báo sự cố truyền file, vào `quality.structural`.

Dấu hiệu nhận biết: schema sniff được co lại còn **một cột mà tên chứa dấu phẩy
hoặc dấu nháy**.

Kết quả cuối: **68 green, 07-22 unreadable, 0 drift.**

### Bắt được file đặt sai tên

`era_by_header()` suy era từ header thật, tên file chỉ để đối chiếu. Thử với
file v3 nhưng giả vờ tên ngày 07-20:

```
[warn] ngày nói v2 nhưng header nói v3 - file đặt sai tên? Đọc theo v3.
('green', 'v3  shopcore_orders_daily.v3.contract.yaml')
```

Ngày trong tên file là **siêu dữ liệu**, header là **dữ liệu**. Mâu thuẫn thì
tin cái quyết định việc đọc. Tin ngày thì `read_csv` dùng `V2_COLS` cho file v3
— map theo vị trí, không lỗi, `'USD'` nằm trong `items`.

### Check cross-field của A05

Công thức thành `abs(total − (items_total − discount_amount)) > 0.01`, sửa đồng
thời ở `a05_validate_day.flag_rows` và bản ghim trong `tests/test_checks.py`.
Thêm hai case cho era v2 để ghim luật mới. 9 test xanh.

> **Ranh giới cần biết cho A11.** `a05_validate_day.stage_day()` vẫn đọc CSV
> bằng `V1_COLS` và giờ phát `discount_amount = 0` cứng. Với era v1 đó là GIÁ
> TRỊ ĐÚNG (cùng lý lẽ `ALIGN['v1']`), nhưng nghĩa là **bộ check A05 hiện chỉ
> chạy được cho file era v1**. `validate_day()` cũng luôn lấy contract 1.3.0.
> Công thức đã viết sẵn dạng đầy đủ để lúc nối vào
> `staging.shopcore_orders_canon` thì không phải sửa biểu thức nào. Việc nối đó
> thuộc A11, cố ý không làm hôm nay.

---

## 9. Task 9 — giải thích lại

**Cái nào breaking:** rename `payment_method`→`payment_type` (với một chương
trình, đó là một cột biến mất và một cột lạ xuất hiện); `order_id` BIGINT→
VARCHAR (hai dạng không bao giờ so bằng nhau); đổi thứ tự cột (mọi reader theo
vị trí đọc sai); và — không có cột nào đổi — việc viết lại nghĩa `order_total`.

**Cái nguy hiểm nhất dù trên giấy tờ không breaking:** `order_total` đổi nghĩa.
Rename và đổi kiểu ít ra còn để lại dấu vết — cột thiếu, join không khớp, có
thể phát hiện bằng cách đối chiếu schema. Còn `order_total` thì schema y
nguyên, kiểu y nguyên, nullability y nguyên. Gate schema không có gì để bắt.
Consumer nào đối chiếu theo công thức cũ vẫn chạy trơn, vẫn ra số, và báo lệch
trên mọi đơn có discount — mà không có một dòng lỗi nào nói cho họ biết. Nó chỉ
bị phát hiện khi ai đó thấy báo cáo tài chính lệch, có thể là vài tuần sau.

Ứng viên thứ hai là đổi thứ tự cột: nạp `'USD'` vào `items` mà không báo lỗi.
Nhưng cái đó ít ra sẽ lộ ở phép kiểm dữ liệu đầu tiên. Còn `order_total` lệch
vài phần trăm thì trông y hệt rác cấp dòng vốn đã có sẵn.

**Yêu cầu shopcore lần sau:** (1) một cột `schema_version` trong chính file —
tự khai era thì consumer không phải đoán bằng header; (2) báo trước theo đúng
`change_management`, 30 ngày cho MAJOR; (3) **coi việc đổi nghĩa một cột là
MAJOR ngang với đổi kiểu** — điều khoản hiện tại chỉ liệt kê rename, xoá, đổi
kiểu, nên shopcore có thể lập luận rằng thay đổi `order_total` không nằm trong
danh sách. Đó là một khoảng trống cần vá.

---

## Observations to carry forward

- **Drift hiếm khi tự xưng tên.** Nó hiện ra thành một lỗi khó hiểu, không phải
  một lỗi trung thực. "Không dò được dialect" gần như không bao giờ nghĩa là
  delimiter đổi — đếm số cột trước.
- **Sáu trên bảy thay đổi của A10 là loại hỏng lặng.** Loại ồn ào là món quà.
- **`columns=` map theo VỊ TRÍ, không theo tên.** Header chỉ bị bỏ qua. Viết
  schema mới xong thì đọc thử một dòng và kiểm hai giá trị.
- **`union_by_name` là công cụ thăm dò.** Nó giấu rename thành hai cột nửa NULL
  và lặng lẽ đẩy khoá lên VARCHAR.
- **Known value thì điền, unknown mới NULL.** `discount_amount = NULL` cho v1
  phá phép đối chiếu; `channel = 'web'` thì bịa dữ liệu.
- **Late correction vượt biên giới era.** Không có `order_id_num` thì mỗi dòng
  sửa của v3 thành một đơn mới trong upsert — đếm trùng, không lỗi.
- **`APPEND` cho ghi partition theo ngày**, không `OVERWRITE_OR_IGNORE`. Và
  chấp nhận rằng script như vậy không chạy lại được: thư mục version mới là cơ
  chế idempotent, không phải chính câu COPY.
- **Kết quả đúng của bài trước có thể là tiền đề sai của bài sau** (§2).
- **Ngày trong tên file là siêu dữ liệu; header là dữ liệu.** Mâu thuẫn thì tin
  header, nhưng phải cảnh báo.
- **Hai bản dựng lại độc lập ra cùng một con số là bằng chứng.** 803 dropped ở
  small cộng từ 25 lần chạy riêng biệt; 99,4% reconciliation trùng khớp giữa ba
  era và trùng với selftest đo theo đường khác.

---

# A11 — Backfill: replay 69 ngày, và cái ngày hỏng

Nạp lại toàn bộ lịch sử vào `core.orders` bằng chính pipeline hằng ngày. Một
ngày trong đó có file hỏng — phát hiện, chẩn đoán theo lớp, vá, chạy lại đúng
ngày đó. Rồi đo xem tuần tự chậm đến mức nào, vì A12 phải đánh bại con số đó.

---

## 1. Đã làm gì

| Task | Việc | Kết quả |
|---|---|---|
| 1 | Đọc manifest trước khi chạy | 69 file, 4.116.840 dòng, **151 dòng hỏng ở 07-22** |
| 2 | `work/pipeline.py` + `work/runner.py` | smoke test khớp manifest |
| 3 | Backfill lần đầu, small | 68 success, 1 failed, 86,4 s |
| 4 | Chẩn đoán 07-22 theo lớp | 150 dòng cắt + 1 nháy không đóng |
| 5 | Vá, cách ly, chạy lại | 1/69 ngày, xanh trong 0,9 s |
| 6 | Full scale | 69/69, 82.337.467 dòng, **1.004,9 s** |
| 7 | Freshness gate + bằng chứng ledger | xanh/đỏ đúng, 3 query lưu lại |
| 8 | Toán chi phí | ngoại suy giữa hai scale lệch 73% |
| 9 | Postmortem | `work/incidents/2026-07-22_corrupt_export.md` |

Code mới: `work/pipeline.py`, `work/runner.py`,
`work/a11_backfill/a11_freshness_gate.py`, `work/a11_backfill/a11_explore.py`.

---

## 2. Backfill không phải một script riêng

Nó là pipeline hằng ngày, chạy lại trên nhiều ngày quá khứ. Viết một "bản
backfill" riêng của cùng logic là tạo ra hai pipeline rồi để chúng trôi xa nhau
— và bản backfill là bản ít được chạy hơn, tức ít được kiểm hơn.

A07 đã làm phần khó: mỗi ngày nạp lại bao nhiêu lần cũng ra một kết quả. A11
chỉ thêm một vòng lặp mỏng quanh nó.

Bốn việc của runner: lập kế hoạch trước khi động tay (`--dry-run`), nhớ đã làm
gì (đọc `ops.etl_runs`), gặp lỗi thì đi tiếp, và ước lượng chi phí.

### Ghép A07 với A10: năm chỗ đổi

| A07 | A11 | Vì sao |
|---|---|---|
| `load_day(con, scale, day, chaos)` | `load_day(con, day, scale, run_id)` | runner truyền `run_id` xuống để đóng dấu `_run_id` |
| tự mở ledger bằng `open_run()` | **không đụng ledger** | runner sở hữu vòng đời lần chạy |
| `UPDATE` ledger trong transaction | runner update sau khi hàm trả về | xem §3 |
| `all_varchar=true`, một schema v1 | `columns=era_columns(d)` | v3 đổi thứ tự cột, đoán là đoán sai |
| cleaner viết thẳng trong SQL | `CANON` của A10 | một bản, dùng cho cả migrate lẫn load |

**Bảng `core.orders` cũ phải DROP.** Nó còn hình dạng v1 13 cột của A07;
`CREATE TABLE IF NOT EXISTS` thấy nó và bỏ qua, rồi ngày đầu chết với
`Binder Error: 13 columns but 18 values`. Là dữ liệu dẫn xuất — raw + code dựng
lại được.

**Không gọi `open_day()` của A10 trong `load_day`.** Hàm đó tự chạy `read_csv`
lần nữa, và lần đó không có `store_rejects` — 07-22 sẽ lại chết. `src` phải trỏ
vào bảng đã đọc xong; `aligned` vẫn dùng nguyên `ALIGN[era]`.

**`load_day` chỉ ghi warehouse core, KHÔNG ghi lake.** Lake dùng
`COPY ... APPEND`, mà `APPEND` không idempotent: replay 69 ngày qua đó sẽ nhân
đôi 68 ngày A10 đã ghi, lặng lẽ, và không phép kiểm nào trước A15 bắt được.
Lake thiếu đúng một ngày và được vá tay một lần.

---

## 3. Ledger đổi chủ — và cái giá của nó

A07 cố ý đặt `UPDATE ops.etl_runs` **bên trong** `BEGIN...COMMIT`, để ledger
không bao giờ khai thành công cho dữ liệu không tồn tại. A11 tách ra: runner
update sau khi `load_day` trả về.

Đó là một bước lùi nhỏ về tính đúng. Chết đúng giữa `COMMIT` và `UPDATE` thì dữ
liệu đã vào mà ledger còn `running`. Đổi lại, vòng đời lần chạy nằm gọn ở một
nơi, và gọi `load_day` trực tiếp không sinh rác ledger.

Khe đó tự lành: resume mặc định chạy lại **mọi ngày không xanh**, kể cả
`running`. Nhưng `--only-failed` thì **không** — nó lọc đúng cái tên nó nói.
Ctrl-C giữa chừng để lại một dòng `running`, và chỉ resume mặc định nhặt nó lên.

### Ba chế độ

| Chế độ | Chọn gì | Dùng khi |
|---|---|---|
| mặc định | bỏ ngày xanh, chạy mọi thứ còn lại | resume sau khi crash |
| `--only-failed` | chỉ ngày có lần cuối `failed` | sửa xong một sự cố |
| `--force` | tất cả, kể cả ngày xanh | restatement |

---

## 4. Va phải một sự cố có thật: sequence vs max()+1

Lần chạy đầu chết ngay dòng INSERT ledger:

```
ConstraintException: Duplicate key "run_id: 62" violates primary key constraint
```

Hai bộ cấp phát `run_id` cùng chạy trên một bảng. A07 và runner dùng
`nextval('ops.run_id_seq')`; **`RunLog._next_id()` của A10 dùng
`SELECT max(run_id) + 1`**, bỏ qua sequence hoàn toàn. 50 dòng A10 ghi vào đã
đẩy `max` lên 111 trong khi sequence vẫn đứng ở 62.

Khoá chính bắt được ngay — đúng việc của nó. Không có nó thì hai lần chạy khác
nhau mang cùng một id, và `_run_id` trên dữ liệu trỏ nhập nhằng.

Sửa: `DROP SEQUENCE` rồi `CREATE SEQUENCE ... START max+1`, cho cả hai scale
(small 112, full 34).

**`max()+1` còn sai nặng hơn trong môi trường nhiều process**: hai process đọc
cùng một `max` rồi ghi cùng một id. A12 chạy song song, nên chỗ này sẽ quan
trọng.

---

## 5. Chẩn đoán theo lớp, không theo cảm hứng

### Rule zero: đọc lỗi, chép vào journal, rồi từ chối tin nó

```
Invalid Input Error: Error when sniffing file ".../orders_2026-07-22.csv".
It was not possible to automatically detect the CSV parsing dialect
```

Dialect **không hề đổi**. Thông báo lỗi trỏ vào triệu chứng gần chỗ code ném
lỗi nhất, không phải nguyên nhân gốc.

### Lớp 1 và 2 — gạch bỏ bằng bằng chứng, không bằng cảm giác

Preflight vừa chạy **xanh cả bảy check cho đúng ngày này** vài giây trước khi
chết. Môi trường, ổ đĩa, quyền, khoá warehouse — loại hết trong một nốt nhạc.

Đó là phần thưởng của việc gọi preflight **trước mỗi ngày** thay vì một lần lúc
khởi động. Bảy dòng PASS lặp lại 69 lần trông rườm rà cho tới đúng lúc này.

### Lớp 3 — dữ liệu

| Câu hỏi | Trả lời |
|---|---|
| File có tồn tại? | có, 18,9 MB — nằm giữa hàng xóm (18,9 / 19,9 / 19,8) |
| Header nguyên? | nguyên, 13 cột v2 đúng thứ tự |
| Dòng đầu? | bình thường, dấu phẩy và nháy kép chuẩn |
| Manifest nói gì? | `corrupt_rows: 151` |

→ hỏng **cấu trúc ở mức dòng**. Đúng tình huống dùng `store_rejects=true`.

### Bệnh

| error_type | records | dòng |
|---|---|---|
| MISSING COLUMNS | 1.500 | 150 |
| CAST | 150 | 150 |
| UNQUOTED VALUE | 1 | 1 |

- **150 dòng bị cắt giữa chừng.** 1.500 ÷ 150 = đúng 10 cột thiếu mỗi dòng, cắt
  sau cột thứ ba. `CAST` là hệ quả kèm theo — mẩu sót lại không ép được kiểu.
- **1 dòng nháy không đóng** — và đây là mắt xích: nó đầu độc bộ dò dialect của
  **cả file**. Sniffer đọc mẫu, gặp nháy mở không bao giờ đóng, mọi tổ hợp
  delimiter/quote đều không giải thích nổi phần còn lại.

**Bệnh là 150 dòng bị cắt lúc shopcore xuất file; triệu chứng là sniffer bó
tay.**

Hai chỗ dễ sai khi dùng `store_rejects`:

- Phải `CREATE TABLE ... AS`. Một `count(*)` trần bị tối ưu bỏ qua khâu parse,
  bảng reject rỗng, và ta kết luận file không sao.
- Đếm `DISTINCT line`, không đếm bản ghi. Một dòng cắt sinh **một bản ghi cho
  mỗi cột thiếu**: 1.651 bản ghi nhưng chỉ 151 dòng.

---

## 6. Vá: đọc khoan dung, giữ bằng chứng, đối soát

Ba mảnh ghép vào `load_day`.

**`store_rejects=true`** — parse được dòng nào giữ dòng đó, dòng hỏng đẩy sang
`reject_errors` thay vì giết cả file.

**Quarantine** — copy sang parquet kèm `_data_date`, `_run_id`, `_rejected_at`.
Rồi **`DELETE FROM reject_errors`**: bảng đó cộng dồn theo connection, không xoá
thì ngày N+1 copy lại cả reject của ngày N và mọi file quarantine sau đó đều
phình lên, âm thầm sai.

**Reconciliation guard** — so số dòng đọc được với `manifest.rows`, **trên bảng
raw, không phải sau CANON**. CANON bỏ các dòng timestamp không parse nổi nên
bảng sau nó luôn nhỏ hơn manifest vài chục dòng; so sai bảng thì mọi ngày đều
đỏ oan.

Với 07-22: manifest khai 58.400, đọc được đúng 58.400. **151 dòng hỏng nằm
NGOÀI con số producer khai**, nên phép cộng đóng. Nếu shopcore đã tính chúng vào
`rows` thì guard sẽ đỏ — và đó mới là sự cố khác.

**File raw không sửa một byte.** Raw là bằng chứng bất biến, và generator cũng
sẽ tạo lại y hệt. Cái đổi là *cách đọc*.

### Kết quả

```
--only-failed  ->  1 trên 69 ngày được chọn
2026-07-22  OK  58,400 dòng  0.9 s
success  69  4116840
```

0,9 giây để sửa một ngày, so với 86 giây chạy lại cả 69. Đó là giá trị cụ thể
của ledger.

Quarantine truy vết được về đúng lần chạy đã gặp nó:

```
orders_2026-07-22.csv | 2026-09-15 14:04:04 | success | 58400 | 151
```

Một phép join giữa parquet và `ops.etl_runs` trên `_run_id`. Parquet **không**
lưu tên file và không cần: `_run_id` dẫn tới ledger, và ledger có tên file.

### Vá lake, một lần, bằng tay

| | trước | sau | thêm |
|---|---|---|---|
| small | 4.057.637 | **4.116.024** | 58.387 = 58.400 − 13 |
| full | 81.153.228 | **82.320.994** | 1.167.766 = 1.168.003 − 237 |

Cả hai vẫn 69 partition.

**Bẫy suýt dính:** `staging.shopcore_orders_day_raw` bị ghi đè mỗi ngày. Sau
lần chạy full, nó đang giữ **08-08**, không phải 07-22 — COPY lúc đó sẽ ghi
nhầm ngày vào lake, và `APPEND` thì không hoàn tác được. Phải
`--force` nạp lại đúng 07-22 trước, và thêm một `assert` kiểm `_data_date`
trước khi ghi.

---

## 7. Freshness gate: người canh chạy theo đồng hồ

**Runner chỉ nhận ra file thiếu nếu có ai đó chạy nó.** Một chuyến giao không
bao giờ đến thì không gây lỗi ở đâu cả, vì công việc lẽ ra phải thất bại đã
không bao giờ khởi động. SLA cần watchdog chạy theo đồng hồ, không theo yêu cầu.

Phân công với A05: `a05_freshness_check.py` dò `max(_data_date)` của
`core.orders` — nhìn warehouse từ **bên trong**, hỏi "kho có cũ không".
`a11_freshness_gate.py` nhìn thư mục giao của shopcore từ **bên ngoài**, hỏi
"nguồn có giao không". Hai cảm biến, cùng đổ về `ops.alerts`.

Đáng giữ phân biệt đó: kho cũ có thể vì nguồn không giao, mà cũng có thể vì
pipeline chết — hai nguyên nhân, hai người phải gọi.

`due_through()` tồn tại thay vì viết thẳng `today - 1` vì **cùng một ngày lịch
cho hai câu trả lời khác nhau tuỳ giờ**. File của D đến hạn vào D+1 lúc 04:00
UTC; chạy lúc 03:00 mà báo động thì sẽ bị tắt chuông trong một tuần.

Demo, không đụng file nào — chuyến giao cuối là 08-08 nên đẩy đồng hồ thêm một
ngày chính là ngày thiếu:

```
--as-of 2026-08-09T04:30  ->  fresh - mọi file tới 2026-08-08 đã được giao
--as-of 2026-08-10T04:30  ->  ALERT freshness  orders_2026-08-09.csv missing
                              exit code 1, một dòng ops.alerts, run_id NULL
```

`run_id` NULL **chính là điểm mấu chốt**: mọi alert khác sinh ra từ một run
đang chạy; alert này sinh ra từ một run không bao giờ xảy ra.

Ba tầng phải cùng nói một điều: console, `ops.alerts`, và **mã thoát**. In màn
hình đỏ mà vẫn `exit 0` là chuông báo cháy không lắp pin.

**Một lỗi cũ phát hiện được nhân tiện:** `write_alert` của A07 dùng tên cột
nhưng bỏ sót `source`, nên 5 alert của nó đều `source = NULL`. Lọc theo `source`
sẽ bỏ sót chúng. Chưa sửa.

---

## 8. Ledger là bằng chứng

Ba query, ba câu hỏi của postmortem.

**Khi nào biết** — số lần thử mỗi ngày: đúng một dòng, `2026-07-22 | 2 | 1`.
Không ngày nào khác cần thử lần hai. Dấu vân tay của sự cố cục bộ.

**Mất bao lâu để sửa** — dòng thời gian: `failed` 10:57:10 (114 ms) →
`success` 14:04:05 (0,9 s). **Thời gian phát hiện gần bằng 0; thời gian sửa là
3 giờ**, và gần hết trong đó là chẩn đoán với viết code.

Lần thử hỏng có `rows = NULL`, không phải 0. Đúng — 0 là một lời khẳng định,
NULL là sự thật: lần đó chưa đếm được gì.

**Lan tới đâu** — biểu đồ dòng theo ngày: cao nguyên 57–62k, hõm cuối tuần
43–47k, hai cột gấp ba ở 06-19 và 07-11, không hố nào, không cột lạ nào. Sự cố
chạm đúng một ngày.

Toàn bộ đọc từ **69 dòng ledger**, không phải 4 triệu dòng dữ liệu. Và
`source_file` ghi ở lệnh `INSERT` chứ không phải `UPDATE` — một ngày thất bại
vẫn phải nói được nó nghẹn ở file nào.

---

## 9. Toán chi phí: ngoại suy giữa hai scale thì sai

| | small | full | tỉ lệ |
|---|---|---|---|
| dòng | 4.116.840 | 82.337.467 | 20,0× |
| wall clock | 86,4 s | **1.004,9 s** | 11,6× |
| dòng/giây | 47.354 | 81.960 | 1,7× |

**1.004,9 giây là baseline chính thức cho A12.** Phải xuống dưới 335 giây.

Ngoại suy: 82.337.467 ÷ 47.354 = **1.739 s**. Thực tế **1.005 s**. Dự đoán sai
73%, và sai theo hướng *quá bi quan*.

Ba lý do:

1. **Chi phí cố định mỗi ngày không co giãn.** Preflight bảy check, mở
   connection, `CREATE TABLE IF NOT EXISTS`, `BEGIN/COMMIT` — cùng một khoản ở
   cả hai scale, cùng 69 ngày, nhưng ở small nó chia cho 60k dòng còn ở full
   chia cho 1,2 triệu.
2. **File lớn stream hiệu quả hơn.** DuckDB đọc theo vector, 8 luồng. File 18 MB
   không đủ việc để 8 luồng bận; file 360 MB thì có.
3. **Hai ngày cao điểm kéo trung bình LÊN, không kéo xuống.**

Bảng năm ngày chậm nhất ở full cho thấy nghịch lý đó rõ nhất:

| ngày | dòng | giây | dòng/giây |
|---|---|---|---|
| 06-19 | 3.643.024 | 41,2 | **88.455** |
| 07-11 | 3.602.310 | 34,0 | **106.031** |
| 08-05 | 1.140.990 | 24,3 | 47.001 |
| 08-06 | 1.222.852 | 22,9 | 53.458 |
| 08-07 | 1.251.587 | 22,4 | 55.924 |

Ngày cao điểm chạy **nhanh hơn** ngày thường tính trên mỗi dòng, dù tốn nhiều
thời gian tuyệt đối hơn.

Ước lượng chi phí cố định: nếu 106.000 dòng/giây là tốc độ thật khi đã chạy,
82,3 triệu dòng cần ~780 s làm việc thật. 1.005 − 780 ≈ **225 s chi phí cố
định**, tức 22% tổng thời gian, ≈ 3,3 s mỗi ngày.

**Đó chính là chỗ A12 nhắm vào.** Chạy 4 ngày song song không làm SQL nhanh
hơn, nhưng 225 giây đó bị chia ra. Và 14,6 s/ngày trên máy 8 luồng mà CPU không
chạm trần nghĩa là phần lớn thời gian máy đang rảnh.

**Luật rút ra: ngoại suy trong cùng scale, đừng ngoại suy giữa hai scale.** Ở
production thật, người ta chạy một chunk nhỏ **trong đúng môi trường đích** rồi
mới ngoại suy, không đo trên máy dev.

---

## 10. Ba câu về phép lịch sự khi backfill

**Chunking — vì sao chia 69 ngày thành 10 chunk tuần thay vì chạy một phát.**
Ba lý do, và không lý do nào là tốc độ. Thứ nhất, bán kính của một lần hỏng:
chạy một phát mà sai logic ở ngày thứ 40 thì 40 ngày phải làm lại; chia chunk
thì mất nhiều nhất một tuần. Thứ hai, áp lực đĩa và `tmp` — spill dồn lại trong
một lần chạy dài, chia chunk cho ổ đĩa thở giữa các đợt. Thứ ba, và đây mới là
lý do thật: sau chunk đầu tiên **có cơ hội nhìn kết quả bằng mắt** trước khi
đốt thêm ba tiếng. Một `SELECT count(*) GROUP BY order_date` sau tuần đầu bắt
được lỗi mà không phép kiểm tự động nào nghĩ tới. Cái giá phải trả là 10 lần
gõ lệnh thay vì 1, và ledger làm cho chuyện đó không tốn gì — mỗi chunk tự biết
nó nối tiếp từ đâu.

**Marts dẫn xuất — backfill xong rồi chúng có tự đúng không.** Không. `core.orders`
vừa nhận thêm 69 ngày, nhưng mọi bảng dẫn xuất từ nó vẫn giữ kết quả tính từ
dữ liệu cũ. Chúng không sai một cách ồn ào — chúng vẫn chạy, vẫn trả về số, và
số đó thiếu 69 ngày. Đây đúng là kiểu hỏng lặng đã gặp ở A10 với `order_total`.
Nên backfill chưa xong khi `core.orders` xanh; nó xong khi mọi thứ đọc từ
`core.orders` đã được dựng lại. Thứ tự bắt buộc là từ dưới lên: core trước, rồi
mart tầng một, rồi mart tầng hai. Dựng mart trước khi core xong thì mart đóng
băng một trạng thái nửa vời. Và nếu mart dựng theo kiểu incremental thì phải
biết ngày nào đã đổi — đó lại là `ops.etl_runs`.

**Thông báo — backfill trên hệ thống dùng chung.** Trước khi chạy, nói cho
người dùng warehouse biết ba điều: chạy lúc nào, đụng vào bảng nào, và trong
lúc đó số liệu sẽ trông thế nào. Điều thứ ba là điều hay bị bỏ quên và là điều
gây hoảng nhất: một dashboard đọc `core.orders` giữa chừng backfill sẽ thấy dữ
liệu tăng dần theo từng ngày được nạp — không sai, nhưng trông như đang có
chuyện. Nếu không muốn ai thấy trạng thái nửa vời thì phải nạp vào bảng khác
rồi đổi tên một lần, đúng cách A09 làm với partition. Sau khi chạy, báo lại
kết quả: bao nhiêu ngày, bao nhiêu dòng, có ngày nào cần chú ý không. Im lặng
trước và sau đều đắt hơn một tin nhắn.

---

## Observations to carry forward

- **Backfill là pipeline hằng ngày chạy lại, không phải script riêng.** Hai bản
  của cùng một logic sẽ trôi xa nhau, và bản ít chạy hơn là bản ít được kiểm hơn.
- **Preflight gọi trước MỖI ngày, không một lần lúc khởi động.** Giá trị của nó
  hiện ra đúng lúc có sự cố: gạch hai lớp chẩn đoán bằng bằng chứng.
- **`APPEND` không idempotent.** Mọi script dùng nó đều không chạy lại được. Cơ
  chế idempotent phải nằm ở tầng khác (thư mục version mới, hoặc DELETE trước).
- **Bảng staging bị ghi đè mỗi ngày.** Trước khi COPY từ nó, kiểm `_data_date`
  bằng `assert`. `APPEND` không hoàn tác được.
- **`reject_errors` cộng dồn theo connection.** Không `DELETE` thì ngày sau
  copy lại reject của ngày trước.
- **Đếm `DISTINCT line`, không đếm bản ghi reject.** Một dòng cắt sinh một bản
  ghi cho mỗi cột thiếu.
- **`store_rejects` cần `CREATE TABLE ... AS`.** `count(*)` trần bị tối ưu bỏ
  qua khâu parse và bảng reject sẽ rỗng.
- **Đối soát trên bảng RAW, trước khi làm sạch.** Sau khi làm sạch thì số luôn
  nhỏ hơn manifest và mọi ngày đỏ oan.
- **Sequence và `max()+1` không dùng chung được** trên một bảng. `max()+1` còn
  sai nặng khi nhiều process — A12 sẽ đụng.
- **`rows = NULL` khác `rows = 0`.** 0 là khẳng định; NULL là chưa đếm được.
- **Thông báo lỗi trỏ vào triệu chứng gần chỗ ném lỗi nhất**, không phải nguyên
  nhân gốc. Chép lại, rồi từ chối tin nó.
- **Tín hiệu có sẵn mà không ai đọc thì bằng không có.** `corrupt_rows: 151`
  nằm trong manifest suốt từ A10.
- **Ngoại suy trong cùng scale.** Chi phí cố định mỗi ngày không co giãn theo
  dữ liệu, nên tỉ lệ dòng/giây của small không nói được gì về full.

# A12 — Parallelization

Máy: 8 core, 16 GB RAM. Dữ liệu ở `D:\etl_lab`. Nhánh `a12-parallelization`.

---

## Task 1 — Ngân sách trên giấy

Hai bất đẳng thức, và không có gì trong DuckDB hay `multiprocessing` kiểm hộ cả.
`memory_limit` là giới hạn **của từng process**; tổng là việc của mình.

```
workers × threads          ≤ 8 core
workers × mem + 4GB (OS)   ≤ 16 GB
```

| cfg | mem/worker | thread | RAM hứa | hợp lệ |
|---|---|---|---|---|
| 1×8 | 8 GB | 8 | 8 GB | có |
| 2×4 | 5 GB | 8 | 10 GB | có |
| 4×2 | 3 GB | 8 | 12 GB | có |
| 8×1 | 1,5 GB | 8 | 12 GB | có |
| 8×8 | 1,5 GB | **64** | 12 GB | không — vi phạm core |
| 8×1 | 12,8 GB | 8 | **102 GB** | không — vi phạm RAM |

Hai dòng cuối sai theo hai kiểu khác nhau: một cái đốt core vào context switch,
một cái hứa RAM không tồn tại.

---

## Task 2 — Một writer, và chỉ một

Terminal 1 giữ `warehouse.duckdb`, terminal 2 xin vào → `IOException`.
Thử lại với `read_only=True` → **cũng hỏng**.

Đó mới là phần đáng nhớ. Luật không phải "một writer, nhiều reader" mà là
**writer giữ file thì không ai vào**.

Hệ quả cho cả bài: worker không thể ghi warehouse, kể cả chỉ đọc một thứ nhỏ.
Nên kiến trúc chỉ còn một hình dạng khả dĩ — worker viết parquet ra file riêng,
một committer gom lại. Đó không phải lựa chọn phong cách, đó là thứ duy nhất
chạy được.

---

## Task 3 — Pool, small scale

Chạy thử month 1 (45 ngày v1), 4×2, mem 3 GB: **13,1 s**, peak RSS 0,4 GB,
staged 2.773.013 dòng.

Thứ tự ngày in ra không theo lịch — dấu hiệu `imap_unordered` đang chạy song
song thật.

Cả 69 ngày: staged **4.116.024**, manifest 4.116.840, dropped **816**.
816 dòng bị bỏ là các dòng timestamp không parse nổi mà `CANON` vứt — nhiều hơn
803 của A10 đúng 13, vì lần này 07-22 cũng được stage (`store_rejects=true`) và
13 dòng của nó nhập vào tổng.

---

## Task 4 — Committer, và phát hiện thật của hôm nay

```
staged rows      : 4,116,024
core.orders_par  : 4,053,208
_data_date       : 69 / 0 NULL
commit wall      : 19,0 s
```

| pha | wall |
|---|---|
| stage (4 worker, 69 ngày) | 20,8 s |
| commit (1 process) | **19,0 s** |
| tổng | 39,8 s |

**Committer chiếm gần một nửa và hoàn toàn tuần tự.** Tôi vừa song song hoá cái
pha mà giờ chỉ còn là một nửa bài toán — định luật Amdahl đập vào mặt ngay lần
chạy đầu. Thêm worker vào stage sẽ không bao giờ đưa tổng xuống dưới 19 s.

Điều này quyết định cách đọc Task 5: bảng thực nghiệm **chỉ đo pha stage**.
8 worker kéo stage xuống 12 s trông như thắng lớn, nhưng tổng chỉ từ 39,8 còn 31.

`4.053.208` thấp hơn `expected_unique` (4.054.002) đúng **794**: với 794 order,
bản duy nhất của nó lại chính là dòng timestamp hỏng đã bị vứt. Mất hẳn order,
không phải mất một bản sao. Đếm và giải thích, không ép cho bằng.

---

## Task 5 — Bảng thực nghiệm (small, 69 ngày)

| cfg | mem | wall | peak RSS | speedup |
|---|---|---|---|---|
| 1×8 | 8 GB | 28,8 s | 0,2 GB | 1,00× |
| 2×4 | 5 GB | 26,3 s | 0,3 GB | 1,09× |
| **4×2** | **3 GB** | **16,0 s** | 0,5 GB | **1,80×** |
| 8×1 | 1500 MB | 17,6 s | 0,7 GB | 1,64× |

Baseline là **1×8**, không phải 1×1: A11 đã dùng cả 8 core bên trong mỗi query
rồi, so với 1×1 là tự tâng bốc.

Ba điều đọc ra:

- **8×1 thua 4×2.** Mỗi worker 1 luồng là mất hẳn phần song song *bên trong* mỗi
  query; phần thêm được từ nhiều process không bù nổi.
- **RSS tăng đều theo số process** (0,2 → 0,7 GB). Mỗi process trả một khoản cố
  định cho Python + DuckDB runtime, bất kể nó làm gì.
- **Đường cong 1,00 → 1,09 → 1,80 → 1,64 có đỉnh.** Lợi ích không tuyến tính.

Chọn **4×2, mem 3 GB**.

---

## Task 6 — Hai lần phá hoại có kiểm soát (full, 7 ngày 06-15→06-21)

Mốc so: 4×2 hợp lệ → **35,3 s**, peak RSS 1,0 GB.

### 6.1 Oversubscription — 8×8 (64 thread trên 8 core)

**56,8 s, peak RSS 4,0 GB. Chậm hơn 61%** — không phải "không nhanh hơn" mà tệ
hơn hẳn.

Chi tiết đáng giá nhất nằm ở cột giây từng ngày, không phải ở wall. Ở 8×8 **mọi
ngày thường đều mất ~43,8 s** — kể cả ngày nhỏ nhất (864k dòng) lẫn ngày to gấp
rưỡi (1,2 triệu). Ở 4×2 chúng chênh rõ: 8,1 s đến 17,0 s.

Nghĩa là khi 64 luồng tranh 8 core, **thời gian một ngày không còn phụ thuộc vào
ngày đó nữa** — nó bằng thời gian cả lô chia đều. Chữ ký của context switching:
máy không làm xong việc gì trước, nó làm dở dang tất cả.

Và RSS 1,0 → 4,0 GB **dù `memory_limit` mỗi worker giảm** từ 3 GB xuống 1,5 GB.
64 luồng, mỗi luồng cần buffer riêng. **Thread không miễn phí về RAM** — một
khoản `memory_limit` không đếm.

### 6.2 Overcommit — 8×1, mem 12 GB (hứa 96 GB trên máy 16 GB)

Chạy êm. **Peak RSS 1,1 GB.** Máy không hề hấn gì.

`memory_limit` là **trần, không phải chỗ đã đặt**. DuckDB không giữ trước RAM,
nó xin khi cần. 7 ngày, mỗi worker 1 luồng, streaming CSV → parquet — khối lượng
thật chỉ ~140 MB/worker, nên trần 12 GB hay 1,5 GB không khác gì nhau.

Overcommit là một lời hứa, và lời hứa chỉ bị đòi khi có ai thực sự cần. Nó sẽ nổ
ở query phải giữ dữ liệu trong bộ nhớ: hash join lớn, sort toàn bộ, window trên
cả bảng — tức là chính cái committer làm.

Bất ngờ thứ hai: **62,0 s, chậm hơn cả 8×8.** Với 1 luồng/worker, 06-19 mất
**61 s** và một mình định ra vạch đích; 7 worker kia xong từ giây 29 rồi ngồi
chơi. Đây mới là straggler thật sự.

---

## Task 7 — Single query: một process, một query, cả danh sách file

| era | files | wall | rows |
|---|---|---|---|
| v1 | 45 | 18,2 s | 2.773.013 |
| v2 | 16 | 8,6 s → 3,7 s (`--relax`) | 849.030 → 907.417 |
| v3 | 8 | 3,2 s | 435.594 |

`2.773.013 + 907.417 + 435.594 = 4.116.024` — **đúng bằng pool**. Và chênh hai
lần chạy v2 là `907.417 − 849.030 = 58.387`, chính xác con số pool báo cho 07-22.
Hai đường độc lập, ba lần cắt khác nhau, khớp tới từng dòng. Đây là một **oracle**
để kiểm chéo, không chỉ một phép đo tốc độ.

| | wall |
|---|---|
| pool 4×2, 69 ngày | **16,0 s** |
| single query, ba era cộng lại | 25,1 s |

Pool thắng. Single query gần đúng bằng tuần tự.

Ba thứ học được khi viết nó:

- **`_data_date` sụp khi một query nuốt 45 file.** Pool có nó miễn phí vì mỗi
  worker biết mình đang làm ngày nào. Ở đây thông tin đó chỉ còn nằm ở tên file →
  `filename=true` rồi `CAST(filename[-14:-5] AS DATE)`. Lát cắt DuckDB đếm từ 1
  và lấy cả hai đầu, khác Python.
- **Phải chèn vào cả hai tầng.** `ALIGN` và `CANON` đều liệt kê cột tường minh;
  chèn một tầng thôi thì SQL vẫn chạy và cột biến mất không một lời nào.
- **`store_rejects` áp cho toàn bộ danh sách.** Một `read_csv` ăn 16 file thì
  không có chỗ nào nói "riêng file này đọc khác". Hai nước, cả hai tệ hơn pool:
  bật relax cho cả era (mất strict cho 15 file lành), hoặc loại ngày hỏng ra
  (thiếu một ngày). Chọn loại ra: thà thiếu một ngày còn hơn nới luật cho 15 ngày.

**Cảnh báo về con số 3,7 s:** nó chạy *sau* lần 8,6 s trên cùng 16 file nên ăn
trọn cache đĩa còn nóng. Đừng đọc là "relax nhanh hơn 2,3×". Đúng cái bẫy
"tin một phép đo đơn lẻ", gặp ngay trong dữ liệu của chính mình.

Chọn single query khi: một era, không file gãy, cần code ngắn, chấp nhận
restatement. Chọn pool khi: nhiều era, có ngày lỗi, cần retry từng ngày và state
trong `ops.etl_runs` — tức gần như mọi lúc trong production.

---

## Task 8 — Skew và thứ tự xử lý

| bối cảnh | `--order date` | `--order size` | chênh |
|---|---|---|---|
| small, 69 ngày, 4×2 | 16,0 s | 16,3 s | **0%** |
| full, 7 ngày, 4×2 | 35,3 s | 29,6 s | **−16%** |

Cùng một tối ưu, cùng một cấu hình, hai kết quả trái ngược. Công thức:

> `--order size` ăn tiền khi (thời gian ngày to − ngày thường) lớn so với tổng
> wall. Small/69 ngày: 0,9 s trên 16 s ≈ 6%, hàng đợi 17 ngày/worker tự cân bằng.
> Full/7 ngày: 19 s trên 35 s, mỗi worker chỉ 2 ngày — không còn gì để cân bằng,
> thứ tự trở thành tất cả.

Nói ngắn: **hàng đợi càng nông, thứ tự càng quan trọng.** Backfill dài tự lo
được; backfill ngắn thì không.

Xếp theo `bytes` chứ không theo `rows`: chi phí staging là chi phí đọc và parse
văn bản, mà cái đó tỉ lệ với byte. Một ngày nhiều dòng ngắn rẻ hơn một ngày ít
dòng nhưng `items` JSON dài.

*(Về cache: lần chạy sau nhanh hơn, mà `size` là lần sau — nếu có thiên vị thì
nó thiên vị có lợi cho `size`, vậy mà vẫn hoà ở small. Kết luận chắc thêm.)*

---

## Task 9 — Full scale, 69 ngày

```
stage 4×2 --order size : 233,9 s   peak RSS 1,2 GB
  staged 82.320.994 / manifest 82.337.467 → dropped 16.473
commit                 : 418,7 s
  core.orders_par 81.063.548, _data_date 69 / 0
```

| | s |
|---|---|
| stage | 233,9 |
| commit | **418,7** |
| **tổng** | **652,6** |
| A11 baseline | 1.004,9 |
| **speedup** | **1,54×** |

**Không đạt đích 3×.** Ghi con số này trước khi sửa gì — một lần chạy trung thực
không đạt đích có giá trị hơn một lần chạy đạt đích sau khi đã biết đáp án.

Ba câu chẩn đoán:

- **Nghẽn đĩa?** Không — stage 233,9 s cho 24 GB CSV, peak RSS 1,2 GB, CPU có việc.
- **Quá ít worker?** Không — 4×2 là cấu hình tốt nhất đo được.
- **Committer nuốt hết?** **Đúng.** Nó được trọn 8 GB/8 luồng mà vẫn 418,7 s, chiếm
  **64% tổng thời gian** và hoàn toàn tuần tự. Đúng như Task 4 đã cảnh báo ở small.

Vì sao một `row_number()` mất 418 s: nó phải **sắp xếp 82 triệu dòng theo
`order_id`** để gom các bản sao — một phép toàn cục.

Hướng sửa (chưa làm): nếu hai bản của cùng một `order_id` luôn có cùng
`order_date` — bản gốc ngày D và correction ngày D+3 vẫn thuộc về đơn hàng ngày
D — thì `PARTITION BY order_id` không bao giờ cần nhìn xuyên qua ranh giới
`order_date`, và phép sắp xếp toàn cục kia là thừa.

---

## Ghi chú kỹ thuật

**Windows spawn.** Process con khởi động bằng cách **re-import** file. Kéo theo:
`SCALE` phải là biến module-level (parse ở `__main__` thì con không thấy);
`load_day()` phải ở top-level (con tìm theo tên, không nhận closure); code khởi
động pool phải nằm dưới `if __name__ == "__main__"`, bỏ đi là bom fork.

**`enable_progress_bar=false` trong worker.** DuckDB tự vẽ thanh tiến trình khi
query chạy >2 s và stdout là terminal. Với N worker, N thanh cùng ghi vào **một**
stdout — dòng chèn lẫn nhau, và con số elapsed cạnh một ngày lại thuộc về worker
khác. Log song song phải do một process in. *(Đã gặp trong thực tế ở lần chạy 8×1:
dòng `100% ?███...?` chen ngang giữa hai ngày.)*

**Một file mỗi ngày, không `PARTITION_BY` trong worker.** Ngày D mang correction
cho D−7..D, nên hai worker sẽ cùng ghi vào thư mục `order_date=...` và đè lặng lẽ
file của nhau — đúng cái bẫy A02. Không chia sẻ thư mục = không cần khoá. Việc
partition để committer làm, một mình.

Cùng câu lệnh `PARTITION_BY` đó lại **an toàn** trong single query của Task 7, vì
ở đó chỉ có một writer trên đời. Kỹ thuật không tốt hay xấu — nó đúng hoặc sai
tuỳ số writer.

**Committer lấy trọn 8 GB / 8 luồng.** Một job song song có nhiều **pha**, và mỗi
pha được ăn cả máy. Chia 4 GB cho committer chỉ vì "worker cũng cần" là chia cho
những process đã chết.

**`PARTITION BY order_id`** — cột VARCHAR `'ORD-...'` mà A10 chuẩn hoá, không phải
`order_id_num`. Đây là chỗ A10 trả lãi: correction era v3 mang id `'ORD-0000123456'`
phải rơi cùng nhóm với bản gốc v1 mang id `123456`.

# A13 — Resource monitoring & optimization

Tự viết một cái monitor bọc quanh process (psutil), rồi dùng nó để trả lời 5 câu: pipeline đang tốn gì, spill nằm ở đâu, biên OOM ở đâu, Parquet đỡ IO bao nhiêu, và cỡ máy đúng thì đáng bao nhiêu tiền.

---

## Số liệu

| Task | Kết quả |
|---|---|
| 1 — build monitor | peak RSS 0,3GB, 7 mẫu, CPU 4% |
| 2 — SQL trên monitor.csv | khớp resmon, đỉnh RSS ở giây đầu |
| 3 — seq vs pool | seq 1 ngày: 23s/0,96GB. Pool 7 ngày: 27,4s/0,98GB, đọc 2,25GB. Ngân sách giấy 12GB, thực tế dùng 1GB |
| 4 — spill lab (8/4/1GB) | 8GB: 691,5s, spill 15,7GB. 4GB: 933,8s, spill 19GB. 1GB: OOM sạch, spill 20,3GB |
| 5 — spill nằm đâu + blast radius | 5 file, 0,27GB, đúng chỗ (`small/tmp`), PASS. Ổ C còn 6,8GB free → 0 lần chạy song song là an toàn |
| 6 — near-OOM cliff (pandas) | 0,63GB/ngày → 69 ngày ≈ 43GB. Guard chặn sau 2 ngày, chặn vì RAM hệ thống cạn |
| 7 — IO: CSV vs Parquet | CSV đọc 10,49GB, Parquet đọc 0,04GB — ít hơn 267 lần |
| 8 — right-size | chạy 2 lần, biên OOM y nguyên cả 2: 512MB ok / 256MB chết |
| 9 — tiền | tốc độ (A12) +$14/năm, xuống tier máy nhỏ +$13/năm |

---

## Rút ra được gì

**Monitor phải cache object `Process`, không tạo mới mỗi lần đo.** `cpu_percent()` so với lần gọi trước trên cùng object — object mới toanh luôn báo 0%, trông như máy đang rảnh trong khi 8 core đang cháy.

**IO byte đọc không đổi theo cache.** Chạy 2 lần cùng file CSV ra cùng số 10,49GB — cache Windows chỉ làm nhanh hơn về *thời gian*, không đổi số byte đã "xin" đọc. Số giống nhau là dấu hiệu đo đúng, không phải bug.

**Spill trên Windows trốn kỹ.** File tạm hiện 0MB trên Explorer suốt lúc query chạy, vì NTFS chỉ cập nhật size khi đóng handle mà DuckDB giữ mở tới cuối query. Muốn thấy size thật phải hỏi `duckdb_temporary_files()` trực tiếp từ engine.

**Clean OOM khác kiểu chết bẩn.** DuckDB hết bộ nhớ thì tự dừng gọn (exception, máy vẫn sống) — 1GB limit ở Task 4 chết kiểu này. Pandas thì cứ ăn RAM tới khi hệ thống nghẹt, cần guard rail riêng chặn trước (RSS của mình + RAM còn trống của máy).

**Số giây tuyệt đối ở workload ngắn không đáng tin, biên ok/OOM mới đáng tin.** Sweep right-size chạy 2 lần ra 2 bộ số giây khác hẳn nhau (do nhiễu, không phải do cache order như đoán ban đầu) — nhưng ranh giới 512MB ok / 256MB chết giữ nguyên cả 2 lần. Đó là tín hiệu chắc, số giây chỉ là nhiễu.

**Tune xong thì quy ra tiền mới thuyết phục được ai.** A12 nhanh hơn tiết kiệm $14/năm; máy vừa cỡ 512MB nên xuống tier rẻ hơn, thêm $13/năm. Giá $0,4/h chỉ là ước lượng — quan trọng là thói quen tính, không phải số tuyệt đối.

# A14 — Performance tuning: đo → giải thích → sửa → đo lại

Nhận lại một job "chạy đúng nhưng chậm". Đo nó, xếp hạng lỗi theo giây đo được (không theo cảm giác), sửa từng cái một, chứng minh mỗi lần sửa không làm hỏng gì.

---

## 1. Đã làm gì

| Task | Việc | Kết quả |
|---|---|---|
| 1 | Dựng cái đồng hồ (warmup + median 3 + fetchall) | COLD 11,5ms > bench 6,1/5,4ms, chênh 11,9% — đồng hồ dùng được |
| 2 | 3 cách lọc cùng 1 ngày | small: 116/23/6,3ms — full: 2528/86,4/11,4ms, A chậm hơn C 221× |
| 3 | Làm quen badpipe, chưa sửa | small TOTAL 60,07s (validate 99,1%) — full TOTAL 2314,45s (validate 99,5%) |
| 4 | Sửa 6 fix, từng cái một, kiểm EXCEPT sau mỗi lần | small 60,07s → 0,10s (600×) |
| 5 | Đối đầu full scale | full 2314,45s → 0,42s (5.510×), EXCEPT 2 chiều = 0 ở cả 2 scale |
| 6 | Row group & zone map | nhóm nhỏ đánh thuế mọi query ~4×; sắp xếp + nhóm lành mạnh làm lọc 1 cửa hàng nhanh 36× |
| 7 | Skew theo ngày | small: 1,0x thời gian cho 3,1x dòng — full: hướng đúng (spike luôn chậm hơn) nhưng độ lớn nhiễu 4x-9x, không hội tụ |
| 8 | Viết `perf_checklist.md` + dọn dẹp | 8 con số tự đo, 4 mục theo đề, dọn ~8GB thí nghiệm |

Code mới: `work/a14_performance/{a14_bench,a14_bootstrap,a14_badpipe,a14_fastpipe,a14_explore}.py`, `work/a14_performance/perf_checklist.md`.

---

## 2. Cái đồng hồ phải tự nghi ngờ trước khi tin

Query nhỏ (~10ms ở small) thì % chênh giữa 2 lần bench dao động mạnh chỉ vì nhiễu hệ thống — chạy 3 lần liên tiếp mới ra được lần COLD rõ ràng chậm hơn (11,5ms vs 6,1/5,4ms). Bài học: không phải mọi lần benchmark đều "đúng ngay từ đầu" — phải tự kiểm cái đồng hồ trước khi tin số nó báo, và ở query quá nhỏ, kiên nhẫn chạy lại quan trọng hơn cố diễn giải một con số nhiễu.

---

## 3. Ba cách lọc, một câu trả lời, chênh nhau 200 lần

`order_date = DATE '...'` (lọc trên cột partition) luôn thắng vì cả file không liên quan không bao giờ được mở — `EXPLAIN ANALYZE` in thẳng ra `Scanning Files: 1/237`. `CAST(order_ts AS DATE)` trông vụng nhưng optimizer viết lại thành phép so khoảng, zone map cứu được — chỉ chậm hơn C vài lần, không phải hàng trăm lần. `strftime(order_ts, '%Y-%m-%d')` giấu cột trong hàm chuỗi mà không optimizer nào nhìn xuyên qua — mọi file bị mở, mọi dòng bị định dạng lại, chậm hơn C tới 221× ở full.

N=237 (tổng số file trong lake) giữ nguyên giữa 2 scale — vì số file phụ thuộc số lần APPEND của A10, không phụ thuộc scale.

---

## 4. Xếp hạng lỗi bằng số đo, không bằng cảm giác

Badpipe có 7 lỗi, nhưng chỉ 1 chiếm 99%+ thời gian: vòng lặp Python chạy 60k lệnh `INSERT` đơn lẻ trong bước `validate`. Mấy lỗi "bắt mắt" — `SELECT DISTINCT *`, `strftime`, `ORDER BY` — cộng lại chưa tới 1% tổng thời gian. Nếu bắt đầu sửa từ chỗ thú vị trước, sẽ tốn cả buổi để tiết kiệm nửa giây.

7 lỗi: (1) vòng lặp Python từng dòng INSERT — validate, 59,50s; (2) lọc `strftime` thay vì cột partition — slice; (3) `DISTINCT` thừa; (4) `ORDER BY` không ai yêu cầu; (5) `SELECT *` kéo `items`/`meta` qua mọi bước; (6) `LEFT JOIN customers` không dùng gì; (7) `ROW_GROUP_SIZE 5000` quá nhỏ.

---

## 5. Sửa từng cái một, đo sau mỗi lần, kiểm EXCEPT sau mỗi lần

| Fix | TOTAL (small) | Việc |
|---|---|---|
| gốc (badpipe) | 60,07s | — |
| 1 | 0,48s | loop → 1 câu SQL |
| 2 | 0,35s | lọc `order_date` thay `strftime` |
| 3 | 0,27s | xoá DISTINCT + ORDER BY (đã chứng minh vô dụng: 59.706 = 59.706 qua DISTINCT) |
| 4 | 0,23s | gọi tên 6 cột hợp đồng — file giảm 3,89MB → 1,38MB |
| 5 | 0,17s | bỏ LEFT JOIN customers |
| 6 | 0,10s | `ROW_GROUP_SIZE` 5000 → 122880 |

EXCEPT 2 chiều (`old-new`, `new-old`) trả về 0 sau **mỗi** lần sửa, cả `orders` (so đúng 6 cột hợp đồng) và `store_daily` (so `*`). Fix 1 mua về nhiều giây nhất — đúng như xếp hạng ở Task 3 dự đoán.

---

## 6. Full scale: khoảng cách thật, không phải khoảng cách đoán

| Scale | badpipe | fastpipe | speedup |
|---|---|---|---|
| small | 60,07s | 0,10s | 600× |
| full | 2314,45s | 0,42s | **5.510×** |

Vượt xa mục tiêu tối thiểu 20×. EXCEPT chạy lại ở full — vẫn 0 cả 2 chiều, kể cả khi 3 era + dữ liệu trễ mà small có thể che giấu. DISTINCT vẫn vô dụng ở full (1.193.931 = 1.193.931).

---

## 7. Row group và zone map: hai núm vặn trả giá lúc ghi, thu lãi lúc đọc

Ghi: file nhóm nhỏ (rg5000) và có sắp xếp là chậm nhất — 298,5s ở full, so với 24,1s của nhóm lành mạnh không sắp xếp.

Đọc, full scale:

| Biến thể | Nhóm | Quét-hết | Lọc 1 cửa hàng |
|---|---|---|---|
| rg5000 (nhỏ), không sắp | 13.424 | 3634,9ms | 3000,6ms |
| rg122880 (lành mạnh), không sắp | 668 | 939,0ms | 827,8ms |
| rg122880, sắp theo store_id | 670 | 784,6ms | **25,7ms** |
| rg5000, sắp | 13.399 | 3020,3ms | 376,4ms |

Nhóm nhỏ đánh thuế **mọi** query (~4×) vì mỗi nhóm có chi phí cố định riêng — kể cả câu quét-hết không lọc gì. Sắp xếp không giúp gì cho quét-hết (vẫn phải đọc hết byte), nhưng với nhóm lành mạnh thì lọc 1 cửa hàng nhanh **36 lần** — zone map bỏ qua gần hết 668 nhóm. Nhìn tận mắt: nhóm 0 chỉ chứa store 1-16, nhóm 1 chỉ 16-22... khoảng hẹp = bỏ qua được; file không sắp thì mọi nhóm đều chứa store 42.

Áp cùng kỹ thuật lên partition ngày spike (06-19, full): 30 nhóm, lọc 1 cửa hàng 11,4ms (plain) → 3,4ms (sorted). Đây là lý do một job viết lại partition (kiểu atomic swap A09) sẽ thêm `ORDER BY store_id` nếu lọc theo cửa hàng là đường nóng — chỉ sắp xếp vật lý được một kiểu, phải dành đúng cho bộ lọc dùng nhiều nhất.

Khi nào đặt row group nhỏ lại là đúng: khi cần ghi liên tục theo lô nhỏ (streaming/near-real-time) và freshness quan trọng hơn tốc độ đọc — đổi lại mọi query đọc sau đó trả thêm phí ~4× suốt vòng đời file.

---

## 8. Skew theo ngày: scale nhỏ xếp hạng được, không đo cỡ được
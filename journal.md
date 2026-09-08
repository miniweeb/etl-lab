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
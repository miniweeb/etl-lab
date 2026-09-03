"""A05 Task 12 — cùng ba luật, viết dưới dạng KHAI BÁO thay vì chuỗi CASE.

Bạn đã tự dựng một bộ validation bằng tay, và đó là cách trung thực duy nhất để
gặp một validation FRAMEWORK: giờ bạn biết chính xác nó đang làm hộ bạn cái gì.

Luật §7.5: công cụ KHÔNG thay thế bản tự dựng, nó đứng CẠNH. Pipeline vẫn gate
bằng SQL.

Cài (chỉ task này; requirements.txt để dòng pandera comment sẵn vì lý do đó):
    pip install pandera
Chạy từ gốc repo:
    python -m work.a05_validation.a05_pandera_demo
"""
import duckdb
import pandera.pandas as pa      # namespace hiện tại của pandas API
                                 # (bản cũ: import pandera as pa)

from lib.labpaths import warehouse_path
from work.a05_validation.a05_validate_day import stage_day   # import, không paste

SCALE, DAY = "small", "2026-06-02"
STATUSES = ['created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded']

con = duckdb.connect(str(warehouse_path(SCALE)))
stage_day(con, DAY, SCALE)                    # staging đang giữ ngày chạy gần nhất
df = con.execute("""
    SELECT order_id, status, order_total FROM staging.shopcore_orders_day
""").df()                                     # CHỈ 3 cột — xem ghi chú kích thước
print(f"{DAY}: {len(df):,} rows -> pandas; order_total dtype = {df.order_total.dtype}")
# Để ý dòng print: DECIMAL(14,2) của DuckDB hạ cánh xuống pandas thành float64.
# Tiền biến thành số thực nhị phân trên đường ra cửa — lý do thứ hai để phán xét
# tiền ở NƠI NÓ ĐƯỢC LƯU.

schema = pa.DataFrameSchema(
    {
        "order_id":    pa.Column("int64", nullable=False),
        "status":      pa.Column(nullable=False, checks=pa.Check.isin(STATUSES)),
        "order_total": pa.Column("float64", nullable=False, checks=pa.Check.ge(0)),
    },
    name="staging.shopcore_orders_day",
    strict=False,          # dung thứ cột chưa khai báo — chính là luật
)                          # change_management của contract

try:
    schema.validate(df, lazy=True)     # lazy: gom MỌI lỗi, không dừng ở lỗi đầu —
    print("pandera: all checks passed")  # cùng lý do concat_ws gom mọi reject_reason
except pa.errors.SchemaErrors as err:
    fc = err.failure_cases.assign(     # bỏ phần biểu diễn tham số của từng check
        check=lambda d: d["check"].str.split("(").str[0])
    print(fc.groupby(["column", "check"]).size().to_string())
    print(fc.dropna(subset=["failure_case"])[
        ["column", "check", "failure_case", "index"]].head(5).to_string(index=False))

# ĐỐI CHIẾU với ops.dq_report — cùng dòng, cùng luật, số khớp nhau:
#   status:      isin 35 + not_nullable 36 = 71   = bad_status
#   order_total: ge   52 + not_nullable 117 = 169 = null_total + negative_total
#
# BA khác biệt mới là bài học:
#   1. pandera TÁCH thứ SQL của bạn GỘP. CASE bad_status gộp "NULL" và "ngoài
#      whitelist" làm một; pandera báo hai check, vì nullability là thuộc tính của
#      CỘT chứ không phải luật GIÁ TRỊ. Chọn hình nào là quyết định thiết kế thật:
#      tách thì triage nhanh hơn, gộp thì so ngưỡng với null_or_invalid_status: 1%
#      của contract đơn giản hơn.
#   2. order_id KHÔNG nói gì cả. Nó pass nên không phát ra dòng nào.
#      ops.dq_report cố ý ghi cả số 0 — thư viện schema báo LỖI, còn CHỨNG MINH
#      một check đã chạy vẫn là việc của bạn.
#   3. err.failure_cases là DataFrame CÓ KIỂU (column/check/failure_case/index),
#      không phải chuỗi: insert được vào bảng, join lại theo index, diff hai run
#      được. Đó chính là thứ unnest(string_split(reject_reason,'; ')) đang dựng
#      thủ công — và là lý lẽ mạnh nhất của pandera.

# KHI NÀO ĐỪNG DÙNG: pandera validate một DataFrame -> dòng phải nằm TRONG RAM.
# Đo và ngoại suy:
print(f"3 cột, 1 ngày small: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
# deep=True xứng đáng: status là cột object, phép đếm nông tính 8 byte cho con trỏ
# và 0 byte cho chuỗi nó trỏ tới — khoảng 1/3 con số thật.
#   ~4,3 MB  cho 3 cột, 1 ngày small
#   -> ~5,9 GB cho 3 cột trên cả 82,3M dòng
#   -> ~36 GB cho cả 12 cột staging, vượt xa memory_limit 8GB
# ...để kiểm những luật DuckDB đánh giá mà không vật chất hoá lấy một dòng.
# Đó là toàn bộ lý do pipeline giữ bộ SQL: CHECK CHẠY Ở NƠI 82M DÒNG ĐANG NẰM.
# Dùng pandera nơi dữ liệu ĐÃ là DataFrame và ĐÃ nhỏ — feature frame, payload API,
# fixture test — hoặc như bản PHÁT BIỂU dễ đọc của luật để reviewer duyệt, đứng
# cạnh SQL cưỡng chế nó ở quy mô lớn. Đừng thay nó vào chỗ gate.
#
# MỘT BẬC NỮA: Great Expectations / Soda. Pandera đưa bạn schema object; hai cái
# này đưa bộ máy quanh nó — expectation suite (THRESHOLDS + danh sách check của
# bạn, đã lớn lên và dùng chung giữa nhiều pipeline), results store (ops.dq_report
# nhưng do công cụ quản lý và lưu trữ), và data docs — HTML sinh tự động cho thấy
# expectation nào pass/fail theo cột, theo run, theo thời gian. Cái cuối là thứ
# bạn KHÔNG có bản tương đương: chỉ được một analyst tài chính hay producer xem,
# điều mà SELECT * FROM ops.dq_report không làm nổi. Lab chỉ gọi tên rồi dừng: mỗi
# cái mang theo tầng config và wiring datasource riêng, cài vào đây sẽ đánh tráo
# bài học hôm nay — check LÀ GÌ và gate PHẢI NGỒI Ở ĐÂU — bằng bài học về YAML của
# người khác.
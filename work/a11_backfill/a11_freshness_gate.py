"""Freshness gate: shopcore đã giao đủ mọi file contract hứa chưa?

contracts/shopcore_orders_daily.contract.yaml, delivery.freshness_sla:
file của business date D hoàn tất trước 02:00 UTC ngày D+1; warehouse phải bắt
đầu nạp được từ 04:00 UTC; file còn thiếu lúc 04:00 UTC là SỰ CỐ PHÍA PRODUCER.

Vì sao cần file này, trong khi runner đã báo lỗi khi thiếu file:

    Runner chỉ nhận ra file thiếu NẾU có ai đó chạy nó.

Một chuyến giao không bao giờ đến thì không gây ra lỗi ở đâu cả, vì công việc
lẽ ra phải thất bại đã không bao giờ khởi động. Một SLA cần người canh chạy
THEO ĐỒNG HỒ, không phải theo yêu cầu.

Phân công với A05: a05_freshness_check.py dò max(_data_date) của core.orders —
nó nhìn warehouse từ BÊN TRONG, hỏi "kho có cũ không". File này nhìn thư mục
giao của shopcore từ BÊN NGOÀI, hỏi "nguồn có giao không". Hai cảm biến khác
nhau, cùng đổ về một chỗ: ops.alerts.

Đó là phân biệt đáng giữ. Kho cũ có thể vì nguồn không giao, mà cũng có thể vì
pipeline chết — hai nguyên nhân, hai người phải gọi.

Chạy:
    python -m work.a11_backfill.a11_freshness_gate --scale small --as-of 2026-08-09T04:30
    python -m work.a11_backfill.a11_freshness_gate --scale small --as-of 2026-08-10T04:30
    echo $LASTEXITCODE

--as-of là đồng hồ giả lập. Không sửa file nào, không xoá file nào: chuyến giao
cuối của feed là 2026-08-08, nên giả vờ đồng hồ nhích thêm một ngày CHÍNH LÀ
ngày thiếu cần demo.
"""
from __future__ import annotations

import argparse
import datetime as dt

import duckdb

from lib.labpaths import raw_orders_dir, warehouse_path

FEED_START = dt.date(2026, 6, 1)     # contract effective_from
SLA_HOUR_UTC = 4                     # mốc 04:00 UTC trong freshness_sla


def due_through(as_of: dt.datetime) -> dt.date:
    """Business date muộn nhất mà file PHẢI đã có mặt tại thời điểm `as_of`.

    File của ngày D đến hạn vào D+1. Nên trước 04:00 thì ngày D-1 chưa quá hạn
    (nó còn vài giờ nữa), sau 04:00 thì nó đã quá hạn.

    Đây là lý do hàm này tồn tại thay vì viết thẳng `today - 1`: cùng một ngày
    lịch cho ra hai câu trả lời khác nhau tuỳ giờ, và một watchdog chạy lúc
    03:00 mà báo động thì sẽ bị tắt chuông trong một tuần.
    """
    lag = 1 if as_of.hour >= SLA_HOUR_UTC else 2
    return as_of.date() - dt.timedelta(days=lag)


def main() -> None:
    p = argparse.ArgumentParser(description="Contract freshness gate (§7.3).")
    p.add_argument("--scale", choices=["small", "full"], required=True)
    p.add_argument("--as-of", required=True, type=dt.datetime.fromisoformat,
                   help="đồng hồ UTC giả lập, ví dụ 2026-08-10T04:30")
    args = p.parse_args()

    last_due = due_through(args.as_of)
    expected = [FEED_START + dt.timedelta(days=i)
                for i in range((last_due - FEED_START).days + 1)]
    missing = [d for d in expected
               if not (raw_orders_dir(args.scale) / f"orders_{d}.csv").exists()]
    #   Kiểm sự TỒN TẠI của file, không kiểm ledger. Một ngày chưa ai chạy và
    #   một ngày file chưa về là hai chuyện khác nhau, và gate này chỉ hỏi
    #   chuyện thứ hai — đúng phần mà contract giao cho producer.

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.alerts (   -- A05 dựng bảng này
                       alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
                       source VARCHAR, message VARCHAR)""")

    for d in missing:
        con.execute(
            "INSERT INTO ops.alerts VALUES (now(), NULL, 'error', 'freshness', ?)",
            [f"orders_{d}.csv not delivered by 04:00 UTC "
             f"(checked {args.as_of:%Y-%m-%d %H:%M}) - freshness SLA breach; "
             f"producer incident, escalate to #shopcore-oncall"])
        #   run_id NULL, và đó CHÍNH LÀ điểm mấu chốt: không có lần chạy nào để
        #   gắn vào. Mọi alert khác trong bảng này đều sinh ra từ một run đang
        #   chạy; alert này sinh ra từ một run KHÔNG BAO GIỜ xảy ra.
        #
        #   Thông điệp hướng tới NGƯỜI TRỰC, không hướng tới lập trình viên:
        #   nó nói thiếu file nào, kiểm lúc mấy giờ, vi phạm điều khoản nào, và
        #   gọi ai. Không có stack trace nào giúp được lúc 4 giờ sáng.
        print(f"ALERT freshness  orders_{d}.csv missing - SLA breached")

    con.close()

    if missing:
        raise SystemExit(1)
        #   Mã thoát khác 0 mới là thứ scheduler tuân theo. In màn hình đỏ mà
        #   vẫn exit 0 là chuông báo cháy không lắp pin — bài học A06, và là lý
        #   do ba tầng (console + ops.alerts + exit code) phải cùng nói một điều.
    print(f"fresh - mọi file tới {last_due} đã được giao")


if __name__ == "__main__":
    main()
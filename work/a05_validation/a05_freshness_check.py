"""A05 Task 9 — core.orders có ĐANG cập nhật không?

Mọi check khác phán xét nội dung một ngày. Không cái nào nhận ra pipeline đơn
giản là đã ngừng chạy: core có thể sạch tuyệt đối mà vẫn trễ ba ngày, âm thầm
nói dối mọi dashboard đọc nó.

"Ngày mới nhất đáng lẽ phải có" đến từ delivery.schedule của contract (file hoàn
tất trước 02:00 UTC ngày D+1) -> trong production nó chính là HÔM QUA, và A11 nối
đồng hồ đó vào SLA thật. Hôm nay đặt tay.

Chạy từ GỐC REPO (thói quen -m của A03; gọi theo đường dẫn thì lib không import được):
    python -m work.a05_validation.a05_freshness_check
"""
import sys

import duckdb

from lib.labpaths import warehouse_path

SCALE, FRESH_EXPECT = "small", "2026-07-11"   # ngày mới nhất core PHẢI chứa

con = duckdb.connect(str(warehouse_path(SCALE)))
latest, stale = con.execute(f"""
    SELECT max(_data_date), max(_data_date) < DATE '{FRESH_EXPECT}'
    FROM core.orders""").fetchone()
# _data_date là cột lineage của A03 — nhờ nó, freshness chỉ là một dòng SQL.

if stale:
    msg = (f"FRESHNESS BREACH: core.orders max(_data_date) = {latest}, "
           f"expected {FRESH_EXPECT}")
    con.execute("INSERT INTO ops.alerts VALUES "
                "(current_timestamp, NULL, 'error', 'freshness', ?)", [msg])
    print("ALERT:", msg)
    sys.exit(1)                               # exit khác 0: scheduler réo pager
print(f"fresh: core.orders is at {latest}")
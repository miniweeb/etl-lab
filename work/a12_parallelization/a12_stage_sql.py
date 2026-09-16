"""A12 — sinh SQL stage cho MỘT ngày, era-aware, bằng cách TÁI DÙNG A10.

Đề A12 đưa sẵn một cleaner v1 rút gọn trong `stage_sql` rồi bảo "TODO(you):
thay bằng canonical staging của A10". Đây là chỗ trả nợ. Ta không copy SQL của
A10 sang — ta import nó. A10 đã tách sẵn hai tầng đúng cho việc này:

    ALIGN[era]  ->  15 cột giống hệt nhau cho mọi era   (việc của SCHEMA)
    CANON       ->  cleaner dùng chung, mù era           (việc của VALUE)

Nếu hôm nay bạn paste lại SQL thay vì import, thì mai sửa một cleaner bạn phải
nhớ có hai bản. Đó đúng là loại lỗi A10 sinh ra để diệt.

Worker import file này. Nên file này KHÔNG được import bất cứ thứ gì mở
warehouse.duckdb — kiểm lại mỗi khi bạn thêm import.
"""
from __future__ import annotations

from pathlib import Path

from work.a10_schema_evolution.a10_canonical import (
    ALIGN, CANON, V1_COLS, V2_COLS, V3_COLS, era_of)

COLS = {"v1": V1_COLS, "v2": V2_COLS, "v3": V3_COLS}


def era_from_manifest(day: str, man: dict | None = None) -> str:
    """Era lấy từ manifest nếu có trường `era`, ngược lại suy từ ngày.

    A10 Task 8 đã dạy: ngày tháng có thể nói dối (file đặt sai tên), HEADER mới
    là sự thật. Ở đây ta không sniff lại 69 file trong mỗi worker — gate A10 đã
    chạy và đã xanh. Manifest là bản ghi của producer, tin nó; era_of() chỉ là
    lưới đỡ khi manifest thiếu trường.
    """
    if man and "era" in man:
        return man["era"]
    return era_of(day)


def stage_sql(day: str, csv_path: Path, out_path: Path, era: str) -> str:
    """COPY (canonical 1 ngày + _data_date) TO một file parquet DUY NHẤT.

    MỘT FILE MỖI NGÀY, không PARTITION_BY. Lý do là late row: ngày D mang
    correction cho D-7..D, nên hai worker sẽ cùng ghi vào thư mục
    order_date=... và đè lặng lẽ file của nhau (đúng cái bẫy A02). Không chia
    sẻ thư mục = không cần khoá. Việc partition để committer làm, một mình.

    _data_date: §7.2 — NGÀY GIAO của lô, không phải order_date của dòng. Một
    dòng order_date=07-14 nằm trong file ngày 07-18 thì _data_date là 07-18.
    Đó là điều khiến nó có ích: nó trả lời "dòng này tới từ lần giao nào".
    _run_id vắng mặt ở đây có chủ ý — scaffold tốc độ này không đăng ký run
    nào; pipeline A11 mới là đường production đóng dấu đủ hai cột.
    """
    cols = COLS[era]
    read = (f"read_csv('{csv_path.as_posix()}', header=true, columns={cols}"
            + (", store_rejects=true" if day == "2026-07-22" else "")
            + ")")
    # store_rejects cho BAD_DAY: A11 đã chẩn ra file 07-22 có dòng gãy. Ở A12
    # ta KHÔNG để nó FAIL nữa — backfill nhanh mà bỏ một ngày thì vẫn là thiếu.
    # Dòng gãy đi vào reject table, phần còn lại vẫn stage được.

    aligned = ALIGN[era].replace("FROM src", f"FROM {read}")
    return f"""
    COPY (
        WITH aligned AS ({aligned}),
             canon AS ({CANON})
        SELECT *, DATE '{day}' AS _data_date FROM canon
    ) TO '{out_path.as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 122880)
    """


# TODO(you) — Task 3, sau khi chạy được:
# CANON có `WHERE ... IS NOT NULL` vứt các dòng timestamp không parse nổi. Ở
# A10 bạn đếm chúng MỖI LẦN CHẠY (DROPPED_SQL) và đóng phép cộng. Ở đây, mỗi
# worker là một process riêng — cộng dồn bằng biến Python KHÔNG qua được ranh
# giới process.
# Câu hỏi để trả lời trong journal: làm sao đưa `dropped` của từng ngày về
# process cha? (Gợi ý: load_day() đã return một tuple rồi. Thêm một phần tử.)
# Con số đích: small stage 4.116.024, manifest cộng 4.116.840 -> 816 dòng bị bỏ.
# 816 chứ không phải 803 của A10: lần này bạn stage cả 07-22, và 13 dòng của nó
# nhập vào tổng.


if __name__ == "__main__":
    from work.a12_parallelization.a12_bootstrap import RAW, STAGE
    for d in ("2026-06-02", "2026-07-16", "2026-08-01"):
        e = era_from_manifest(d)
        print(f"\n--- {d}  era={e} ---")
        print(stage_sql(d, RAW / f"orders_{d}.csv", STAGE / f"orders_{d}.parquet", e)[:400])
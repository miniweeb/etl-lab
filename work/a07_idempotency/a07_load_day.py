"""Loader idempotent: stage → validate → publish nguyên tử.

Thay cho lệnh INSERT trần của A05. Chạy một lần hay mười lần với cùng một ngày
thì core.orders phải giống hệt nhau.

Ba chi tiết trong bộ khung, đừng lướt qua:

1. Lệnh UPDATE ghi 'success' vào ops.etl_runs nằm BÊN TRONG BEGIN…COMMIT. Nên
   ledger không bao giờ khai thành công cho dữ liệu không tồn tại.
2. DELETE WHERE _data_date = ? chạy ngay cả ở lần load đầu tiên — xoá 0 dòng,
   không tốn gì. Lần chạy đầu và lần chạy lại đi CÙNG MỘT NHÁNH CODE. Ít nhánh
   thì ít bug.
3. Mỗi LẦN THỬ tự cấp một run_id riêng rồi tự thuật vào logs/run_<run_id>.log.
   Console cho bạn xem, file để làm dấu vết audit.

Chạy:
    python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03
    python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force
    python -m work.a07_idempotency.a07_load_day --scale small --date 2026-06-03 --force --chaos 0.6
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))          # để work.* và lib.* import được

from lib.labpaths import data_root, warehouse_path   # noqa: E402
from work import preflight                            # noqa: E402

LOG_DIR = REPO_ROOT / "logs"
log = logging.getLogger("a07")


# ══════════════════════════════════════════════════════════════════════
# Chaos monkey — Task 6
# ══════════════════════════════════════════════════════════════════════
class TransientError(RuntimeError):
    """Lỗi ĐÁNG retry: mạng chập, file bị khoá, chaos monkey.

    Cố tình KHÔNG bao gồm lỗi dữ liệu. ValueError từ khâu validate nghĩa là
    DỮ LIỆU sai — retry không sửa được và phải fail ngay.
    """


def maybe_fail(point: str, chaos: float) -> None:
    if chaos and random.random() < chaos:
        raise TransientError(f"chaos monkey struck at: {point}")


# ══════════════════════════════════════════════════════════════════════
# Ledger — Task 2
# ══════════════════════════════════════════════════════════════════════
def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Dựng ops.etl_runs và core.orders nếu chưa có.

    TODO(you): MỘT dòng dưới đây cố ý đặt SAI CHỖ. CREATE SCHEMA phải nằm
    TRƯỚC bảng cần tới nó — nếu không, lần chạy đầu tiên trên một warehouse
    sạch sẽ nổ ở dòng CREATE TABLE. Tìm và chuyển nó lên.
    """
    # con.execute("""
    #     CREATE TABLE IF NOT EXISTS ops.etl_runs (
    #         run_id      BIGINT PRIMARY KEY,   -- nextval('ops.run_id_seq')
    #         run_date    DATE,                 -- ngày dữ liệu đang được load
    #         step        VARCHAR,              -- 'load_day' hôm nay; sau này thêm step khác
    #         status      VARCHAR,              -- 'running' | 'success' | 'failed'
    #         started_at  TIMESTAMP,
    #         finished_at TIMESTAMP,            -- lúc lần load đáp xuống
    #         rows        BIGINT,               -- số dòng đã publish (chỉ khi success)
    #         source_file VARCHAR,              -- lần thử này đọc file nào
    #         error       VARCHAR               -- thông điệp lỗi (chỉ khi failed)
    #     )""")
    # con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    # con.execute("CREATE SCHEMA IF NOT EXISTS core")
    # con.execute("CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq")
    
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq")

    con.execute("""
        CREATE TABLE IF NOT EXISTS ops.etl_runs (
            run_id      BIGINT PRIMARY KEY,   -- nextval('ops.run_id_seq')
            run_date    DATE,                 -- ngày dữ liệu đang được load
            step        VARCHAR,              -- 'load_day' hôm nay; sau này thêm step khác
            status      VARCHAR,              -- 'running' | 'success' | 'failed'
            started_at  TIMESTAMP,
            finished_at TIMESTAMP,            -- lúc lần load đáp xuống
            rows        BIGINT,               -- số dòng đã publish (chỉ khi success)
            source_file VARCHAR,              -- lần thử này đọc file nào
            error       VARCHAR               -- thông điệp lỗi (chỉ khi failed)
        )""")
    #   Mỗi LẦN THỬ một dòng riêng — thất bại là lịch sử đáng giữ, không phải
    #   điều đáng ghi đè.
    #
    #   source_file và finished_at sống Ở ĐÂY chứ không phải trên từng dòng dữ
    #   liệu. Đó chính là hai cột mà A03 đã từ chối (_source_file, _loaded_at).
    #   Chúng là thuộc tính của LẦN CHẠY: lưu một lần, và 62.707 dòng trỏ tới
    #   bằng một _run_id 8 byte. Nạp lại ngày đó từ file đã sửa thì chỉ MỘT dòng
    #   ledger đổi, thay vì ghi lại cùng một chuỗi 62.707 lần.


def already_succeeded(con: duckdb.DuckDBPyConnection, day: date) -> bool:
    """Ngày này đã từng load thành công chưa?

    TODO(you): truy vấn ops.etl_runs, trả về True nếu có ÍT NHẤT MỘT dòng với
    run_date = day VÀ step = 'load_day' VÀ status = 'success'.

    Để ý: chỉ tính 'success'. Một ngày có mười dòng 'failed' thì vẫn là chưa
    load — và phải load lại được mà không cần --force.
    """
    # raise NotImplementedError("TODO(you): already_succeeded")

    query = """
        SELECT count(*) > 0
        FROM ops.etl_runs
        WHERE run_date = ?
          AND step = 'load_day'
          AND status = 'success'
    """
    return bool(con.execute(query, [day]).fetchone()[0])

def open_run(con: duckdb.DuckDBPyConnection, day: date, src: Path) -> int:
    """Mở một dòng ledger ở trạng thái 'running', trả về run_id."""
    run_id = con.execute("SELECT nextval('ops.run_id_seq')").fetchone()[0]
    con.execute("""
        INSERT INTO ops.etl_runs
              (run_id, run_date, step, status, started_at, source_file)
        VALUES (?, ?, 'load_day', 'running', now(), ?)""",
        [run_id, day, src.name])
    #   Ghi source_file NGAY, TRƯỚC KHI có gì đó hỏng. Một lần thử thất bại vẫn
    #   phải nói được nó nghẹn ở file nào.
    return run_id


def fail_run(con: duckdb.DuckDBPyConnection, run_id: int, exc: Exception) -> None:
    con.execute("""
        UPDATE ops.etl_runs SET status = 'failed', finished_at = now(), error = ?
        WHERE run_id = ?""", [f"{type(exc).__name__}: {exc}", run_id])


def write_alert(con: duckdb.DuckDBPyConnection, day: date,
                run_id: int | None, exc: Exception) -> None:
    """Một lần load bỏ cuộc phải được NGHE THẤY — Task 7.

    Ghi vào ops.alerts, chính bảng bộ check A05 đã tạo. Chính sách retry và
    chính sách cảnh báo là HAI quyết định khác nhau: retry hết lượt rồi vẫn
    phải báo động, và một ValueError fail-ngay cũng phải báo động.
    """
    con.execute("""
        INSERT INTO ops.alerts (alert_ts, severity, run_id, message)
        VALUES (now(), 'error', ?, ?)""",
        [run_id, f"load_day {day} gave up: {type(exc).__name__}: {exc}"])


# ══════════════════════════════════════════════════════════════════════
# Retry — Task 7
# ══════════════════════════════════════════════════════════════════════
def with_retry(fn, max_attempts: int = 8, base_delay: float = 1.0,
               max_delay: float = 30.0):
    """Gọi fn(); gặp TransientError thì chờ rồi thử lại. Thời gian chờ nhân đôi."""
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except TransientError as exc:
            if attempt == max_attempts:
                log.error("attempt %d/%d failed - giving up", attempt, max_attempts)
                raise
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            log.warning("attempt %d/%d failed (%s) - retrying in %.1fs",
                        attempt, max_attempts, exc, delay)
            time.sleep(delay)
    #   CHỈ bắt TransientError. Một ValueError từ validate nghĩa là DỮ LIỆU sai
    #   — retry không sửa được và phải fail ngay. Đây là quy tắc bị bỏ qua nhiều
    #   nhất trong thiết kế retry.


# ══════════════════════════════════════════════════════════════════════
# Loader
# ══════════════════════════════════════════════════════════════════════
def load_day(con: duckdb.DuckDBPyConnection, scale: str, day: date,
             chaos: float = 0.0) -> int:
    root = data_root(scale)
    src = root / "raw" / "orders" / f"orders_{day}.csv"
    run_id = open_run(con, day, src)
    log.info("run_id=%s day=%s src=%s", run_id, day, src.name)

    try:
        # ---- bước 1: đọc manifest ---------------------------------------
        maybe_fail("after_read_manifest", chaos)

        # ---- bước 2: stage — đọc thô rồi làm sạch ------------------------
        # Các biểu thức timestamp và tiền dưới đây là fragment của
        # work/cleaners.py từ A03. Trong script của bạn hãy IMPORT chúng thay
        # vì dán lại: tests/test_cleaners.py đã ghim hành vi, và một fragment
        # sửa ở một chỗ là sửa ở mọi nơi import nó.
        con.execute("DROP TABLE IF EXISTS stg_day")
        con.execute(f"""
            CREATE TEMP TABLE stg_day AS
            SELECT
                TRY_CAST(order_id AS BIGINT)                       AS order_id,
                TRY_CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT) AS customer_id,
                TRY_CAST(store_id AS INTEGER)                      AS store_id,
                COALESCE(
                    try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']),
                    TRY_CAST(order_ts AS TIMESTAMP))               AS order_ts,
                TRY_CAST(updated_at AS TIMESTAMP)                  AS updated_at,
                lower(trim(status))                                AS status,
                lower(trim(payment_method))                        AS payment_method,
                TRY_CAST(replace(replace(replace(order_total, '$', ''), '.', ''), ',', '.')
                         AS DECIMAL(14,2))                         AS order_total,
                items, meta
            FROM read_csv('{src.as_posix()}', header=true, all_varchar=true)""")
        staged = con.execute("SELECT count(*) FROM stg_day").fetchone()[0]
        log.info("staged %s rows", staged)
        maybe_fail("after_stage", chaos)

        # ---- bước 3: validate -------------------------------------------
        # Dữ liệu sai thì ValueError — KHÔNG phải TransientError, nên retry
        # sẽ không đụng vào nó.
        if staged == 0:
            raise ValueError(f"staged 0 rows from {src.name}")

        # ---- bước 4: publish — dữ liệu VÀ ledger trong MỘT transaction ---
        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM core.orders WHERE _data_date = ?", [day])
        maybe_fail("mid_transaction", chaos)
        #   Điểm đáng sợ nhất: ngày cũ đã bị xoá, ngày mới chưa được ghi.
        #   Không có transaction thì lần crash này LÀM MẤT MỘT NGÀY DỮ LIỆU.
        con.execute("""
            INSERT INTO core.orders
            SELECT *, CAST(COALESCE(order_ts, updated_at) AS DATE) AS order_date,
                   ? AS _data_date, ? AS _run_id
            FROM stg_day""", [day, run_id])
        #   COALESCE(order_ts, updated_at): ~0,02% dòng có timestamp bất khả thi
        #   và thành NULL khi cleaning (13 dòng ở small 2026-06-03). Ta vẫn cần
        #   một order_date khác NULL, và updated_at là phương án trung thực.
        #   A05 mới là nơi quarantine chúng cho đúng.
        maybe_fail("before_commit", chaos)
        con.execute("""
            UPDATE ops.etl_runs SET status = 'success', finished_at = now(),
                   rows = ? WHERE run_id = ?""", [staged, run_id])
        con.execute("COMMIT")
        log.info("published %s rows", staged)
        return staged

    except Exception as exc:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass        # không có transaction nào đang mở — bình thường
        fail_run(con, run_id, exc)
        log.error("run_id=%s failed: %s", run_id, exc)
        exc.run_id = run_id          # để main() ghi alert đúng run_id
        raise


def setup_logging(run_tag: str) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(LOG_DIR / f"run_{run_tag}.log", encoding="utf-8")],
        force=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Idempotent daily loader")
    p.add_argument("--scale", choices=["small", "full"], default="small")
    p.add_argument("--date", required=True)
    p.add_argument("--force", action="store_true", help="load lại dù đã success")
    p.add_argument("--chaos", type=float, default=0.0, help="xác suất fail mỗi điểm")
    p.add_argument("--max-attempts", type=int, default=8)
    args = p.parse_args()

    day = date.fromisoformat(args.date)
    setup_logging(f"{day}_{int(time.time())}")

    if not preflight.run_checks(args.scale, day):
        sys.exit(2)     # preflight đỏ = không đụng vào dữ liệu

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=8")
    con.execute(f"SET temp_directory='{(data_root(args.scale) / 'tmp').as_posix()}'")
    ensure_tables(con)

    if already_succeeded(con, day) and not args.force:
        log.info("%s already loaded - skipping", day)
        con.close()
        return

    try:
        with_retry(lambda: load_day(con, args.scale, day, chaos=args.chaos),
                   max_attempts=args.max_attempts)
    except Exception as exc:
        write_alert(con, day, getattr(exc, "run_id", None), exc)
        con.close()
        raise
    con.close()


if __name__ == "__main__":
    main()
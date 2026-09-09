"""A08 — ba chiến lược load trên cùng một cửa sổ 10 ngày, rồi reconcile.

Chạy dạng module từ repo root (KHÔNG chạy `python work\\...\\file.py`):

    python -m work.a08_load_strategies.a08_load_strategies --scale small
    python -m work.a08_load_strategies.a08_load_strategies --scale full --skip-bad

Bảng core.orders thật từ A05/A07 KHÔNG bị đụng tới. Script này chỉ tạo và ghi
đè ba bảng thí nghiệm: core.orders_log, core.orders_upsert, core.orders_refresh
(cộng core.orders_refresh_bad nếu bật --bad), và view core.orders_latest.
"""

import argparse
import datetime as dt
import time

import duckdb

from lib.labpaths import data_root, warehouse_path

START = dt.date(2026, 6, 1)
NDAYS = 10
WINDOW_DAYS = 7  # contract A06: late_corrections.window_days

TS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"

COLUMNS = (
    "{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
    "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
    "'payment_method':'VARCHAR','order_total':'VARCHAR',"
    "'items':'VARCHAR','meta':'VARCHAR'}"
)


def staged_select(raw_dir, d: dt.date) -> str:
    """Một file một ngày, staged tối thiểu bằng cleaner của A03, kèm _data_date.

    Đúng một cột lineage (_data_date, §7.2): contract quy định một file cho mỗi
    business date, nên _data_date CHÍNH LÀ orders_<_data_date>.csv. Tên file,
    thời điểm load, status, số dòng là sự thật về LẦN CHẠY và nằm ở ops.etl_runs.

    order_date COALESCE về ngày của file: khoá partition không bao giờ được NULL,
    nếu không chiến lược C sẽ lặng lẽ đánh mất dòng đó vĩnh viễn.
    """
    f = (raw_dir / f"orders_{d}.csv").as_posix()
    return f"""
    SELECT
      order_id,
      customer_id,
      store_id,
      try_strptime(order_ts, {TS}) AS order_ts,
      updated_at,
      status,
      payment_method,
      TRY_CAST(NULLIF(
        CASE WHEN order_total LIKE '%,%'
             THEN replace(replace(order_total, '.', ''), ',', '.')
             ELSE replace(TRIM(order_total), '$', '') END,
        'N/A') AS DECIMAL(14,2)) AS order_total,
      COALESCE(CAST(try_strptime(order_ts, {TS}) AS DATE), DATE '{d}') AS order_date,
      DATE '{d}' AS _data_date
    FROM read_csv('{f}', header=true, columns={COLUMNS})
    """


# ─────────────────────────── Task 1 ───────────────────────────


def measure(con, manifest_glob: str, dates) -> None:
    """Đo hiện tượng TRƯỚC khi dựng gì cả. small: 577673 / 7750 / 284."""
    row = con.execute(f"""
      SELECT sum(rows), sum(late_rows), sum(dup_rows)
      FROM read_json_auto('{manifest_glob}')
      WHERE CAST(date AS DATE) BETWEEN DATE '{dates[0]}' AND DATE '{dates[-1]}'
    """).fetchone()
    print(f"[manifest] rows={row[0]} late={row[1]} dups={row[2]}")

    # TODO(bạn): đếm order_id xuất hiện ở CẢ file 06-01 lẫn 06-02 bằng INTERSECT.
    # Kỳ vọng 883, trong khi late_rows của 06-02 ghi 885 — giải thích chỗ chênh 2.


# ─────────────────────────── Task 2, 3 ───────────────────────────


def build_log(con, raw_dir, dates) -> None:
    """Chiến lược A — append-only log. Bản sao trung thực của những gì đã về."""
    con.execute(
        "-- grain: một dòng = một PHIÊN BẢN đơn hàng đã được giao\n"
        f"CREATE OR REPLACE TABLE core.orders_log AS {staged_select(raw_dir, dates[0])} LIMIT 0"
    )
    t0 = time.perf_counter()
    for d in dates:
        # DELETE trước INSERT: bài học A07 — chạy lại một ngày không nạp đôi được
        con.execute(f"DELETE FROM core.orders_log WHERE _data_date = DATE '{d}'")
        con.execute(f"INSERT INTO core.orders_log {staged_select(raw_dir, d)}")
    n, nulls = con.execute("""
      SELECT count(*), count(*) FILTER (WHERE order_date IS NULL) FROM core.orders_log
    """).fetchone()
    print(f"[append ] {time.perf_counter() - t0:5.1f}s  rows={n}  null_dates={nulls}")


def build_latest_view(con) -> None:
    """View dedupe lúc đọc. Khoá sắp xếp thứ hai là luật phá thế hoà:
    cùng updated_at thì file về trước thắng — đúng lựa chọn mà MERGE sẽ làm."""
    con.execute("""
    -- grain: một dòng = một ĐƠN HÀNG, ở phiên bản mới nhất
    CREATE OR REPLACE VIEW core.orders_latest AS
    SELECT * EXCLUDE (rn) FROM (
      SELECT *,
             row_number() OVER (PARTITION BY order_id
                                ORDER BY updated_at DESC, _data_date ASC) AS rn
      FROM core.orders_log
    ) WHERE rn = 1
    """)
    n = con.execute("SELECT count(*) FROM core.orders_latest").fetchone()[0]
    multi = con.execute("""
      SELECT count(*) FROM (SELECT order_id FROM core.orders_log
                            GROUP BY 1 HAVING count(DISTINCT _data_date) > 1)
    """).fetchone()[0]
    print(f"[view   ] rows={n}  ids_in_multiple_files={multi} ({multi / n:.2%})")


# ─────────────────────────── Task 4 ───────────────────────────


def build_upsert(con, raw_dir, dates) -> None:
    """Chiến lược B — upsert bằng MERGE.

    Quy tắc 1: dedupe nguồn trước. Key trùng ở nguồn KHÔNG làm MERGE báo lỗi;
               nó lặng lẽ chọn bừa một dòng rồi báo thành công.
    Quy tắc 2: chặn bằng s.updated_at > t.updated_at, nếu không bản sửa cũ sẽ đè
               lên dòng mới hơn — số dòng không đổi, chỉ diff nội dung mới thấy.
    """
    con.execute(
        "-- grain: một dòng = một đơn hàng, cùng grain với core.orders_latest\n"
        "CREATE OR REPLACE TABLE core.orders_upsert AS SELECT * FROM core.orders_log LIMIT 0"
    )
    t0 = time.perf_counter()
    written = 0
    for d in dates:
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE stg_day AS
          SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC) AS rn
            FROM ({staged_select(raw_dir, d)})
          ) WHERE rn = 1
        """)
        written += con.execute("""
          MERGE INTO core.orders_upsert t USING stg_day s ON t.order_id = s.order_id
          WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE
          WHEN NOT MATCHED THEN INSERT
        """).fetchone()[0]
    n = con.execute("SELECT count(*) FROM core.orders_upsert").fetchone()[0]
    print(f"[upsert ] {time.perf_counter() - t0:5.1f}s  written={written}  rows={n}")


# ─────────────────────────── Task 5 ───────────────────────────


def build_refresh(con, dates, table="core.orders_refresh", window_days=WINDOW_DAYS) -> None:
    """Chiến lược C — refresh cả partition [D−window, D] từ log.

    _data_date <= D là điều kiện AS-OF: log đã có cả 10 file, nhưng vào ngày D
    pipeline mới chỉ thấy các file tới D. Pipeline chạy thật thì tự đúng.

    Hình dạng query: dedupe trên TOÀN BỘ log as-of trước, RỒI mới giữ bản thắng
    trong cửa sổ. Chọn toàn cục, đặt cục bộ. Lọc trước rồi dedupe sẽ làm sống lại
    phiên bản cũ — small không phát hiện ra, full thì để lại 7 đơn trùng.

    window_days=0 chính là cửa sổ SAI của Task 6b.
    """
    con.execute(
        "-- grain: một dòng = một đơn hàng, phiên bản mới nhất tính tới ngày refresh cuối\n"
        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM core.orders_log LIMIT 0"
    )
    t0 = time.perf_counter()
    deleted = written = 0
    for d in dates:
        lo = max(dates[0], d - dt.timedelta(days=window_days))
        con.execute("BEGIN")  # A07: crash giữa DELETE và INSERT không được để lại lỗ hổng
        deleted += con.execute(
            f"DELETE FROM {table} WHERE order_date BETWEEN ? AND ?", [lo, d]
        ).fetchone()[0]
        written += con.execute(f"""
          INSERT INTO {table}
          SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC, _data_date ASC) AS rn
            FROM core.orders_log
            WHERE _data_date <= DATE '{d}'
          ) WHERE rn = 1 AND order_date BETWEEN DATE '{lo}' AND DATE '{d}'
        """).fetchone()[0]
        con.execute("COMMIT")
    n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    label = "refresh" if window_days else "refr_BAD"
    print(f"[{label:8}] {time.perf_counter() - t0:5.1f}s  deleted={deleted} "
          f"written={written}  rows={n}")


# ─────────────────────────── Task 6 ───────────────────────────


def reconcile(con) -> bool:
    """Đếm KHÔNG phải là reconcile. EXCEPT cả hai chiều, luôn luôn."""
    counts = con.execute("""
      SELECT (SELECT count(*) FROM core.orders_latest),
             (SELECT count(*) FROM core.orders_upsert),
             (SELECT count(*) FROM core.orders_refresh)
    """).fetchone()
    print(f"[counts ] view={counts[0]} upsert={counts[1]} refresh={counts[2]}")

    diffs = {}
    pairs = [("core.orders_latest", "core.orders_upsert"),
             ("core.orders_upsert", "core.orders_latest"),
             ("core.orders_latest", "core.orders_refresh"),
             ("core.orders_refresh", "core.orders_latest")]
    for a, b in pairs:
        k = f"{a.split('_')[-1]}−{b.split('_')[-1]}"
        diffs[k] = con.execute(
            f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT SELECT * FROM {b})"
        ).fetchone()[0]
    print("[diffs  ]", "  ".join(f"{k}={v}" for k, v in diffs.items()))

    ok = len(set(counts)) == 1 and not any(diffs.values())
    print("[verdict]", "ba chiến lược ĐỒNG THUẬN" if ok else "LỆCH — xem diff ở trên")
    return ok


def main() -> None:
    p = argparse.ArgumentParser(description="A08 — ba chiến lược load")
    p.add_argument("--scale", choices=["small", "full"], default="small")
    p.add_argument("--bad", action="store_true",
                   help="chạy thêm cửa sổ SAI [D,D] vào core.orders_refresh_bad (Task 6b)")
    args = p.parse_args()

    root = data_root(args.scale)
    raw = root / "raw" / "orders"
    manifest_glob = (root / "raw" / "manifest" / "orders_*.json").as_posix()
    dates = [START + dt.timedelta(days=i) for i in range(NDAYS)]

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute(
        f"SET memory_limit='8GB'; SET threads=8; "
        f"SET temp_directory='{(root / 'tmp').as_posix()}';"
    )
    con.execute("CREATE SCHEMA IF NOT EXISTS core")

    print(f"=== A08 · scale={args.scale} · {dates[0]} → {dates[-1]} ===")
    try:
        measure(con, manifest_glob, dates)
        build_log(con, raw, dates)
        build_latest_view(con)
        build_upsert(con, raw, dates)
        build_refresh(con, dates)
        ok = reconcile(con)

        if args.bad:
            build_refresh(con, dates, "core.orders_refresh_bad", window_days=0)
            wrong = con.execute("""
              SELECT count(*) FROM (SELECT * FROM core.orders_latest
                                    EXCEPT SELECT * FROM core.orders_refresh_bad)
            """).fetchone()[0]
            print(f"[lesson ] cửa sổ sai: số dòng gần như không đổi, nhưng {wrong} dòng SAI")
            con.execute("DROP TABLE core.orders_refresh_bad")
    finally:
        con.close()

    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
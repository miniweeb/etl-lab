# """work/pipeline.py — một ngày: raw CSV -> canonical core.orders, runner gọi được.

# Không phải code mới. Đây là khâu lại:
#   - A07 cho thứ tự stage -> validate -> BEGIN; DELETE; INSERT; COMMIT
#   - A10 cho era dispatch, aligner, và CANON

# Khác A07 ở năm chỗ, mỗi chỗ có lý do:

# 1. Chữ ký đổi thành load_day(con, day, scale, run_id) — bỏ chaos, thêm run_id.
# 2. KHÔNG đụng ops.etl_runs. A07 tự mở dòng ledger bằng open_run(); từ A11 runner
#    sở hữu ledger. Gọi trực tiếp load_day thì không sinh dòng ledger nào.
# 3. Do đó lệnh UPDATE ledger rời khỏi transaction. A07 cố ý đặt nó BÊN TRONG
#    BEGIN...COMMIT để ledger không bao giờ khai success cho dữ liệu không tồn
#    tại. Giờ runner update sau khi hàm này trả về, nên có một khe hẹp: chết đúng
#    giữa COMMIT và UPDATE thì dữ liệu đã vào mà ledger còn 'running'. Đổi lại,
#    vòng đời lần chạy nằm gọn ở một nơi. Resume mặc định chạy lại mọi ngày không
#    xanh, nên khe đó tự lành ở lần chạy sau.
# 4. read_csv dùng columns= theo era thay vì all_varchar=true. all_varchar để
#    DuckDB đoán cột; era v3 đổi thứ tự cột nên đoán là đoán sai.
# 5. Làm sạch bằng CANON của A10 thay vì biểu thức viết thẳng. Một bản, dùng cho
#    cả migrate lẫn load.

# PHẠM VI: chỉ ghi warehouse core. KHÔNG ghi lake. Lake dùng COPY ... APPEND, mà
# APPEND không idempotent — replay 69 ngày qua đó sẽ nhân đôi 68 ngày A10 đã ghi,
# lặng lẽ. Lake thiếu đúng một ngày (2026-07-22) và vá tay một lần.
# """
# from __future__ import annotations

# import datetime as dt

# import duckdb

# from lib.labpaths import manifest_dir, raw_orders_dir
# from work.a10_schema_evolution.a10_canonical import (CANON, V1_COLS, V2_COLS,
#                                                      V3_COLS, era_of, open_day)


# def era_columns(day: dt.date | str) -> dict[str, str]:
#     """Schema read_csv cho era của ngày này. Một dòng, dựng từ thứ A10 đã có."""
#     return {"v1": V1_COLS, "v2": V2_COLS, "v3": V3_COLS}[era_of(day)]


# def ensure_core(con: duckdb.DuckDBPyConnection) -> None:
#     """core.orders ở schema canonical + ĐÚNG HAI cột lineage.

#     Hai cột, và đó là cả danh sách (§7.2): _data_date, _run_id. Tên file, giờ
#     nạp, kết quả, số dòng đều sống ở ops.etl_runs và cách một phép join.

#     LƯU Ý: bảng core.orders của A07 còn hình dạng v1 13 cột. CREATE TABLE IF NOT
#     EXISTS sẽ THẤY nó và không làm gì, rồi ngày đầu tiên chết với
#     'Binder Error: table orders has 13 columns but 18 values were supplied'.
#     DROP TABLE core.orders một lần trước khi chạy — nó là dữ liệu dẫn xuất,
#     raw + code dựng lại được.
#     """
#     con.execute("CREATE SCHEMA IF NOT EXISTS staging")
#     con.execute("CREATE SCHEMA IF NOT EXISTS core")
#     con.execute("""
#         CREATE TABLE IF NOT EXISTS core.orders (
#             order_id        VARCHAR,        -- 'ORD-0000000123', dạng v3
#             order_id_num    BIGINT,         -- dạng số, để khớp lịch sử trước v3
#             customer_id     BIGINT,
#             store_id        INTEGER,
#             order_ts        TIMESTAMP,
#             updated_at      TIMESTAMP,
#             status          VARCHAR,
#             payment_method  VARCHAR,        -- v3 gọi payment_type, aligner đổi lại
#             order_total     DECIMAL(14,2),
#             currency        VARCHAR,        -- v1 backfill 'USD' (biết)
#             discount_amount DECIMAL(14,2),  -- v1 backfill 0    (biết)
#             channel         VARCHAR,        -- v1 backfill NULL (không biết)
#             loyalty_tier    VARCHAR,        -- v1,v2 backfill NULL
#             items           STRUCT(sku VARCHAR, qty INTEGER,
#                                    unit_price DOUBLE, disc DOUBLE)[],
#             meta            VARCHAR,
#             order_date      DATE,
#             _data_date      DATE,           -- §7.2
#             _run_id         BIGINT          -- §7.2
#         )""")


# def load_day(con: duckdb.DuckDBPyConnection, day: dt.date,
#              scale: str = "small", run_id: int | None = None) -> int:
#     """Nạp một ngày. Trả về số dòng thô đã đọc — con số runner ghi vào ledger.

#     Trả về số THÔ (đếm trên aligned), không phải số đã publish. Đó là con số
#     manifest khai và là con số phép đối soát của A15 sẽ dùng. Chênh lệch giữa
#     hai số chính là các dòng CANON bỏ vì timestamp không parse nổi.
#     """
#     ensure_core(con)
#     src = raw_orders_dir(scale) / f"orders_{day}.csv"
#     rid = "CAST(NULL AS BIGINT)" if run_id is None else int(run_id)
#     #   Gọi ad-hoc thì _run_id NULL, nhưng vẫn ĐÚNG KIỂU BIGINT. Để trần chữ
#     #   NULL thì DuckDB suy ra kiểu khác và INSERT gãy.

#     # ---- 1. stage: đọc theo era, rồi canonical hoá -----------------------
#     open_day(con, str(day), raw_orders_dir(scale))     # tạo view src + aligned
#     raw_n = con.execute("SELECT count(*) FROM aligned").fetchone()[0]

#     con.execute("DROP TABLE IF EXISTS stg_day")
#     con.execute(f"""
#         CREATE TEMP TABLE stg_day AS
#         SELECT *, DATE '{day}' AS _data_date, {rid} AS _run_id
#         FROM ({CANON})""")

#     # ---- 2. validate: đối soát với lời khai của producer -----------------
#     # Đếm trên `aligned`, KHÔNG phải stg_day: CANON bỏ các dòng không parse nổi
#     # timestamp, nên stg_day luôn nhỏ hơn manifest vài chục dòng. So sai bảng
#     # thì mọi ngày đều đỏ.
#     mf = (manifest_dir(scale) / f"orders_{day}.json").as_posix()
#     expected = con.execute(f"SELECT rows FROM read_json_auto('{mf}')").fetchone()[0]
#     if raw_n != expected:
#         raise ValueError(f"{day}: đọc được {raw_n:,} dòng, manifest khai "
#                          f"{expected:,} - short load, KHÔNG publish")
#         #   ValueError chứ không TransientError: đây là lỗi DỮ LIỆU, thử lại
#         #   không sửa được. Luật phân loại lỗi của A07.

#     # ---- 3. publish: xoá rồi ghi, trong MỘT transaction ------------------
#     con.execute("BEGIN TRANSACTION")
#     try:
#         con.execute("DELETE FROM core.orders WHERE _data_date = ?", [day])
#         #   Chạy cả ở lần nạp đầu tiên — xoá 0 dòng, không tốn gì. Lần đầu và
#         #   lần chạy lại đi CÙNG MỘT NHÁNH CODE. Đó là toàn bộ idempotency.
#         con.execute("INSERT INTO core.orders SELECT * FROM stg_day")
#         con.execute("COMMIT")
#     except Exception:
#         con.execute("ROLLBACK")
#         raise

#     return raw_n


# if __name__ == "__main__":
#     import sys

#     from lib.labpaths import tmp_dir, warehouse_path

#     scale = sys.argv[1] if len(sys.argv) > 1 else "small"
#     con = duckdb.connect(str(warehouse_path(scale)))
#     con.execute(f"SET memory_limit='8GB'; SET threads=8; "
#                 f"SET temp_directory='{tmp_dir(scale).as_posix()}';")
#     for d in (dt.date(2026, 6, 2), dt.date(2026, 7, 20)):
#         print(d, era_of(d), load_day(con, d, scale=scale))
#     con.close()
#     # small: 2026-06-02 v1 59024  |  2026-07-20 v2 59743
#     # Mỗi số phải bằng `rows` của manifest ngày đó. Một phép bằng nhau chứng
#     # minh cả mạch: đọc theo era, aligner, canonicalizer, publish nguyên tử.

"""work/pipeline.py — một ngày: raw CSV -> canonical core.orders, runner gọi được.

Không phải code mới. Đây là khâu lại:
  - A07 cho thứ tự stage -> validate -> BEGIN; DELETE; INSERT; COMMIT
  - A10 cho era dispatch, aligner, và CANON
  - A05 cho reconciliation guard và quarantine

Khác A07 ở năm chỗ, mỗi chỗ có lý do:

1. Chữ ký đổi thành load_day(con, day, scale, run_id) — bỏ chaos, thêm run_id.
2. KHÔNG đụng ops.etl_runs. A07 tự mở dòng ledger bằng open_run(); từ A11 runner
   sở hữu ledger. Gọi trực tiếp load_day thì không sinh dòng ledger nào.
3. Do đó lệnh UPDATE ledger rời khỏi transaction. A07 cố ý đặt nó BÊN TRONG
   BEGIN...COMMIT để ledger không bao giờ khai success cho dữ liệu không tồn
   tại. Giờ runner update sau khi hàm này trả về, nên có một khe hẹp: chết đúng
   giữa COMMIT và UPDATE thì dữ liệu đã vào mà ledger còn 'running'. Đổi lại,
   vòng đời lần chạy nằm gọn ở một nơi. Resume mặc định chạy lại mọi ngày không
   xanh, nên khe đó tự lành ở lần chạy sau.
4. read_csv dùng columns= theo era thay vì all_varchar=true. all_varchar để
   DuckDB đoán cột; era v3 đổi thứ tự cột nên đoán là đoán sai.
5. Làm sạch bằng CANON của A10 thay vì biểu thức viết thẳng. Một bản, dùng cho
   cả migrate lẫn load.

PHẠM VI: chỉ ghi warehouse core. KHÔNG ghi lake. Lake dùng COPY ... APPEND, mà
APPEND không idempotent — replay 69 ngày qua đó sẽ nhân đôi 68 ngày A10 đã ghi,
lặng lẽ, và không phép kiểm nào trước A15 bắt được. Lake thiếu đúng một ngày
(2026-07-22) và được vá tay một lần.
"""
from __future__ import annotations

import datetime as dt

import duckdb

from lib.labpaths import manifest_dir, quarantine_dir, raw_orders_dir
from work.a10_schema_evolution.a10_canonical import (ALIGN, CANON, V1_COLS,
                                                     V2_COLS, V3_COLS, era_of)


def era_columns(day: dt.date | str) -> dict[str, str]:
    """Schema read_csv cho era của ngày này. Một dòng, dựng từ thứ A10 đã có."""
    return {"v1": V1_COLS, "v2": V2_COLS, "v3": V3_COLS}[era_of(str(day))]


def ensure_core(con: duckdb.DuckDBPyConnection) -> None:
    """core.orders ở schema canonical + ĐÚNG HAI cột lineage.

    Hai cột, và đó là cả danh sách (§7.2): _data_date, _run_id. Tên file, giờ
    nạp, kết quả, số dòng đều sống ở ops.etl_runs và cách một phép join.

    LƯU Ý: bảng core.orders của A07 còn hình dạng v1 13 cột. CREATE TABLE IF NOT
    EXISTS sẽ THẤY nó và không làm gì, rồi ngày đầu tiên chết với
    'Binder Error: table orders has 13 columns but 18 values were supplied'.
    DROP TABLE core.orders một lần trước khi chạy — nó là dữ liệu dẫn xuất,
    raw + code dựng lại được.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("""
        CREATE TABLE IF NOT EXISTS core.orders (
            order_id        VARCHAR,        -- 'ORD-0000000123', dạng v3
            order_id_num    BIGINT,         -- dạng số, để khớp lịch sử trước v3
            customer_id     BIGINT,
            store_id        INTEGER,
            order_ts        TIMESTAMP,
            updated_at      TIMESTAMP,
            status          VARCHAR,
            payment_method  VARCHAR,        -- v3 gọi payment_type, aligner đổi lại
            order_total     DECIMAL(14,2),
            currency        VARCHAR,        -- v1 backfill 'USD' (biết)
            discount_amount DECIMAL(14,2),  -- v1 backfill 0    (biết)
            channel         VARCHAR,        -- v1 backfill NULL (không biết)
            loyalty_tier    VARCHAR,        -- v1,v2 backfill NULL
            items           STRUCT(sku VARCHAR, qty INTEGER,
                                   unit_price DOUBLE, disc DOUBLE)[],
            meta            VARCHAR,
            order_date      DATE,
            _data_date      DATE,           -- §7.2
            _run_id         BIGINT          -- §7.2
        )""")


def load_day(con: duckdb.DuckDBPyConnection, day: dt.date,
             scale: str = "small", run_id: int | None = None) -> int:
    """Nạp một ngày. Trả về số dòng THÔ đã đọc — con số runner ghi vào ledger.

    Trả về số thô (đếm trên bảng raw), không phải số đã publish. Đó là con số
    manifest khai và là con số phép đối soát của A15 sẽ dùng. Chênh lệch giữa
    hai số chính là các dòng CANON bỏ vì timestamp không parse nổi.
    """
    ensure_core(con)
    f = raw_orders_dir(scale) / f"orders_{day}.csv"
    rid = "CAST(NULL AS BIGINT)" if run_id is None else int(run_id)
    #   Gọi ad-hoc thì _run_id NULL, nhưng vẫn ĐÚNG KIỂU BIGINT. Để trần chữ
    #   NULL thì DuckDB suy ra kiểu khác và INSERT gãy.

    # ── 1. đọc khoan dung ────────────────────────────────────────────────
    # store_rejects=true: parse được dòng nào thì giữ dòng đó, dòng hỏng đẩy
    # sang bảng reject_errors thay vì giết cả file. Đây là thứ biến 07-22 từ
    # "mất trọn một ngày" thành "mất 151 dòng, có biên bản".
    #
    # Tên bảng theo §7.1: shopcore_orders_day_raw nói RÕ feed nào. Một warehouse
    # đầy những bảng tên `day_raw` sẽ không đọc nổi vào ngày nguồn thứ hai về.
    con.execute(f"""
        CREATE OR REPLACE TABLE staging.shopcore_orders_day_raw AS
        SELECT *,
               DATE '{day}' AS _data_date,   -- §7.2, đóng dấu ngay lúc ingest:
               {rid}        AS _run_id       --  đúng hai cột, không hơn
        FROM read_csv('{f.as_posix()}', header=true,
                      columns={era_columns(day)},
                      store_rejects=true)""")

    # ── 2. giữ bằng chứng ────────────────────────────────────────────────
    n_rejects = con.execute("SELECT count(*) FROM reject_errors").fetchone()[0]
    if n_rejects:
        out = quarantine_dir(scale) / "orders_badlines" / f"file_date={day}"
        out.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT DATE '{day}' AS _data_date, {rid} AS _run_id,
                         now() AS _rejected_at,
                         line, column_name, error_type, error_message, csv_line
                  FROM reject_errors)
            TO '{(out / "rejects.parquet").as_posix()}' (FORMAT parquet)""")
        con.execute("DELETE FROM reject_errors")
        #   reject_errors CỘNG DỒN theo connection. Không xoá thì ngày N+1 sẽ
        #   copy lại cả reject của ngày N — và file quarantine của mọi ngày sau
        #   đều phình lên, âm thầm sai.
        bad_lines = con.execute(
            f"SELECT count(DISTINCT line) FROM read_parquet("
            f"'{(out / 'rejects.parquet').as_posix()}')").fetchone()[0]
        print(f"  [quarantine] {day}: {bad_lines} dòng hỏng -> {out}")
        #   Đếm DISTINCT line, không đếm bản ghi: một dòng bị cắt sinh MỘT bản
        #   ghi cho MỖI cột thiếu. 07-22 có 1.651 bản ghi nhưng chỉ 151 dòng.

    # ── 3. đối soát với lời khai của producer ────────────────────────────
    # Check rẻ nhất, giá trị nhất (A05). Nó biến "nạp hụt lặng lẽ" thành
    # "hỏng ồn ào". So trên bảng RAW, không phải sau CANON: CANON bỏ các dòng
    # timestamp không parse nổi, nên bảng sau nó luôn nhỏ hơn manifest vài chục
    # dòng và mọi ngày đều đỏ oan.
    mf = manifest_dir(scale) / f"orders_{day}.json"
    expected = con.execute(
        f"SELECT rows FROM read_json_auto('{mf.as_posix()}')").fetchone()[0]
    loaded = con.execute(
        "SELECT count(*) FROM staging.shopcore_orders_day_raw").fetchone()[0]
    if loaded != expected:
        raise ValueError(f"{day}: đọc được {loaded:,} dòng, manifest khai "
                         f"{expected:,} - short load, KHÔNG publish")
        #   ValueError chứ không TransientError: đây là lỗi DỮ LIỆU, thử lại
        #   không sửa được. Luật phân loại lỗi của A07.
        #
        #   Với 07-22: manifest khai 58.400 và store_rejects đọc được đúng
        #   58.400 — 151 dòng hỏng nằm NGOÀI con số producer khai. Phép cộng
        #   đóng, và đó là lý do ngày này giờ xanh được.

    # ── 4. canonical hoá: era dispatch -> aligner -> CANON ───────────────
    era = era_of(str(day))
    con.execute("CREATE OR REPLACE VIEW src AS "
                "SELECT * FROM staging.shopcore_orders_day_raw")
    con.execute(f"CREATE OR REPLACE VIEW aligned AS {ALIGN[era]}")
    #   KHÔNG dùng open_day() của A10 ở đây: hàm đó tự đọc read_csv, tức đọc
    #   file LẦN THỨ HAI và lần đó không có store_rejects — 07-22 sẽ lại chết.
    #   src trỏ vào bảng đã đọc xong, aligner giữ nguyên bản A10.

    con.execute("DROP TABLE IF EXISTS stg_day")
    con.execute(f"""
        CREATE TEMP TABLE stg_day AS
        SELECT *, DATE '{day}' AS _data_date, {rid} AS _run_id
        FROM ({CANON})""")

    # ── 5. publish: xoá rồi ghi, trong MỘT transaction ───────────────────
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM core.orders WHERE _data_date = ?", [day])
        #   Chạy cả ở lần nạp đầu tiên — xoá 0 dòng, không tốn gì. Lần đầu và
        #   lần chạy lại đi CÙNG MỘT NHÁNH CODE. Đó là toàn bộ idempotency, và
        #   là lý do logic resume của runner đáng tin: một ngày chết giữa chừng
        #   không để lại gì.
        con.execute("INSERT INTO core.orders SELECT * FROM stg_day")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return loaded


if __name__ == "__main__":
    import sys

    from lib.labpaths import tmp_dir, warehouse_path

    scale = sys.argv[1] if len(sys.argv) > 1 else "small"
    con = duckdb.connect(str(warehouse_path(scale)))
    con.execute(f"SET memory_limit='8GB'; SET threads=8; "
                f"SET temp_directory='{tmp_dir(scale).as_posix()}';")
    for d in (dt.date(2026, 6, 2), dt.date(2026, 7, 20), dt.date(2026, 7, 22)):
        print(d, era_of(str(d)), load_day(con, d, scale=scale))
    con.close()
    # small: 2026-06-02 v1 59024 | 2026-07-20 v2 59743 | 2026-07-22 v2 58400
    # Mỗi số phải bằng `rows` của manifest ngày đó. Ngày thứ ba là ngày A10 bỏ
    # lại: nó xanh được KHÔNG PHẢI vì file đã sửa — file vẫn hỏng y nguyên, raw
    # là bằng chứng bất biến — mà vì cách đọc đã khoan dung và 151 dòng hỏng
    # giờ nằm trong quarantine chứ không nằm trong một stack trace.
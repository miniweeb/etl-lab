"""Pre-flight contract gate: kiểm một file CSV orders thô của shopcore đối chiếu
với contract đã ký TRƯỚC KHI nạp bất kỳ dòng nào.

Exit 0 = mọi file pass; 1 = ít nhất một file hỏng. Mã thoát đó chính là thứ cho
phép Task 8 tự động hoá gate này.

Chính contract RA LỆNH dựng cái này: change_management.policy ghi rõ pipeline
phía consumer "phải hỏng ỒN ÀO (pre-load gate) khi có drift không khai báo, thay
vì nạp rác vào".

Thứ tự kiểm, và nó DỪNG SỚM khi tầng sâu hơn trở nên vô nghĩa:
    1. đặt tên file        -> sai feed thì không cần đọc tiếp
    2. tín hiệu hoàn tất    -> manifest
    3. schema               -> tên cột và THỨ TỰ cột, chính xác
    4. probe giá trị (mẫu)  -> cột sai thì check giá trị vô nghĩa
    5. cửa sổ sửa trễ
"""
import argparse
import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import yaml

from lib.labpaths import data_root

from work.a06_data_contract.a06_contract_model import Contract

REPO_ROOT = Path(__file__).resolve().parents[2]          # work/ -> gốc repo
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"

SAMPLE_ROWS = 100_000
#   Lựa chọn THIẾT KẾ của gate, KHÔNG phải điều khoản contract. Đây là lý do
#   sweep full scale (file ~350MB) vẫn chạy dưới một phút: gate đọc 100k dòng
#   rồi dừng. Gate đắt là gate bị bỏ qua.

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
#   Dạng VIẾT của order_ts được contract ghim (schema.order_ts: "written as
#   'YYYY-MM-DD HH:MM:SS', no offset"). Đây là cách đánh vần nó cho strptime.

NAMING_RX = re.compile(r"^orders_\d{4}-\d{2}-\d{2}\.csv$")
#   delivery.path_pattern "orders_YYYY-MM-DD.csv" dịch sang regex.


def pct(s) -> float:
    """'0.5%' -> 0.005 — contract viết tolerance theo cách con người đọc."""
    return float(str(s).split("%")[0].strip()) / 100.0


def load_contract() -> dict:
    """Nạp contract và validate qua Pydantic: contract hỏng thì KHÔNG chạy gì cả."""
    raw = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    Contract.model_validate(raw)
    return raw


def check_file(csv_path: Path, contract: dict) -> list[str]:
    """Trả về danh sách vấn đề HƯỚNG TỚI PRODUCER. Rỗng = PASS.

    Mọi thông điệp phải để shopcore hành động được mà KHÔNG cần đọc code của ta —
    đó là lý do chúng trích dẫn điều khoản chứ không in stack trace.
    """
    problems: list[str] = []
    con = duckdb.connect()          # kết nối in-memory dùng một lần rồi vứt;
                                    # warehouse KHÔNG bị đụng tới

    # ---- 1. delivery: đặt tên file ---------------------------------------
    if not NAMING_RX.match(csv_path.name):
        return [f"file name '{csv_path.name}' does not match delivery.path_pattern "
                f"'{contract['delivery']['path_pattern']}' - wrong file or wrong feed?"]
        # return NGAY, không phải append: sai feed thì mọi check sau đều vô nghĩa.
    file_date = date.fromisoformat(csv_path.stem.split("_")[1])

    # ---- 2. tín hiệu hoàn tất: manifest ----------------------------------
    manifest_path = csv_path.parent.parent / "manifest" / (csv_path.stem + ".json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        size = csv_path.stat().st_size
        if size != manifest["bytes"]:
            problems.append(f"bytes on disk ({size}) != manifest.bytes "
                            f"({manifest['bytes']}) - truncated or partial delivery? "
                            "(quality.reconciliation)")
    else:
        # KHÔNG fail: file ad-hoc (bản copy scratch của Task 7) không có manifest.
        # Gate NÓI RA điều đó thay vì im lặng bỏ qua một check.
        print(f"  [warn] {csv_path.name}: no manifest found - completeness signal "
              "not verified (ad-hoc file?)")

    # ---- 3. schema: tên chính xác, THỨ TỰ chính xác ----------------------
    expected = [c["name"] for c in contract["schema"]]
    sniffed = con.execute("SELECT Columns FROM sniff_csv(?)", [str(csv_path)]).fetchone()[0]
    actual = [c["name"] for c in sniffed]
    #   CHỈ lấy TÊN từ sniffer, KHÔNG lấy kiểu. Kiểu đoán được sẽ TRÔI theo rác
    #   lọt vào mẫu — sniffer gọi customer_id là DOUBLE (vì rác Excel-float
    #   "123456.0" của A03) và gán cho updated_at một timezone CHẲNG AI HỨA.
    #   Điểm cố định để đối chiếu là block schema của contract.
    if actual != expected:
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        if missing:
            problems.append(f"missing column(s) {missing} - dropped or renamed upstream? "
                            "change_management: that is a MAJOR bump with 30 days notice.")
        if extra:
            problems.append(f"unexpected column(s) {extra} - new export version? "
                            "change_management: additive needs a MINOR bump, 7 days notice.")
        if not missing and not extra:
            problems.append(f"column order changed: expected {expected}, got {actual}")
        return problems     # check giá trị là vô nghĩa trên cột sai

    # ---- 4. probe giá trị trên MẪU, đọc mọi thứ dưới dạng TEXT -----------
    con.execute("CREATE TEMP TABLE sample AS SELECT * FROM "
                "read_csv(?, header=true, all_varchar=true) LIMIT ?",
                [str(csv_path), SAMPLE_ROWS])
    #   all_varchar=true: gate TỰ phán xét kiểu bằng TRY_CAST, không để DuckDB
    #   phán hộ. LIMIT là thứ giữ cho sweep full scale rẻ.
    total = con.execute("SELECT count(*) FROM sample").fetchone()[0]
    tol = {k: pct(v) for k, v in contract["quality"]["row_level_tolerances"].items()}
    enums = {c["name"]: c["allowed_values"]
             for c in contract["schema"] if "allowed_values" in c}
    status_list = ", ".join(f"'{s}'" for s in enums["status"])

    ts_recover = (f"COALESCE(try_strptime(order_ts, ['{TS_FORMAT}', "
                  "'%d/%m/%Y %H:%M:%S']), TRY_CAST(order_ts AS TIMESTAMP))")
    #   "KHÔNG PARSE ĐƯỢC" HẸP HƠN "SAI ĐỊNH DẠNG". ~0,7% dòng phá dạng viết đã
    #   ghim (DD/MM và ISO-T) — đó là VI PHẠM bạn đếm và báo cáo ở A05. Nhưng
    #   tolerance hard-fail 0,5% dành cho unparseable_order_ts: những dòng KHÔNG
    #   định dạng nào cứu nổi (ngày bất khả thi, ~0,03%). Vì thế ts_recover thử
    #   các dạng cứu hộ TRƯỚC khi tuyên bố một dòng đã mất.

    # check -> (cột hiện trong ví dụ, điều kiện SQL cho dòng XẤU,
    #           tolerance, điều khoản trích cho producer)
    checks: dict[str, tuple[str, str, float, str]] = {
        "order_id_not_castable": (
            "order_id", "order_id IS NULL OR TRY_CAST(order_id AS BIGINT) IS NULL",
            0.0, "schema.order_id: BIGINT, not nullable - no tolerance declared"),
        "unparseable_order_ts": (
            "order_ts", f"order_ts IS NULL OR {ts_recover} IS NULL",
            tol["unparseable_order_ts"], "quality.row_level_tolerances"),
        "null_or_invalid_status": (
            "status", f"status IS NULL OR status NOT IN ({status_list})",
            tol["null_or_invalid_status"], "schema.status: allowed_values + case"),
        # customer_id: nullable: true -> CHỈ đếm giá trị có mặt mà không cast được sang BIGINT
        "customer_id_not_castable": (
            "customer_id",
            "customer_id IS NOT NULL AND TRY_CAST(customer_id AS BIGINT) IS NULL",
            0.0,
            "schema.customer_id: BIGINT, nullable: true",
        ),
        # store_id: INTEGER, not nullable
        "store_id_not_castable": (
            "store_id",
            "store_id IS NULL OR TRY_CAST(store_id AS INTEGER) IS NULL",
            0.0,
            "schema.store_id: INTEGER, not nullable - no tolerance declared",
        ),
        # unparseable_order_total: định dạng USD thuần, scale 2, không âm, không ký hiệu
        "unparseable_order_total": (
            "order_total",
            "order_total IS NULL OR NOT regexp_matches(order_total, '^[0-9]+\\.[0-9]{2}$')",
            tol["unparseable_order_total"],
            "quality.row_level_tolerances.unparseable_order_total",
        ),
    }

    for name, (col, bad, tolerance, clause) in checks.items():
        n = con.execute(f"SELECT count(*) FROM sample WHERE {bad}").fetchone()[0]
        if n / total > tolerance:
            ex = [r[0] for r in con.execute(
                f"SELECT DISTINCT {col} FROM sample WHERE {bad} LIMIT 3").fetchall()]
            problems.append(f"{name}: {n}/{total} sampled rows ({n / total:.2%}) exceed "
                            f"tolerance {tolerance:.2%} ({clause}). Examples: {ex}")

    # payment_method: enum và case ĐƯỢC GHIM, nhưng row_level_tolerances IM LẶNG
    # về nó. Không có ngưỡng đã thoả thuận -> ĐO và BÁO CÁO, không hard-fail.
    pay_list = ", ".join(f"'{p}'" for p in enums["payment_method"])
    n = con.execute("SELECT count(*) FROM sample WHERE payment_method IS NULL "
                    f"OR payment_method NOT IN ({pay_list})").fetchone()[0]
    if n:
        print(f"  [warn] {csv_path.name}: payment_method {n}/{total} ({n / total:.2%}) "
              "outside the enum - a violation to report weekly (slo_reporting), "
              "but no hard-fail tolerance is declared. Amendment material.")

    # ---- 5. cửa sổ sửa trễ ------------------------------------------------
    window_days = contract["late_corrections"]["window_days"]
    min_ts, max_ts = con.execute(
        f"SELECT min({ts_recover}), max({ts_recover}) FROM sample"
    ).fetchone()

    if min_ts is not None and max_ts is not None:
        min_date = min_ts.date()
        max_date = max_ts.date()
        earliest_allowed = file_date - timedelta(days=window_days)

        if min_date < earliest_allowed:
            problems.append(
                f"late_corrections window breached: earliest timestamp {min_date} is older than "
                f"allowed limit {earliest_allowed} (late_corrections.window_days: {window_days})"
            )
        if max_date > file_date:
            problems.append(
                f"future order detected: latest timestamp {max_date} > file business date {file_date} "
                "(schema.order_ts: range [business_date - 7 days, business_date + 1 day))"
            )

    con.close()
    return problems


def main() -> None:
    """CLI thực thi Pre-flight contract gate."""
    parser = argparse.ArgumentParser(description="Pre-flight contract gate")
    parser.add_argument("files", nargs="*", help="Đường dẫn file CSV cụ thể cần kiểm tra")
    parser.add_argument("--scale", choices=["small", "full"], default="small")
    parser.add_argument("--start", help="Ngày bắt đầu (YYYY-MM-DD)")
    parser.add_argument("--end", help="Ngày kết thúc (YYYY-MM-DD)")
    args = parser.parse_args()

    contract = load_contract()

    if args.files:
        paths = [Path(f) for f in args.files]
    else:
        # orders_dir = DATA_ROOT / args.scale / "raw" / "orders" # task 9 a06 đổi thành hàng ở dưới
        orders_dir = data_root(args.scale) / "raw" / "orders"
        if not args.start or not args.end:
            parser.error("Cần truyền file cụ thể hoặc đủ cả --start và --end")

        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
        days_count = (end_date - start_date).days + 1

        paths = [
            orders_dir / f"orders_{start_date + timedelta(days=i)}.csv"
            for i in range(days_count)
        ]

    passed = 0
    total = len(paths)

    for p in paths:
        if not p.exists():
            print(f"[FAIL] {p.name}: file does not exist - freshness breach! (delivery.freshness_sla)")
            continue

        probs = check_file(p, contract)
        if probs:
            print(f"[FAIL] {p.name}")
            for item in probs:
                print(f"   - {item}")
        else:
            print(f"[PASS] {p.name}")
            passed += 1

    contract_name = contract["contract"]
    contract_ver = contract["version"]
    print(f"\n{passed}/{total} files passed contract {contract_name} v{contract_ver}")

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
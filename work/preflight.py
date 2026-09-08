"""Preflight: kiểm tra MÔI TRƯỜNG trước khi chạm vào dữ liệu.

Thứ làm hỏng pipeline lúc 2 giờ sáng hiếm khi là SQL. Ổ đĩa đầy. File chưa về.
Một process khác đang giữ database. Config trỏ thư mục tạm về ổ hệ thống. Mỗi
thứ đó kiểm được trong chưa tới một giây, TRƯỚC KHI dữ liệu di chuyển.

File này nằm THẲNG trong work/ chứ không trong work/a07_idempotency/, vì nó là
hạ tầng dùng chung: A11 sẽ import nó trước từng ngày mà backfill runner load.

Chạy độc lập:
    python -m work.preflight --scale small --date 2026-06-03
    python -m work.preflight --scale small --date 2026-06-03 --tmp C:\\Windows\\Temp

Mã thoát 0 = bảy PASS. Mã thoát 1 = ít nhất một FAIL.
Mã thoát mới là thứ scheduler tuân theo; báo cáo đỏ mà vẫn exit 0 là chuông báo
cháy không lắp pin.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from lib.labpaths import data_root, warehouse_path  # noqa: E402

CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"
MIN_FREE_GB = 5.0


# ── kết quả một check ────────────────────────────────────────────────────
# (số thứ tự, tên, ok?, chi tiết, gợi ý sửa)
Result = tuple[int, str, bool, str, str]


def _fmt(r: Result) -> str:
    n, name, ok, detail, hint = r
    tag = "[PASS]" if ok else "[FAIL]"
    line = f"{tag} {n}. {name:<32} {detail}"
    if not ok and hint:
        line += f"\n         fix: {hint}"
    return line


def run_checks(scale: str, day: date, tmp_override: str | None = None) -> bool:
    """Trả về True nếu cả bảy check PASS. In báo cáo ra console."""
    results: list[Result] = []
    root = data_root(scale)
    con = None

    # ---- 1. data root ghi được ------------------------------------------
    # Thư mục CÓ THỂ tồn tại mà vẫn từ chối ghi (quyền, ổ mạng, read-only mount).
    # Nên phép kiểm đúng là THỬ GHI, không phải root.exists().
    probe = root / ".preflight_probe"
    try:
        root.mkdir(parents=True, exist_ok=True)
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        results.append((1, "data root writable", True, str(root), ""))
    except OSError as exc:
        results.append((1, "data root writable", False, f"{root}: {exc}",
                        "kiểm quyền ghi, hoặc ETL_LAB_DATA đang trỏ sai chỗ"))

    # ---- 2. chỗ trống trên Ổ CHỨA DỮ LIỆU -------------------------------
    # Chỗ trống trên ổ C không giúp được gì nếu dữ liệu nằm ở ổ D.
    try:
        free_gb = shutil.disk_usage(root).free / 1024**3
        ok = free_gb >= MIN_FREE_GB
        results.append((2, "free space on data volume", ok,
                        f"{free_gb:.1f} GB free on {root.drive or root.anchor}",
                        f"cần ít nhất {MIN_FREE_GB} GB; dọn {root}/tmp trước"))
    except OSError as exc:
        results.append((2, "free space on data volume", False, str(exc), ""))

    # ---- 3. thư mục tạm nằm CÙNG Ổ với dữ liệu --------------------------
    # Spill phải nằm cạnh dữ liệu. Một query lớn spill sang ổ hệ thống sẽ làm
    # đầy ổ C và treo cả máy — đúng bài học ổ C còn 14 GB ở A00.
    tmp = Path(tmp_override) if tmp_override else root / "tmp"
    data_drive = os.path.splitdrive(str(root))[0].upper()
    tmp_drive = os.path.splitdrive(str(tmp))[0].upper()
    ok = tmp_drive == data_drive
    results.append((3, "temp dir on data volume", ok,
                    f"{tmp}" if ok else f"{tmp} is not on the data volume ({data_drive})",
                    f"spill belongs next to the data: SET temp_directory='{root / 'tmp'}'"))

    # ---- 4. source CSV + manifest ---------------------------------------
    # TODO(you): BA cách một delivery bị hỏng, gộp thành ĐÚNG MỘT dòng results:
    #   a) file CSV không tồn tại
    #   b) file CSV tồn tại nhưng 0 byte
    #   c) file manifest JSON không tồn tại
    # ok chỉ khi KHÔNG rơi vào cả ba.
    #
    # Khi FAIL, detail phải NÊU ĐÍCH DANH đường dẫn sai — người trực lúc 2 giờ
    # sáng cần địa chỉ, không cần lời than. Và hint phải nói làm gì:
    #   - gõ nhầm --date?
    #   - chưa chạy generator?
    #   - file 0 byte là SỰ CỐ phải báo lên shopcore, không phải lỗi của bạn
    #
    # Đường dẫn để dùng:
    #   csv      = root / "raw" / "orders" / f"orders_{day}.csv"
    #   manifest = root / "raw" / "manifest" / f"orders_{day}.json"
    # Gợi ý: csv.exists(), csv.stat().st_size, manifest.exists()
    # results.append((4, "source CSV + manifest", False, "TODO(you)", "viết check 4"))
    # tạm đổi dòng cuối của check 4 thành dòng dưới
    # results.append((4, "source CSV + manifest", True, "TODO(you) - tạm bỏ qua", ""))
    # ---- 4. source CSV + manifest ---------------------------------------
    csv = root / "raw" / "orders" / f"orders_{day}.csv"
    manifest = root / "raw" / "manifest" / f"orders_{day}.json"

    if not csv.exists():
        results.append(
            (
                4,
                "source CSV + manifest",
                False,
                f"{csv} not found",
                "kiểm tra lại --date, hoặc chạy generator nếu chưa sinh dữ liệu",
            )
        )
    elif csv.stat().st_size == 0:
        results.append(
            (
                4,
                "source CSV + manifest",
                False,
                f"{csv} is 0 bytes",
                "truyền file đứt đoạn — sự cố thượng nguồn, báo ngay cho shopcore",
            )
        )
    elif not manifest.exists():
        results.append(
            (
                4,
                "source CSV + manifest",
                False,
                f"{manifest} not found",
                "thiếu completeness signal — shopcore chưa hoàn tất xuất file hoặc mất manifest",
            )
        )
    else:
        results.append(
            (
                4,
                "source CSV + manifest",
                True,
                f"{csv.name} ({csv.stat().st_size / 1024**2:.1f} MB) + manifest",
                "",
            )
        )

    # ---- 5. warehouse ghi được, không bị khoá ---------------------------
    # MỞ CONNECTION CHÍNH LÀ phép dò khoá — không cần thủ thuật nào khác.
    # Kernel Jupyter đang giữ warehouse.duckdb là nguyên nhân kinh điển.
    wh = warehouse_path(scale)
    try:
        con = duckdb.connect(str(wh))
        results.append((5, "warehouse writable, not locked", True, str(wh), ""))
    except duckdb.IOException as exc:
        results.append((5, "warehouse writable, not locked", False, f"{wh}: {exc}",
                        "đóng kernel Jupyter hoặc process khác đang mở warehouse"))

    # ---- 6. tạo được schema ---------------------------------------------
    # CREATE SCHEMA IF NOT EXISTS là phép dò quyền MIỄN PHÍ: nó không đổi gì
    # nếu schema đã có, nhưng sẽ nổ ngay nếu connection là read-only.
    if con is None:
        # Báo TRUNG THỰC là bỏ qua, đừng in PASS cho một check chưa chạy.
        results.append((6, "schemas creatable", False, "skipped: no connection", ""))
    else:
        try:
            for s in ("staging", "core", "ops"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
            results.append((6, "schemas creatable", True, "staging, core, ops", ""))
        except duckdb.Error as exc:
            results.append((6, "schemas creatable", False, str(exc), ""))

    # ---- 7. contract parse được -----------------------------------------
    # YAML hỏng phải fail Ở ĐÂY, không phải ở file thứ 30 giữa pipeline.
    # Đây chính là bài học T5 của A06, dời lên sớm hơn một bậc.
    try:
        c = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
        results.append((7, "contract parseable", True,
                        f"{c['contract']} v{c['version']}", ""))
    except (OSError, yaml.YAMLError) as exc:
        results.append((7, "contract parseable", False, f"{CONTRACT}: {exc}",
                        "chạy python -m work.a06_data_contract.a06_contract_model để xem lỗi chi tiết"))

    if con is not None:
        con.close()

    for r in results:
        print(_fmt(r))
    return all(r[2] for r in results)


def main() -> None:
    p = argparse.ArgumentParser(description="Preflight environment checks")
    p.add_argument("--scale", choices=["small", "full"], default="small")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--tmp", default=None, help="ghi đè thư mục tạm (để demo check 3)")
    args = p.parse_args()

    ok = run_checks(args.scale, date.fromisoformat(args.date), args.tmp)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
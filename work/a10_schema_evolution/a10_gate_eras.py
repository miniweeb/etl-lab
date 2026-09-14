"""A10 Task 8 — gate A06 biết chọn contract theo era, và biết phân biệt
"file drift" với "file không đọc nổi".

  python -m work.a10_schema_evolution.a10_gate_eras --scale small
  python -m work.a10_schema_evolution.a10_gate_eras --scale small -v

Gate cũ mô tả v1 nên giờ nó (đúng đắn) từ chối 2/3 feed. Việc cần làm là
TIẾN HOÁ CONTRACT, không phải nới code.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from work.a06_data_contract.a06_contract_model import load_typed
from work.a10_schema_evolution.a10_bootstrap import BAD_DAY, boot, section
from work.a10_schema_evolution.a10_canonical import era_of

REPO_ROOT = Path(__file__).resolve().parents[2]

# Ba bản của CÙNG một contract, mỗi bản có effective_from riêng.
#   1.3.0  era v1 — bản gốc đã ký, hiệu lực tới 2026-07-15
#   2.0.0  era v2 — ba cột thêm + luật reconciliation MỚI
#          (order_total = items_total - discount_amount). Thêm cột thôi thì chỉ
#          minor; VIẾT LẠI NGHĨA của một business rule đang có mới là thứ phá
#          mọi consumer -> MAJOR. Gộp luôn điều khoản locale của bản v1.4.0 hồi
#          A06, vốn kẹt ở proposed vì shopcore chưa bao giờ ký.
#   3.0.0  era v3 — rename, đổi type id, loyalty_tier, items.disc, đổi thứ tự cột.
CONTRACTS = {
    "v1": "contracts/shopcore_orders_daily.contract.yaml",
    "v2": "contracts/shopcore_orders_daily.v2.contract.yaml",
    "v3": "contracts/shopcore_orders_daily.v3.contract.yaml",
}

EXPECTED: dict[str, list[str]] = {}   # era -> tên cột, đúng thứ tự. Điền ở main().


def load_all_contracts() -> dict[str, list[str]]:
    """Nạp cả ba bản một lần lúc khởi động, không nạp lại 69 lần.

    load_typed() chứ không phải yaml.safe_load(): nếu một amendment có khoá gõ
    sai hoặc kiểu sai, ta muốn nó chết NGAY ĐÂY kèm đường dẫn tới đúng trường,
    chứ không để gate chạy sai lặng lẽ trên cả 69 file. Bài học của A06.
    """
    return {era: [col.name for col in load_typed(REPO_ROOT / rel).columns]
            for era, rel in CONTRACTS.items()}


def is_unreadable(sniffed_cols: list[str]) -> bool:
    """Phân biệt 'không đọc nổi' với 'schema đã drift'.

    sniff_csv KHÔNG ném lỗi trên file hỏng — nó thoái hoá: các dòng gãy làm
    hỏng việc dò dialect, sniffer chốt một delimiter thậm chí không có trong
    data, rồi trả về ĐÚNG MỘT cột mà TÊN của nó là cả dòng header 13 cột.
    Gate cũ sẽ báo "thiếu toàn bộ cột + 1 cột lạ khổng lồ" — một phán quyết
    schema-drift cho một file thực ra chỉ là không đọc được.
    """
    return len(sniffed_cols) == 1 and ("," in sniffed_cols[0] or '"' in sniffed_cols[0])


def era_by_header(cols: list[str]) -> str | None:
    """Era suy ra từ CHÍNH HEADER, không nhìn tên file.

    Tồn tại để bắt một thứ mà ngày tháng không bao giờ bắt được: file bị đặt
    sai tên. Một file v3 mang tên ngày tháng 7 sẽ được đọc bằng V2_COLS —
    mapping theo vị trí, không lỗi, và 'USD' nằm trong cột items.
    """
    for era, expected in EXPECTED.items():
        if cols == expected:
            return era
    return None


def check_one(con, path: Path, day: str) -> tuple[str, str]:
    """Trả (verdict, detail). verdict ∈ {'green','drift','unreadable'}."""
    era_date = era_of(day)

    # ---- 1. đọc được không? ------------------------------------------------
    try:
        sniffed = con.execute("SELECT Columns FROM sniff_csv(?)",
                              [str(path)]).fetchone()[0]
        cols = [c["name"] for c in sniffed]
    except Exception as e:
        return "unreadable", str(e).splitlines()[0][:120]

    if is_unreadable(cols):
        # Báo vào quality.structural (sự cố phía producer), KHÔNG vào khối schema.
        return "unreadable", "sniffed schema collapsed to 1 mega-column"

    # ---- 2. ngày và header có đồng ý với nhau không? -----------------------
    era_hdr = era_by_header(cols)
    if era_hdr is None:
        return "drift", (f"header khớp không era nào đã biết; ngày nói {era_date}. "
                         f"Bản export mới? change_management.notice_breaking: "
                         f"đáng lẽ phải báo trước 30 ngày.")
    if era_hdr != era_date:
        # Cảnh báo, không phải lỗi: file ĐỌC ĐƯỢC, chỉ là tên nói một đằng nội
        # dung một nẻo. Tin HEADER, vì đó mới là thứ quyết định cách đọc file.
        print(f"  [warn] {path.name}: ngày nói {era_date} nhưng header nói "
              f"{era_hdr} - file đặt sai tên? Đọc theo {era_hdr}.")

    # ---- 3. schema: tên chính xác, THỨ TỰ chính xác ------------------------
    expected = EXPECTED[era_hdr]
    if cols != expected:
        missing = [c for c in expected if c not in cols]
        extra = [c for c in cols if c not in expected]
        if missing or extra:
            return "drift", f"missing={missing} unexpected={extra}"
        return "drift", f"thứ tự cột đổi: contract {expected}, file {cols}"

    return "green", f"{era_hdr}  {CONTRACTS[era_hdr].split('/')[-1]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="small")
    ap.add_argument("-v", "--verbose", action="store_true", help="in cả dòng green")
    args = ap.parse_args()

    con, ROOT, RAW, MANIFEST = boot(args.scale)

    global EXPECTED
    EXPECTED = load_all_contracts()

    section("Task 8 — gate 69 file")
    for era, cols in EXPECTED.items():
        print(f"  {era}: {len(cols):2d} cols   ...{cols[-3:]}")
    print()

    tally = {"green": 0, "drift": 0, "unreadable": 0}
    for f in sorted(RAW.glob("orders_*.csv")):
        day = f.stem[-10:]
        verdict, detail = check_one(con, f, day)
        tally[verdict] += 1
        if verdict != "green" or args.verbose:
            print(f"{day}  {verdict.upper():10} {detail}")

    print(f"\n{tally}")
    print(f"\nExpect: 68 green; orders_{BAD_DAY}.csv fail dạng UNREADABLE, KHÔNG")
    print("phải dạng contract violation. Ghi rõ phân biệt đó vào journal cho A11.")
    con.close()


if __name__ == "__main__":
    main()
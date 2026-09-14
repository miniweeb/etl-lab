"""A10 Task 8 — gate A06 biết chọn contract theo era, và biết phân biệt
"file drift" với "file không đọc nổi".

  python -m work.a10_schema_evolution.a10_gate_eras --scale small

Gate cũ mô tả v1 nên giờ nó (đúng đắn) từ chối 2/3 feed. Việc cần làm là
TIẾN HOÁ CONTRACT, không phải nới code.
"""
from __future__ import annotations

import argparse

from work.a10_schema_evolution.a10_bootstrap import boot, section, BAD_DAY
from work.a10_schema_evolution.a10_canonical import era_of

# Ba bản của CÙNG một contract, mỗi bản có effective_from riêng.
# TODO(you) — viết hai amendment vào contracts/ theo đúng cách producer đáng lẽ
# phải làm, version-controlled:
#   2.0.0  era v2: ba cột thêm + nullability, VÀ luật reconciliation MỚI
#          order_total = items_total - discount_amount.
#          Thêm cột thôi thì chỉ minor; VIẾT LẠI NGHĨA của một business rule
#          đang có mới là thứ phá mọi consumer -> MAJOR.
#          Gộp luôn điều khoản locale của bản v1.4.0 hồi A06 (nó kẹt ở trạng
#          thái proposed, shopcore chưa bao giờ ký) để nó đi cùng chuyến này.
#   3.0.0  era v3: rename, đổi type id, loyalty_tier, items.disc, đổi thứ tự cột.
CONTRACTS = {
    "v1": "contracts/shopcore_orders_daily.contract.yaml",         # 1.3.0
    "v2": "contracts/shopcore_orders_daily.v2.contract.yaml",      # 2.0.0  TODO(you)
    "v3": "contracts/shopcore_orders_daily.v3.contract.yaml",      # 3.0.0  TODO(you)
}


def is_unreadable(sniffed_cols: list[str]) -> bool:
    """Phân biệt 'không đọc nổi' với 'schema đã drift'.

    sniff_csv KHÔNG ném lỗi trên file hỏng — nó thoái hoá: các dòng gãy làm
    hỏng việc dò dialect, sniffer chốt một delimiter thậm chí không có trong
    data, rồi trả về ĐÚNG MỘT cột mà TÊN của nó là cả dòng header 13 cột.
    Gate cũ sẽ báo "thiếu toàn bộ cột + 1 cột lạ khổng lồ" — một phán quyết
    schema-drift cho một file thực ra chỉ là không đọc được.
    """
    return len(sniffed_cols) == 1 and ("," in sniffed_cols[0] or '"' in sniffed_cols[0])


def check_one(con, path, day: str) -> tuple[str, str]:
    """Trả (verdict, detail). verdict ∈ {'green','drift','unreadable'}."""
    contract_path = CONTRACTS[era_of(day)]
    try:
        cols = [r[0] for r in con.execute(
            f"SELECT * FROM sniff_csv('{path.as_posix()}')").df()["Columns"][0]]
    except Exception as e:
        return "unreadable", str(e).splitlines()[0][:120]
    if is_unreadable(cols):
        # Báo vào quality.structural (producer incident), KHÔNG báo vào khối schema.
        return "unreadable", "sniffed schema collapsed to 1 mega-column"
    # TODO(you): gọi lại load_contract/check_file của A06 với contract_path,
    # và thêm một cảnh báo nếu NGÀY và HEADER bất đồng ý kiến về era.
    return "green", contract_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="small")
    args = ap.parse_args()
    con, ROOT, RAW, MANIFEST = boot(args.scale)

    section("Task 8 — gate 69 file")
    tally = {"green": 0, "drift": 0, "unreadable": 0}
    for f in sorted(RAW.glob("orders_*.csv")):
        day = f.stem[-10:]
        verdict, detail = check_one(con, f, day)
        tally[verdict] += 1
        if verdict != "green":
            print(f"{day}  {verdict.upper():10} {detail}")
    print(tally)
    # Expect: 68 green; orders_2026-07-22.csv fail dạng UNREADABLE,
    # KHÔNG phải dạng contract violation. Ghi rõ phân biệt đó vào journal cho A11.
    con.close()


if __name__ == "__main__":
    main()
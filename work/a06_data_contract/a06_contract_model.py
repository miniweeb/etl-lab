"""Typed model của contract shopcore orders.

Nhiệm vụ: biến YAML đã ký thành các object Python có hình dạng được kiểm NGAY
LÚC ĐỌC, để một khoá gõ sai hoặc một kiểu sai hỏng NGAY TẠI ĐÂY, kèm đường dẫn
tới đúng trường — thay vì âm thầm TẮT một luật ba check sau đó.

Giả định: những block mà gate THỰC SỰ đọc thì mô hình hoá từng trường; block
không ai đọc thì khai dưới dạng dict mờ. Chúng vẫn PHẢI được khai — đó chính là
thứ giữ cho extra='forbid' ở tầng trên cùng có ý nghĩa.
"""
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

REPO_ROOT = Path(__file__).resolve().parents[2]          # work/ -> gốc repo
CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"

# Tolerance được viết theo cách con người đọc ('0.5%'). Một số trần 0.5 thì MƠ HỒ
# — nửa phần trăm hay năm mươi? — nên ta ghim cả HÌNH DẠNG, không chỉ kiểu.
Percent = Annotated[str, StringConstraints(pattern=r"^\d+(\.\d+)?%$")]


class Clause(BaseModel):
    """Nền cho mọi block được mô hình hoá: khoá lạ là LỖI, không phải cái nhún vai."""
    model_config = ConfigDict(extra="forbid")


class Money(Clause):
    currency: str
    scale: int
    rounding: str
    negative_allowed: bool
    transport_locale: dict[str, str] | None = None
    #   Sắc bén nhất trong cả model. Trong một dict thô, contract ghi
    #   negative_allowed: "false" sẽ trả về CHUỖI "false" — mà chuỗi rỗng-khác
    #   là TRUTHY trong Python: `if money["negative_allowed"]:` đọc thành "cho
    #   phép âm" và luật tiền LẬT NGƯỢC. Model trả bool thật, hoặc từ chối.


class Column(Clause):
    name: str
    type: str
    nullable: bool                  # BẮT BUỘC: 'nullible:' không thể lọt qua đây
    allowed_values: list[str] | None = None
    case: str | None = None
    money: Money | None = None
    references: str | None = None
    range: str | None = None
    shape: str | None = None
    timezone: str | None = None
    unique: str | None = None
    warehouse_policy: str | None = None
    business_rule: str | None = None
    comment: str | None = None
    semantics: str | None = None


class Tolerances(Clause):
    """Cả sáu ngưỡng, theo tên.

    Thiếu một cái thì phép tra của gate sẽ KeyError giữa chừng sweep; gõ sai một
    cái thì luật nó canh giữ trở nên không thể cưỡng chế — trong im lặng.
    """
    null_or_invalid_status: Percent
    unparseable_order_ts: Percent
    unparseable_order_total: Percent
    orphan_customer_id: Percent
    orphan_sku_lines: Percent
    total_vs_items_mismatch: Percent


class Quality(Clause):
    row_level_tolerances: Tolerances
    structural: dict[str, str]      # khai, không mô hình hoá — gate bỏ qua
    reconciliation: list[str]
    duplicates: dict[str, Any]


class Delivery(Clause):
    path_pattern: str
    method: str
    business_date: str
    format: str
    schedule: str
    freshness_sla: str
    completeness_signal: str
    volume_expectation: str


class LateCorrections(Clause):
    window_days: int                # có SỐ HỌC chạy trên nó — phải là int
    semantics: str


class Contract(Clause):
    contract: str
    version: str
    status: Literal["draft", "proposed", "agreed", "deprecated"]
    effective_from: date
    supersedes: str | None = None
    delivery: Delivery
    late_corrections: LateCorrections
    columns: list[Column] = Field(alias="schema")
    #   BaseModel.schema đã bị pydantic chiếm, nên trường tên là `columns` với
    #   alias="schema". Validation vẫn đọc khoá YAML `schema`, và — tiện lợi —
    #   đường dẫn lỗi vẫn NÓI `schema`.
    quality: Quality
    # Khai, không mô hình hoá: gate không đọc gì trong đây. Vẫn phải khai, vì với
    # extra="forbid" ở tầng trên cùng, một block chưa khai là LỖI — và một tên
    # block gõ sai (chnage_management:) NÊN là lỗi.
    producer: dict[str, Any]
    consumer: dict[str, Any]
    change_management: dict[str, Any]
    business_rules: dict[str, Any]
    slo_reporting: dict[str, Any]

    def column(self, name: str) -> Column:
        """Tra một cột theo tên trong contract (ném lỗi nếu contract không có)."""
        return next(c for c in self.columns if c.name == name)


def load_typed(path: Path = CONTRACT) -> Contract:
    """Parse + validate contract.

    Ném pydantic.ValidationError với BẤT KỲ khoá lạ, khoá thiếu hoặc kiểu sai
    nào, và gọi tên chính xác trường đó.
    """
    return Contract.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    c = load_typed()
    print(f"{c.contract} v{c.version} ({c.status}, effective {c.effective_from})")
    print(f"  {len(c.columns)} columns, late window {c.late_corrections.window_days} days")
    print(f"  order_total negative_allowed = {c.column('order_total').money.negative_allowed!r}"
          f" ({type(c.column('order_total').money.negative_allowed).__name__})")
    print(f"  status tolerance = {c.quality.row_level_tolerances.null_or_invalid_status}")
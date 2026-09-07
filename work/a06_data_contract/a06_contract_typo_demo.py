"""Chứng minh model xứng đáng tồn tại: làm hỏng một BẢN SAO của contract, xem nó chết.

KHÔNG BAO GIỜ đụng tới contracts/ — file đã ký giữ nguyên từng byte. Đó là toàn
bộ đạo lý của assignment này: bản hỏng nằm ở scratch space, giống các CSV bị
phá của Task 7.
"""
import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from work.a06_data_contract.a06_contract_model import CONTRACT, load_typed
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
SCRATCH = DATA_ROOT / "tmp" / "contract_typo"
SCRATCH.mkdir(parents=True, exist_ok=True)
broken = SCRATCH / CONTRACT.name

# --- 1. contract thật, đã ký: phải parse sạch --------------------------------
# Model từ chối tài liệu THẬT là bug của MODEL, không phải của contract.
c = load_typed()
print(f"[typed] {c.contract} v{c.version} - {len(c.columns)} columns OK")

# --- 2. bản sao mang hai lỗi THỰC SỰ HAY XẢY RA ------------------------------
text = CONTRACT.read_text(encoding="utf-8")
text = text.replace("nullable: true              # guest checkout",
                    "nullible: true              # guest checkout")   # khoá gõ sai
text = text.replace("  window_days: 7", "  window_days: seven")       # kiểu sai
broken.write_text(text, encoding="utf-8")

# --- 3. yaml.safe_load hoàn toàn hài lòng: ĐÓ CHÍNH LÀ vấn đề ----------------
raw = yaml.safe_load(broken.read_text(encoding="utf-8"))
print(f"[yaml ] loads fine: {len(raw['schema'])} columns, "
      f"customer_id.nullable = {raw['schema'][1].get('nullable', '<<GONE>>')}, "
      f"window_days = {raw['late_corrections']['window_days']!r}")
# Đọc dòng này HAI LẦN: safe_load nạp file hỏng KHÔNG MỘT TIẾNG ĐỘNG, và báo
# customer_id.nullable = <<GONE>>. Đó chính là thứ gate Task 4 của bạn đang chạy trên.

# --- 4. model thì không ------------------------------------------------------
try:
    load_typed(broken)
    print("[typed] no error - the model is not doing its job")
except ValidationError as e:
    print(f"[typed] {e}")
# Đọc các ĐƯỜNG DẪN LỖI. Không phải "contract có gì đó sai" — schema.1.nullible
# nói: CỘT THỨ HAI của block schema, KHOÁ nullible, không được phép. Đó là một
# ĐỊA CHỈ bạn trả lại được cho người soạn amendment.
# Một lỗi chính tả sinh ra HAI lỗi (trường biến mất + trường xuất hiện), và kiểu
# sai bị bắt trong CÙNG một lượt thay vì ở file thứ 30 của sweep.
# Amendment v1.4.0 — shopcore_orders_daily

**From:** 1.3.0 (agreed, effective 2026-06-01)
**Proposed by:** Data Engineering (consumer)
**Status:** proposed

---

## Clause 1 — Cách viết số tiền khi truyền (transport locale)

**Đặt ở:** `schema.order_total.money`

```yaml
  - name: order_total
    money:
      transport_locale:
        decimal_separator: "."
        thousands_separator: none
        currency_symbols: none
```

**Vấn đề:** Contract ghim `currency: USD`, `scale: 2`, `rounding: half-up` — nói
rõ tiền tệ và cách làm tròn. Nhưng không nói **viết con số ra sao**. Nên khi
shopcore gửi `93,32` (dấu phẩy châu Âu) hay `$79.98` (có ký hiệu), consumer không
đổ lỗi được cho ai: đó chưa từng là lời hứa bị phá.

**Số đo:** A01 đo `order_total` không cast được ~0.7%. A03 phải viết riêng một
cleaning rule cho nó, sửa 187 dòng dấu phẩy châu Âu trong một ngày small. Rác này
lặp lại ổn định qua mọi ngày và cả hai scale.

Rủi ro không nằm ở chỗ ép kiểu lỗi — cái đó ồn ào, dễ thấy. Nó nằm ở chỗ xoá nhầm
dấu phẩy: `1.234,56` thành `1.23456` là sai 100 lần mà không có lỗi cú pháp nào.

**Sau clause này:** `93,32` và `$79.98` thành violation, đếm được và báo được.
Consumer bỏ được phần đoán trong cleaning rule vì đã có một dạng viết duy nhất
được thoả thuận.

**Ai bị ảnh hưởng:** Shopcore phải xuất đúng một dạng. Consumer không đổi code —
rule hiện tại vẫn chạy, chỉ là từ nay có chỗ dựa để báo cáo.

---

## Clause 2 — `updated_at` đo cái gì

**Đặt ở:** `schema.updated_at`

```yaml
  - name: updated_at
    type: TIMESTAMP
    timezone: UTC
    semantics: event_time
    nullable: false
    comment: >
      Thời điểm bản ghi thay đổi tại OLTP nguồn, theo UTC.
      Không phải thời điểm chạy job xuất file.
```

**Vấn đề:** `updated_at` là một nửa của `latest_wins_key` — cột quyết định bản nào
của một đơn gửi lại sẽ thắng. Nhưng contract không nói nó ở timezone nào, cũng
không nói nó đo lúc đơn thay đổi hay lúc shopcore xuất file. Chính contract đánh
dấu đây là chỗ im lặng chịu lực.

**Hỏng thế nào nếu để trống:** Nếu nó là lúc xuất file thì cả triệu dòng trong một
file mang cùng giá trị, và cách chọn bản thắng của consumer sập hoàn toàn. Nếu
shopcore đổi server sang timezone khác thì bản cũ đè bản mới. Cả hai đều hỏng lặng
lẽ — gate hiện tại không bắt được cái nào, vì schema vẫn đúng y nguyên.

**Ai bị ảnh hưởng:** Shopcore xác nhận cách sinh giá trị hiện tại. Consumer bỏ
được phần đoán trong logic dedupe — A07 dựng loader trên một cam kết thay vì một
giả định.

---

## Rollout

**Version:** 1.3.0 → 1.4.0, MINOR. Hai clause này chỉ viết ra thành lời cái ý định
vốn đã có trong contract — `money` đã ghim USD và scale 2, `latest_wins_key` đã
dựa vào `updated_at`. Không code consumer nào vỡ, và không cột nào bị xoá, đổi tên
hay đổi kiểu. Nhưng nó nhiều hơn một PATCH: PATCH là sửa câu chữ, còn đây là thêm
ràng buộc mà gate cưỡng chế được.

**Thời hạn báo trước:** 7 ngày qua `#shopcore-data-changes`, theo
`change_management.notice_additive`.

**Trạng thái:** `proposed` cho tới khi shopcore ký đối ứng. Trước lúc đó consumer
không được dựa vào hai clause này — gate vẫn chỉ đếm và báo cáo, chưa được chặn.

---

## Paste-check

Cả hai đoạn yaml đã chạy qua `yaml.safe_load` thành công. Một clause có lỗi cú
pháp là clause không review được.
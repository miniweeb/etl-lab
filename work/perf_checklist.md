# Performance Checklist — A14

## 1. Trước khi đụng vào gì

- Lấy baseline có đo TỪNG BƯỚC (không chỉ tổng), ở scale nhỏ trước: badpipe small — slice 0,34s / validate 59,50s / extract 0,10s / mart 0,12s / TOTAL 60,07s.
- Viết sẵn phép kiểm bằng nhau (EXCEPT 2 chiều, trên cả 2 artifact) TRƯỚC khi sửa dòng code đầu tiên — không phải sau khi "thấy nhanh hơn rồi mới nghĩ tới kiểm".
- Xác nhận baseline ở full scale trước khi hứa số với ai: small không đo được ĐỘ LỚN của vấn đề, chỉ xếp hạng được nó (badpipe full TOTAL 2314,45s, validate chiếm 99,5%).

## 2. Nghi phạm quen mặt, theo đúng thứ tự đã gặp (thứ tự chi phí, không phải thứ tự bắt mắt)

1. **Vòng lặp Python thay cho SQL tập hợp** — validate 59,50s → 0,07s sau khi gộp thành 1 câu `WHERE`. Luôn là nghi phạm số 1, kiểm trước tất cả.
2. **Thiếu lọc trên cột partition** — `strftime(order_ts,...)` mở toàn bộ 237 file; `order_date = DATE '...'` chỉ mở 1/237. Chậm hơn tới 221× ở full.
3. **`SELECT *` kéo cột nặng qua mọi bước** — gọi tên đúng 6 cột hợp đồng làm file giảm 3,89MB → 1,38MB (2,8×), không chỉ nhanh hơn mà còn nhẹ hơn.
4. **DISTINCT/ORDER BY thừa** — chỉ xoá SAU khi đếm chứng minh vô dụng (59.706 = 59.706 qua DISTINCT ở small; 1.193.931 = 1.193.931 ở full). Không xoá theo niềm tin.
5. **Join không dùng cột nào từ bảng joined** — bỏ `LEFT JOIN customers` vô ích, mart giảm từ 0,13s xuống 0,04s (small).
6. **Row group size sai** — `ROW_GROUP_SIZE 5000` đánh thuế MỌI query sau này ~4× (939ms vs 3635ms ở full, quét-hết); nhóm 122880 + sắp xếp đúng cột lọc nóng làm lọc 1 cửa hàng nhanh 36× (827,8ms → 25,7ms).

## 3. Kiểm chứng bằng gì

- **`bench()`** — warmup 1 + median 3 + `fetchall()`. Không phát biểu số nào mà không qua đây.
- **`EXPLAIN ANALYZE`** — dòng `Scanning Files: 1/N` là bằng chứng partition pruning thật, không phải suy đoán. `Total Files Read: 237` (không lọc được) vs `1` (lọc được) là bằng chứng trực tiếp.
- **Profile JSON** (`PRAGMA enable_profiling='json'`) — khi plan quá lớn để đọc bằng mắt, đổ về JSON rồi sắp theo `operator_timing` để tìm top-N toán tử tốn nhất.
- **EXCEPT 2 chiều** — sau MỖI lần sửa, không phải 1 lần ở cuối. Tốc độ không kèm bằng chứng kết quả không đổi là tin đồn.

## 4. Khi nào dừng

"Đủ nhanh" là một con số nghiệp vụ (job phải xong trước giờ nào, chi phí máy chấp nhận được là bao nhiêu), không phải con số để tự hào cá nhân. Với A14: mục tiêu đề ra là ≥20×, đạt được 600× (small) và 5.510× (full) — vượt xa mục tiêu là dấu hiệu tốt, nhưng không có nghĩa là còn tối ưu tiếp mãi mãi có lợi; mỗi lần sửa thêm vẫn phải trả lời được câu "khoản tiết kiệm này đáng công không" (xem A13 #51 — quy hiệu năng ra tiền).

---

**8 con số tự đo dùng trong checklist này:** 59,50s→0,07s (fix 1) · 221× (strftime vs order_date, full) · 3,89MB→1,38MB (projection 6 cột) · 0,13s→0,04s (bỏ join) · 600× (speedup small) · 5.510× (speedup full) · ~4× (thuế row group nhỏ, mọi query) · 36× (lọc 1 cửa hàng, sort+row group lành mạnh).
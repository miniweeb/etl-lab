-- A15 Task 4 — mang theo phần kiểu dữ liệu phức tạp của A04: chuỗi tags ngăn
-- bằng dấu gạch đứng thành một LIST, JSON trong attrs thành các cột thật.
select
    sku, product_name, category, subcategory, unit_price,
    string_split(tags, '|')                                  as tags,
    cast(attrs as json)->>'brand'                            as brand,
    try_cast(cast(attrs as json)->>'weight_g' as double)     as weight_g,
    cast(attrs as json)->>'color'                            as color
from {{ ref('stg_shopcore__products') }}

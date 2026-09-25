-- TODO(you) — A15 Task 3.
-- Nghiệm thu: stg_shopcore__products.unit_price KHÔNG có NULL nào.
select
    sku,
    product_name,
    category,
    subcategory,
    cast(
        try_cast(nullif(
            case when unit_price like '%,%'
                 then replace(replace(unit_price, '.', ''), ',', '.')
                 else replace(trim(unit_price), '$', '')
            end,
        'N/A') as decimal(14,2))
    as decimal(10,2))                                        as unit_price,
    tags,
    attrs
from {{ source('shopcore', 'products') }}

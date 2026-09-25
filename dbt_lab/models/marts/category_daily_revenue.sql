-- TODO(you) — A15 Task 6.
--   from fct_order_items, LEFT JOIN dim_products on sku
--   grain    = (order_date, category, currency)
--   measures = count(distinct order_id), sum(qty) as units,
--              sum(qty * unit_price) as gross_revenue
--
-- LEFT JOIN là CỐ Ý: ~0,2% SKU mồ côi của A04 phải rơi vào một category NULL
-- NHÌN THẤY ĐƯỢC, chứ không được biến mất vào một inner join.
-- Nghiệm thu: 1.990 dòng, trong đó 143 dòng category NULL.
select
    i.order_date,
    p.category,
    i.currency,
    count(distinct i.order_id)     as orders,
    sum(i.qty)                     as units,
    sum(i.qty * i.unit_price)      as gross_revenue
from {{ ref('fct_order_items') }} i
left join {{ ref('dim_products') }} p on i.sku = p.sku
group by 1, 2, 3

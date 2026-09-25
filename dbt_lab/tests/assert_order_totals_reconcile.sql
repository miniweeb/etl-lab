-- A15 Task 7 — singular test: luật bắc qua nhiều trường, không cột đơn lẻ nào
-- phát biểu nổi.
with items_sum as (
    select order_id, sum(qty * unit_price) as items_total
    from {{ ref('fct_order_items') }}
    group by 1
),

checked as (
    select o.order_id, o.order_total, o.discount_amount, i.items_total
    from {{ ref('fct_orders') }} o
    join items_sum i using (order_id)
    where o.order_total is not null
)

select
    count(*) filter (
        where abs(order_total - (items_total - discount_amount)) > 0.01
    )                          as bad_rows,
    count(*)                   as checked_rows
from checked
having bad_rows > 0.01 * checked_rows

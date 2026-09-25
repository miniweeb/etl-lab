-- A15 Task 6 — bung mảng items Ở PHÍA SAU bảng fact đã khử trùng lặp, nên
-- không dòng hàng nào bị đếm hai lần.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_date'
) }}

with orders as (

    select order_id, order_id_num, order_date, store_id, customer_id,
           currency, items
    from {{ ref('fct_orders') }}
    {% if is_incremental() %}
    where order_date between date '{{ var("start_date") }}'
                         and date '{{ var("end_date") }}'
    {% endif %}

)

select
    order_id, order_id_num, order_date, store_id, customer_id, currency,
    generate_subscripts(items, 1)      as line_no,
    -- generate_subscripts phát ra 1, 2, 3... SONG SONG NHỊP với unnest, nhờ vậy
    -- mỗi dòng hàng có số thứ tự của mình. Hàm SQL mới duy nhất hôm nay.
    unnest(items, recursive := true)   -- -> sku, qty, unit_price, disc
from orders
where items is not null

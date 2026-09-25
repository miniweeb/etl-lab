-- A15 Task 5 — mỗi dòng một đơn hàng, bản sửa mới nhất thắng.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_date'
) }}

with batch as (

    select *, row_number() over () as _rid
    from {{ ref('stg_shopcore__orders') }}
    {% if is_incremental() %}
    where order_date between date '{{ var("start_date") }}'
                         and date '{{ var("end_date") }}'
    {% endif %}

),

ranked as (
    select _rid,
           row_number() over (partition by order_id_num
                              order by updated_at desc) as rn
    from batch
)

select
    order_id, order_id_num, customer_id, store_id,
    order_ts, updated_at, status, payment_method,
    order_total, currency, discount_amount, channel, loyalty_tier,
    items, meta, order_date
from batch
where _rid in (select _rid from ranked where rn = 1)
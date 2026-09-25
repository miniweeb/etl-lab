select
    order_id, order_id_num, customer_id, store_id,
    order_ts, updated_at,
    case when lower(trim(status)) in
              ('created','paid','shipped','delivered','cancelled','refunded')
         then lower(trim(status)) end                       as status,
    nullif(lower(trim(payment_method)), '')                 as payment_method,
    order_total, currency, discount_amount, channel, loyalty_tier,
    items, meta, order_date
from {{ source('shopcore', 'orders') }}

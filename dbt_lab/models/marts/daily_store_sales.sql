-- A15 Task 6. Một vết sẹo của A10 đã gấp vào trong: CURRENCY NẰM TRONG GRAIN,
-- vì cộng USD với VND thành một số là lời nói dối gấp 25.000 lần.
select
    o.order_date, o.store_id, o.currency,
    any_value(s.store_name)                          as store_name,
    any_value(s.region)                              as region,
    count(*)                                         as orders,
    count(distinct o.customer_id)                    as customers,
    sum(o.order_total)                               as gross_revenue,
    round(avg(o.order_total), 2)                     as avg_order_value,
    count(*) filter (where o.status = 'cancelled')   as cancelled_orders,
    count(*) filter (where o.order_total is null)    as orders_missing_total
from {{ ref('fct_orders') }} o
left join {{ ref('dim_stores') }} s using (store_id)
group by 1, 2, 3

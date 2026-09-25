-- TODO(you) — A15 Task 4. Một dòng, cùng khuôn với dim_customers.
select * from {{ ref('stg_shopcore__stores') }}

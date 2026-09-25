-- TODO(you) — A15 Task 3.
select
    cast(store_id as integer)      as store_id,
    store_name,
    region,
    country,
    cast(opened_date as date)      as opened_date
from {{ source('shopcore', 'stores') }}
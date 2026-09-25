-- A15 Task 3 — Task 6 
-- Seed được join qua ref(), nhờ vậy country_map lọt vào DAG.
with src as (
    select * from {{ source('shopcore', 'customers') }}
),

cleaned as (
    select
        cast(customer_id as bigint)                          as customer_id,
        case when name like '%,%'    -- "NGUYEN, Linh" -> "Linh Nguyen"
             then trim(split_part(name, ',', 2)) || ' ' ||
                  upper(substr(trim(split_part(name, ',', 1)), 1, 1)) ||
                  lower(substr(trim(split_part(name, ',', 1)), 2))
             else trim(name) end                             as name,
        md5(lower(trim(email)))                              as email_hash,
        lower(trim(country))                                 as country_variant,
        city,
        case when nullif(trim(signup_ts), '') is null then null
             when regexp_full_match(signup_ts, '[0-9]+')     -- epoch giây
                 then cast(to_timestamp(cast(signup_ts as bigint)) as timestamp)
             else try_strptime(signup_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y'])
        end                                                  as signup_ts,
        case when lower(trim(is_active)) in ('true','1','yes','y','t') then true
             when lower(trim(is_active)) in ('false','0','no','n','f') then false
        end                                                  as is_active
    from src
)

select
    c.customer_id, c.name, c.email_hash,
    m.iso2 as country,
    c.city, c.signup_ts, c.is_active
from cleaned c
left join {{ ref('country_map') }} m on c.country_variant = m.variant

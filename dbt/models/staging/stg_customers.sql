-- Staging: trim and cast the raw customers seed.
select
    customer_id,
    trim(full_name)            as full_name,
    trim(city)                 as city,
    cast(signup_date as date)  as signup_date
from {{ ref('raw_customers') }}

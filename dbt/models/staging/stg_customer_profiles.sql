-- The customer profile feed "as of" the current batch (see var `batch`).
select
    customer_id,
    trim(full_name)               as full_name,
    trim(city)                    as city,
    lower(trim(tier))             as tier,
    cast(updated_ts as timestamp) as updated_at
from {{ ref('raw_customer_profiles') }}
where batch = {{ var('batch') | int }}

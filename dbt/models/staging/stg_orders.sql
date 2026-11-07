-- Staging: rename, trim, cast and standardise the raw orders seed.
with source as (
    select * from {{ ref('raw_orders') }}
)

select
    cast(order_id as integer)                 as order_id,
    customer_id,
    cast(order_ts as timestamp)               as ordered_at,
    cast(cast(order_ts as timestamp) as date) as order_date,
    lower(trim(status))                       as order_status,
    cast(amount_aed as decimal(12, 2))        as amount_aed,
    lower(trim(payment_method))               as payment_method
from source

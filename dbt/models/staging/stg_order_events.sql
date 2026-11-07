-- All order rows the warehouse has "received" so far.
-- batch 1: the original orders (updated_at = ordered_at)
-- batch 2: also the late-arriving updates (a status change + new orders)
select
    order_id, customer_id, ordered_at, order_date, order_status, amount_aed, payment_method,
    ordered_at as updated_at
from {{ ref('stg_orders') }}
{% if var('batch') | int >= 2 %}
-- drop the old version of any order that has an update
where order_id not in (select cast(order_id as integer) from {{ ref('raw_order_updates') }})
union all
select
    cast(order_id as integer),
    customer_id,
    cast(order_ts as timestamp),
    cast(cast(order_ts as timestamp) as date),
    lower(trim(status)),
    cast(amount_aed as decimal(12, 2)),
    lower(trim(payment_method)),
    cast(updated_ts as timestamp)
from {{ ref('raw_order_updates') }}
{% endif %}

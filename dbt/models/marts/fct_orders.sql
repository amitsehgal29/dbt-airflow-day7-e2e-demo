{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

-- Incremental fact table: the first run loads everything; later runs only
-- process rows newer than what is already in the table, and replace any
-- order_id that changed (delete+insert on unique_key).
select
    order_id, customer_id, ordered_at, order_date, order_status,
    amount_aed, payment_method, updated_at,
    current_timestamp as loaded_at
from {{ ref('stg_order_events') }}
{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), timestamp '1900-01-01') from {{ this }})
{% endif %}

-- Singular test: returns the rows that break the amount rules, so it passes when empty.
-- Returned orders must be negative, cancelled must be zero, everything else positive.
select order_id, order_status, amount_aed
from {{ ref('stg_orders') }}
where (order_status = 'returned'  and amount_aed >= 0)
   or (order_status = 'cancelled' and amount_aed <> 0)
   or (order_status not in ('returned', 'cancelled') and amount_aed <= 0)

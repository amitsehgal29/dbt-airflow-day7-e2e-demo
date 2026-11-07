{% docs order_status %}
Lifecycle status of an order, lowercased and trimmed in staging.

| status    | meaning                                   |
|-----------|-------------------------------------------|
| pending   | placed, not yet paid or confirmed         |
| shipped   | handed to the courier                     |
| completed | delivered and paid                        |
| cancelled | cancelled before shipping, amount is 0    |
| returned  | refunded after delivery, amount negative  |
{% enddocs %}

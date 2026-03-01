select
    transaction_type,
    count(*) as total_transactions,
    sum(case when is_fraud  then 1 else 0 end) as fraud_transactions,
    round(
        100.0 * sum(case when is_fraud then 1 else 0 end) / count(*),
        2
    ) as fraud_rate_percent
from {{ ref('stg_transactions') }}
group by transaction_type
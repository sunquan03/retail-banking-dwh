select 
    sender_id as account_id,
    count(*) as total_transactions,
    sum(case when is_fraud then 1 else 0 end) as fraud_transactions,
    round(
        100.0 * sum(case when is_fraud then 1 else 0 end) / count(*),
        2
    ) as fraud_rate_percent
from {{ ref('stg_transactions') }}
group by sender_id
union all 
select 
    receiver_id as account_id,
    count(*) as total_transactions,
    sum(case when is_fraud then 1 else 0 end) as fraud_transactions,
    round(
        100.0 * sum(case when is_fraud then 1 else 0 end) / count(*),
        2
    ) as fraud_rate_percent
from {{ ref('stg_transactions') }}
group by receiver_id

with t as (
  select
    ft.transaction_id,
    ft.ingested_at,
    tt.transaction_type,
    ft.amount,
    ft.is_fraud,
    ft.sender_balance_before,
    ft.sender_balance_after,
    (ft.sender_balance_after - ft.sender_balance_before) as sender_balance_delta
  from analytics_mart.fct_transactions ft 
  left join analytics_mart.dim_transaction_type tt on ft.transaction_type_id = tt.transaction_type_id
),

flags as (
  select
    *,
    case when sender_balance_after < 0 then 1 else 0 end as f_negative_after,
    case when sender_balance_before = 0 and amount > 0 then 1 else 0 end as f_zero_before_nonzero_amount,
    case when amount > 0 and sender_balance_delta = 0 then 1 else 0 end as f_nonzero_amount_zero_delta,
    case when abs(sender_balance_delta) > 0 and abs(sender_balance_delta - (-amount)) > 0.01 then 1 else 0 end as f_delta_not_matching_amount
  from t
)

select
  transaction_type,
  count(*) as total,
  sum(f_negative_after) as negative_after_cnt,
  sum(f_zero_before_nonzero_amount) as zero_before_amount_cnt,
  sum(f_nonzero_amount_zero_delta) as nonzero_amount_zero_delta_cnt,
  sum(f_delta_not_matching_amount) as delta_mismatch_cnt,
  sum(case when is_fraud then 1 else 0 end) as fraud_cnt
from flags
group by transaction_type;
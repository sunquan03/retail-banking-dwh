SELECT
    transaction_id,
    step,
    type AS transaction_type,
    amount,
    nameorig AS sender_id,
    oldbalanceorg AS sender_balance_before,
    newbalanceorig AS sender_balance_after,
    namedest AS receiver_id,
    oldbalancedest AS receiver_balance_before,
    newbalancedest AS receiver_balance_after,
    isfraud AS is_fraud,
    isflaggedfraud AS is_flagged_fraud,
    ingested_at
FROM raw.transactions
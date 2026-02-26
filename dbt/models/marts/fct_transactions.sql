SELECT
    t.transaction_id,
    t.step                          AS step_id,          -- FK → dim_date
    tt.transaction_type_id,                              -- FK → dim_transaction_type
    s.account_id_sk                 AS sender_sk,        -- FK → dim_account
    r.account_id_sk                 AS receiver_sk,      -- FK → dim_account
    t.amount,
    t.sender_balance_before,
    t.sender_balance_after,
    t.sender_balance_after - t.sender_balance_before    AS sender_balance_delta,
    t.receiver_balance_before,
    t.receiver_balance_after,
    t.receiver_balance_after - t.receiver_balance_before AS receiver_balance_delta,
    t.is_fraud,
    t.is_flagged_fraud,
    t.ingested_at
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_transaction_type') }} tt  ON t.transaction_type  = tt.transaction_type
LEFT JOIN {{ ref('dim_account') }}          s   ON t.sender_id         = s.account_id
LEFT JOIN {{ ref('dim_account') }}          r   ON t.receiver_id       = r.account_id
SELECT
    ROW_NUMBER() OVER (ORDER BY transaction_type)   AS transaction_type_id,
    transaction_type,
    CASE transaction_type
        WHEN 'TRANSFER'  THEN 'Movement of funds between accounts'
        WHEN 'CASH_OUT'  THEN 'Withdrawal to cash'
        WHEN 'CASH_IN'   THEN 'Deposit from cash'
        WHEN 'PAYMENT'   THEN 'Payment to merchant'
        WHEN 'DEBIT'     THEN 'Direct debit'
        ELSE 'Unknown'
    END AS description
FROM (
    SELECT DISTINCT transaction_type
    FROM {{ ref('stg_transactions') }}
) t
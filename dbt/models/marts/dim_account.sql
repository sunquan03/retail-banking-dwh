WITH all_accounts AS (
    SELECT DISTINCT sender_id AS account_id FROM {{ ref('stg_transactions') }}
    UNION
    SELECT DISTINCT receiver_id FROM {{ ref('stg_transactions') }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY account_id) AS account_id_sk,
    account_id,
    LEFT(account_id, 1) AS account_type
FROM all_accounts
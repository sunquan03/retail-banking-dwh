CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    step INT,
    type TEXT,
    amount NUMERIC(18,2),
    nameOrig TEXT,
    oldbalanceOrg NUMERIC(18,2),
    newbalanceOrig NUMERIC(18,2),
    nameDest TEXT,
    oldbalanceDest NUMERIC(18,2),
    newbalanceDest NUMERIC(18,2),
    isFraud BOOLEAN,
    isFlaggedFraud BOOLEAN,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
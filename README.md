# Banking Fraud Transactions Data Warehouse

Stack: **PostgreSQL +
dbt + Python**.  

------------------------------------------------------------------------

This repository demonstrates how to design and implement a fraud
analytics warehouse using:

- Layered data architecture
- Star schema modeling
- Feature engineering in SQL
- Fraud risk segmentation marts
- Statistical hypothesis testing in Python

The project focuses on validating whether balance-based features are
statistically significant predictors of fraud.

------------------------------------------------------------------------

## Architecture

Raw Layer (raw.transactions)  
  
Staging Layer (stg_transactions)  
  
Dimensions: - dim_account - dim_transaction_type  
  
Fact Table: - fct_transactions  
  
Data Marts: - mart_fraud_by_type - mart_fraud_balance_bands -
mart_sender_risk_profile - mart_fraud_anomalous_balance_transitions  
  
Analytics (Python notebooks)

------------------------------------------------------------------------

## Tech Stack

- PostgreSQL
- dbt
- Python
- Airflow

------------------------------------------------------------------------

## Data Model

### Fact Table: `fct_transactions`

------------------------------------------------------------------------

## Data Marts

### 1. Fraud Rate by Transaction Type

`mart_fraud_by_type`

Aggregates fraud rate by transaction category.

------------------------------------------------------------------------

### 2. Fraud Rate by Balance Bands

`mart_fraud_balance_bands`

Segments fraud by: - Sender balance before transaction - Sender balance
delta buckets

Purpose: Identify high-risk balance regimes.

------------------------------------------------------------------------

### 3. Sender Risk Profile

`mart_sender_risk_profile`

Aggregated per sender: - Transaction count - Historical fraud rate -
Average amount - Median absolute balance delta

Purpose: Simulates customer-level risk scoring.

------------------------------------------------------------------------

### 4. Balance Transition Anomalies

`mart_fraud_anomalous_balance_transitions`

Shows suspicious patterns: - Negative balances - Zero balance before
non-zero transfer - Inconsistent delta vs amount - Abnormal balance
transitions

------------------------------------------------------------------------

## Statistical Validation

Formal hypothesis testing validated balance features.

### Features Tested

- sender_balance_before
- sender_balance_after
- sender_balance_delta

### Tests Applied

- Mann–Whitney U test (non-parametric)
- Welch’s t-test (unequal variance)
- Benjamini–Hochberg correction (multiple testing)


Sender balance features — particularly balance deltas — are strong
discriminative predictors for fraud detection models.

------------------------------------------------------------------------

## Initial dataset

https://www.kaggle.com/datasets/ealaxi/paysim1
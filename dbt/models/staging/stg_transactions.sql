with src as (
    select *
    from raw.transactions
)
select
    step,
    type,
    amount,
    nameorig,
    oldbalanceorg,
    newbalanceorig,
    namedest,
    oldbalancedest,
    newbalancedest,
    isfraud,
    isflaggedfraud
from src
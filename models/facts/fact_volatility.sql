{{ config(
    materialized='table'
) }}

-- Fact table: volatility and risk metrics per symbol and date
select
  symbol,
  date,
  daily_return,
  volatility,
  beta
from {{ ref('int_test_volatility') }}
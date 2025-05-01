{{ config(
    materialized='view'
) }}

-- Intermediate model: daily volatility and risk metrics per symbol
select
  symbol,
  date,
  daily_return,
  volatility,
  beta
from {{ ref('stg_test_volatility') }}
{{ config(
    materialized='view'
) }}

-- Staging model: test volatility and risk metrics
select
  symbol,
  date,
  daily_return,
  volatility,
  beta
from `{{ target.project }}.{{ target.dataset }}.test_volatility`

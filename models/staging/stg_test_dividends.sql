{{ config(
    materialized='view'
) }}

-- Staging model: synthetic dividend data
select
  symbol,
  date,
  dividend_amount,
  dividend_yield
from `{{ target.project }}.{{ target.dataset }}.test_dividends`
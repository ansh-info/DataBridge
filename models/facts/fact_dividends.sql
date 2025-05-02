{{ config(
    materialized='table'
) }}

-- Fact table: dividend payouts per symbol and date
select
  symbol,
  date,
  dividend_amount,
  dividend_yield
from {{ ref('int_test_dividends') }}
{{ config(
    materialized='view'
) }}

-- Intermediate model: filter only dividend occurrences
select
  symbol,
  date,
  dividend_amount,
  dividend_yield
from {{ ref('stg_test_dividends') }}
where dividend_amount > 0
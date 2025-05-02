{{ config(
    materialized='table'
) }}

-- Fact table: earnings events per symbol and date
select
  symbol,
  date,
  est_eps,
  actual_eps,
  surprise_pct
from {{ ref('int_test_earnings') }}
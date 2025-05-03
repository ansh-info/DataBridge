{{ config(
    materialized='view'
) }}

-- Staging model: test earnings event data
select
  symbol,
  date,
  est_eps,
  actual_eps,
  surprise_pct
from `{{ target.project }}.{{ target.dataset }}.test_earnings`

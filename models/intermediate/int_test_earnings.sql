{{ config(
    materialized='view'
) }}

-- Intermediate model: filter only actual earnings events
select
  symbol,
  date,
  est_eps,
  actual_eps,
  surprise_pct
from {{ ref('stg_test_earnings') }}
where actual_eps is not null
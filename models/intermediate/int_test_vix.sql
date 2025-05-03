{{ config(
    materialized='view'
) }}

-- Intermediate model: test VIX index data per date
select
  date,
  implied_volatility
from {{ ref('stg_test_vix') }}

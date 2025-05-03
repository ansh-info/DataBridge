{{ config(
    materialized='table'
) }}

-- Fact table: test VIX implied volatility index per date
select
  date,
  implied_volatility
from {{ ref('int_test_vix') }}

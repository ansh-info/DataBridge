{{ config(
    materialized='table'
) }}

-- Fact table: synthetic VIX implied volatility index per date
select
  date,
  implied_volatility
from {{ ref('int_test_vix') }}
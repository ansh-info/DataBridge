{{ config(
    materialized='table'
) }}

-- Fact table: synthetic FX rates per currency pair and date
select
  currency_pair,
  date,
  rate
from {{ ref('int_test_fx_rates') }}
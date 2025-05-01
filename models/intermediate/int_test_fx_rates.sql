{{ config(
    materialized='view'
) }}

-- Intermediate model: synthetic FX rates per currency pair
select
  currency_pair,
  date,
  rate
from {{ ref('stg_test_fx_rates') }}
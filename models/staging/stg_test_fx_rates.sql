{{ config(
    materialized='view'
) }}

-- Staging model: synthetic FX rates data
select
  currency_pair,
  date,
  rate
from `{{ target.project }}.{{ target.dataset }}.test_fx_rates`
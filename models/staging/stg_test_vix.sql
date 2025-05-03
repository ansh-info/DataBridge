{{ config(
    materialized='view'
) }}

-- Staging model: test VIX implied volatility index data
select
  date,
  implied_volatility
from `{{ target.project }}.{{ target.dataset }}.test_vix`

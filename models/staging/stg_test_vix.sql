{{ config(
    materialized='view'
) }}

-- Staging model: synthetic VIX implied volatility index data
select
  date,
  implied_volatility
from `{{ target.project }}.{{ target.dataset }}.test_vix`
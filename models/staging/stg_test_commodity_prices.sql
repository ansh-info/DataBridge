{{ config(
    materialized='view'
) }}

-- Staging model: test commodity price data
select
  commodity,
  date,
  price
from `{{ target.project }}.{{ target.dataset }}.test_commodity_prices`

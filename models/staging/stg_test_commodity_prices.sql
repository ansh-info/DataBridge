{{ config(
    materialized='view'
) }}

-- Staging model: synthetic commodity price data
select
  commodity,
  date,
  price
from `{{ target.project }}.{{ target.dataset }}.test_commodity_prices`
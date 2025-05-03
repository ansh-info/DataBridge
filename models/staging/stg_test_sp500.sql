{{ config(
    materialized='view'
) }}

-- Staging model: test S&P 500 index OHLCV data
select
  date,
  open,
  high,
  low,
  close,
  volume
from `{{ target.project }}.{{ target.dataset }}.test_sp500`

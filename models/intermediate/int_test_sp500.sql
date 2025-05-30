{{ config(
    materialized='view'
) }}

-- Intermediate model: test S&P 500 index daily OHLCV
select
  date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_test_sp500') }}

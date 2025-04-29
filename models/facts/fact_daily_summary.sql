{{ config(materialized='table') }}

-- Fact: daily OHLCV summary per stock symbol
select
  symbol,
  date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('int_daily_stock_summary') }}
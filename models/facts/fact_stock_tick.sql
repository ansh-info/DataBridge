{{ config(materialized='table') }}

-- Fact: intraday stock tick data (tested real-time stream)
select
  symbol,
  timestamp,
  date(timestamp) as date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_test_realtime_stock_data') }}

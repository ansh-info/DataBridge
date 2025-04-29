{{ config(materialized='table') }}

-- Fact: intraday stock tick data (mocked real-time stream)
select
  symbol,
  timestamp,
  date(timestamp) as date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_realtime_mock_stock_data') }}
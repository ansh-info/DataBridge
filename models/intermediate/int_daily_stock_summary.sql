{{ config(
    materialized='view'
) }}

-- Intermediate model: daily OHLCV summary for mock real-time stock data
with base as (
  select *
  from {{ ref('stg_realtime_mock_stock_data') }}
),
windowed as (
  select
    symbol,
    date,
    first_value(open) over (
      partition by symbol, date order by time
    ) as open,
    max(high) over (
      partition by symbol, date
    ) as high,
    min(low) over (
      partition by symbol, date
    ) as low,
    last_value(close) over (
      partition by symbol, date order by time
      rows between unbounded preceding and unbounded following
    ) as close,
    sum(volume) over (
      partition by symbol, date
    ) as volume,
    row_number() over (
      partition by symbol, date order by time
    ) as rn
  from base
)
select
  symbol,
  date,
  open,
  high,
  low,
  close,
  volume
from windowed
where rn = 1
order by symbol, date
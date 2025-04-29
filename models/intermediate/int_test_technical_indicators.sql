{{ config(
    materialized='view'
) }}

-- Intermediate aggregate: average technical indicators per symbol
select
  symbol,
  avg(ma_10) as avg_ma_10,
  avg(ma_50) as avg_ma_50,
  avg(rsi) as avg_rsi,
  avg(macd) as avg_macd,
  avg(bb_lower) as avg_bb_lower,
  avg(bb_upper) as avg_bb_upper
from {{ ref('stg_test_technical_indicators') }}
group by symbol
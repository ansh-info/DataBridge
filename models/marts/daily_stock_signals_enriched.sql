{{ config(
    materialized='table',
    enabled=false
) }}

-- Enriched mart model: combine daily stock summary with expanded static and streaming features
with
  stock as (
    select * from {{ ref('int_daily_stock_summary') }}
  ),
  sp500 as (
    select date, close as sp500_close
    from {{ ref('stg_sandp500_data') }}
  ),
  crypto as (
    select date(date) as date, avg(close) as crypto_avg_close
    from {{ ref('stg_crypto_price_history') }}
    group by date(date)
  ),
  econ_real as (
    select
      Year as year,
      Gross_Domestic_Product_GDP as gdp_per_capita
    from `{{ target.project }}.{{ target.dataset }}.global_economy_indicators`
    where Country = 'United States'
  ),
  fundamentals as (
    select symbol, date, eps, pe_ratio, market_cap, sector
    from {{ ref('stg_test_fundamentals') }}
  ),
  technical as (
    select symbol, date, ma_10, ma_50, rsi, macd, bb_lower, bb_upper
    from {{ ref('stg_test_technical_indicators') }}
  ),
  sentiment as (
    select symbol, date, sentiment_score, news_volume, twitter_mentions
    from {{ ref('stg_test_sentiment') }}
  ),
  econ_synth as (
    select date, inflation_rate, unemployment_rate, gdp_growth, interest_rate
    from {{ ref('stg_test_economic_indicators') }}
  )

select
  s.symbol,
  s.date,
  s.open,
  s.high,
  s.low,
  s.close,
  s.volume,
  sp.sp500_close,
  c.crypto_avg_close,
  er.gdp_per_capita,
  f.eps,
  f.pe_ratio,
  f.market_cap,
  f.sector,
  t.ma_10,
  t.ma_50,
  t.rsi,
  t.macd,
  t.bb_lower,
  t.bb_upper,
  se.sentiment_score,
  se.news_volume,
  se.twitter_mentions,
  es.inflation_rate,
  es.unemployment_rate,
  es.gdp_growth,
  es.interest_rate
from stock s
left join sp500 sp on s.date = sp.date
left join crypto c on s.date = c.date
left join econ_real er on extract(year from s.date) = er.year
left join fundamentals f on s.symbol = f.symbol and s.date = f.date
left join technical t on s.symbol = t.symbol and s.date = t.date
left join sentiment se on s.symbol = se.symbol and s.date = se.date
left join econ_synth es on s.date = es.date
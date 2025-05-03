{{ config(materialized='table') }}

-- Fact: daily static & test features per stock symbol
select
  f.symbol,
  f.date,
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
  s.sentiment_score,
  s.news_volume,
  s.twitter_mentions,
  e.inflation_rate,
  e.unemployment_rate,
  e.gdp_growth,
  e.interest_rate
from {{ ref('stg_test_fundamentals') }} f
left join {{ ref('stg_test_technical_indicators') }} t using (symbol, date)
left join {{ ref('stg_test_sentiment') }} s using (symbol, date)
left join {{ ref('stg_test_economic_indicators') }} e using (date)

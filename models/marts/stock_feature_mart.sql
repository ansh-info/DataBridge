{{ config(
    materialized='table'
) }}

-- Stock feature mart: one row per symbol, aggregated static and latest streaming features
with
  latest_stream as (
    select
      symbol,
      close as last_close,
      volume as last_volume
    from (
      select *,
        row_number() over (partition by symbol order by timestamp desc) as rn
    from {{ ref('stg_test_realtime_stock_data') }}
    )
    where rn = 1
  ),
  fund as (
    select *
    from {{ ref('int_test_fundamentals') }}
  ),
  tech as (
    select *
    from {{ ref('int_test_technical_indicators') }}
  ),
  sent as (
    select *
    from {{ ref('int_test_sentiment') }}
  ),
  econ as (
    select *
    from {{ ref('int_test_economic_indicators') }}
  )

select
  ls.symbol,
  ls.last_close,
  ls.last_volume,
  fund.avg_eps,
  fund.avg_pe_ratio,
  fund.avg_market_cap,
  tech.avg_ma_10,
  tech.avg_ma_50,
  tech.avg_rsi,
  tech.avg_macd,
  tech.avg_bb_lower,
  tech.avg_bb_upper,
  sent.avg_sentiment_score,
  sent.avg_news_volume,
  sent.avg_twitter_mentions,
  econ.inflation_rate,
  econ.unemployment_rate,
  econ.gdp_growth,
  econ.interest_rate
from latest_stream ls
left join fund on ls.symbol = fund.symbol
left join tech on ls.symbol = tech.symbol
left join sent on ls.symbol = sent.symbol
cross join econ

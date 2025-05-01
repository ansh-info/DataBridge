{{ config(
    materialized='table',
    enabled=false
) }}

-- Mart model: combine daily stock summary with market and macro indicators
with stock as (
  select *
  from {{ ref('int_daily_stock_summary') }}
),
sp500 as (
  select
    date,
    close as sp500_close
  from {{ ref('stg_sandp500_data') }}
),
crypto as (
  select
    date(date) as date,
    avg(close) as crypto_avg_close
  from {{ ref('stg_crypto_price_history') }}
  group by date(date)
), economy as (
  select
    Year as year,
    Gross_Domestic_Product_GDP as gdp_per_capita
  from `{{ target.project }}.{{ target.dataset }}.global_economy_indicators`
  where Country = 'United States'
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
  e.gdp_per_capita
from stock s
left join sp500 sp
  on s.date = sp.date
left join crypto c
  on s.date = c.date
left join economy e
  on extract(year from s.date) = e.year

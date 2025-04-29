{{ config(
    materialized='view'
) }}

-- Intermediate aggregate: average fundamentals per symbol
select
  symbol,
  avg(eps) as avg_eps,
  avg(pe_ratio) as avg_pe_ratio,
  avg(market_cap) as avg_market_cap
from {{ ref('stg_test_fundamentals') }}
group by symbol
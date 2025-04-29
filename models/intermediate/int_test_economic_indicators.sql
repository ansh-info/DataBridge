{{ config(
    materialized='view'
) }}

-- Intermediate: pick latest synthetic economic indicators snapshot
with numbered as (
  select *,
    row_number() over (order by date desc) as rn
  from {{ ref('stg_test_economic_indicators') }}
)
select
  inflation_rate,
  unemployment_rate,
  gdp_growth,
  interest_rate
from numbered
where rn = 1
{{ config(materialized='table') }}

-- Dimension: stocks master data
select
  symbol,
  any_value(sector) as sector
from {{ ref('stg_test_fundamentals') }}
group by symbol
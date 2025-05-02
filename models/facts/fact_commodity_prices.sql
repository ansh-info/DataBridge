{{ config(
    materialized='table'
) }}

-- Fact table: synthetic commodity prices per date
select
  commodity,
  date,
  price
from {{ ref('int_test_commodity_prices') }}
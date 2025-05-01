{{ config(
    materialized='view'
) }}

-- Intermediate model: synthetic commodity prices per date
select
  commodity,
  date,
  price
from {{ ref('stg_test_commodity_prices') }}
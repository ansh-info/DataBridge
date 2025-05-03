{{ config(
    materialized='view'
) }}

-- Intermediate model: test commodity prices per date
select
  commodity,
  date,
  price
from {{ ref('stg_test_commodity_prices') }}

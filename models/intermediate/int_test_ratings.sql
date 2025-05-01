{{ config(
    materialized='view'
) }}

-- Intermediate model: daily analyst ratings per symbol
select
  symbol,
  date,
  rating,
  rating_count
from {{ ref('stg_test_ratings') }}
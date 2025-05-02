{{ config(
    materialized='table'
) }}

-- Fact table: analyst ratings per symbol and date
select
  symbol,
  date,
  rating,
  rating_count
from {{ ref('int_test_ratings') }}
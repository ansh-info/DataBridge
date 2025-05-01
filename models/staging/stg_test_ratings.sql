{{ config(
    materialized='view'
) }}

-- Staging model: synthetic analyst rating data
select
  symbol,
  date,
  rating,
  rating_count
from `{{ target.project }}.{{ target.dataset }}.test_ratings`
{{ config(
    materialized='view'
) }}

-- Staging model: test stock split data
select
  symbol,
  date,
  split_ratio
from `{{ target.project }}.{{ target.dataset }}.test_splits`

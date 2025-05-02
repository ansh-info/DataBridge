{{ config(
    materialized='table'
) }}

-- Fact table: stock split events per symbol and date
select
  symbol,
  date,
  split_ratio
from {{ ref('int_test_splits') }}
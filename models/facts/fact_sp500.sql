{{ config(
    materialized='table'
) }}

-- Fact table: test S&P 500 index data per date
select
  date,
  open,
  high,
  low,
  close,
  volume
from {{ ref('int_test_sp500') }}

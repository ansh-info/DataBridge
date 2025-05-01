{{ config(
    materialized='view'
) }}

-- Intermediate model: filter only stock split events
select
  symbol,
  date,
  split_ratio
from {{ ref('stg_test_splits') }}
where split_ratio != 1.0
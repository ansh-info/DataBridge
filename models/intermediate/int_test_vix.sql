{{ config(
    materialized='view'
) }}

-- Intermediate model: synthetic VIX index data per date
select
  date,
  implied_volatility
from {{ ref('stg_test_vix') }}
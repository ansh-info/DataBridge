{{ config(materialized='table') }}

-- Dimension: calendar dates
with bounds as (
  select
    min(date) as min_date,
    max(date) as max_date
  from {{ ref('stg_test_economic_indicators') }}
), calendar as (
  select generate_date_array(min_date, max_date) as dates
  from bounds
)
select
  date as date,
  extract(year from date)    as year,
  extract(month from date)   as month,
  extract(day from date)     as day,
  extract(dayofweek from date) as weekday
from calendar, unnest(dates) as date
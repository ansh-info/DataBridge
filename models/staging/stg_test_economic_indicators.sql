{{ config(
    materialized='view'
) }}

-- Staging model: synthetic economic indicators (no symbol)
select
    date(date) as date,
    inflation_rate,
    unemployment_rate,
    gdp_growth,
    interest_rate
from `{{ target.project }}.{{ target.dataset }}.test_economic_indicators`
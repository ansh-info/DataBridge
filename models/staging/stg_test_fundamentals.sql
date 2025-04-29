{{ config(
    materialized='view'
) }}

-- Staging model: synthetic fundamental metrics for stocks
select
    symbol,
    date(date) as date,
    eps,
    pe_ratio,
    market_cap,
    sector
from `{{ target.project }}.{{ target.dataset }}.test_fundamentals`
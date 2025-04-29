{{ config(
    materialized='view'
) }}

-- Staging model: synthetic technical indicators for stocks
select
    symbol,
    date(date) as date,
    ma_10,
    ma_50,
    rsi,
    macd,
    bb_lower,
    bb_upper
from `{{ target.project }}.{{ target.dataset }}.test_technical_indicators`
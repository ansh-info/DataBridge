{{ config(
    materialized='view'
) }}

-- Staging model: test real-time stock data points generated every interval
select
    symbol,
    timestamp,
    date,
    time,
    open,
    high,
    low,
    close,
    volume
from `{{ target.project }}.{{ target.dataset }}.realtime_test_stock_data`
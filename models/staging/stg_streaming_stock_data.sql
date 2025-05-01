{{ config(
    materialized='view',
    enabled=false
) }}

-- Staging model: raw intraday stock data from streaming pipeline
select
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    volume
from `{{ target.project }}.{{ target.dataset }}.streaming_stock_data`
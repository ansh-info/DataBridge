{{ config(
    materialized='view',
    enabled=false
) }}

-- Staging model: raw cryptocurrency price history data from Kaggle static pipeline
select
    Date   as date,
    Symbol as symbol,
    Open   as open,
    High   as high,
    Low    as low,
    Close  as close,
    Volume as volume
from `{{ target.project }}.{{ target.dataset }}.crypto_price_history`
{{ config(
    materialized='view',
    enabled=false
) }}

-- Staging model: raw S&P 500 daily index data from Kaggle static pipeline
select
    Date  as date,
    Open  as open,
    High  as high,
    Low   as low,
    Close as close,
    Volume as volume
from `{{ target.project }}.{{ target.dataset }}.sandp500_data`
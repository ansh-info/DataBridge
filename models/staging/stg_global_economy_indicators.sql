{{ config(
    materialized='view',
    enabled=false
) }}

-- Staging model: raw global economy indicators data from Kaggle static pipeline
select *
from `{{ target.project }}.{{ target.dataset }}.global_economy_indicators`
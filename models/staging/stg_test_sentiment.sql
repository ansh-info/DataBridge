{{ config(
    materialized='view'
) }}

-- Staging model: test sentiment data for stocks
select
    symbol,
    date(date) as date,
    sentiment_score,
    news_volume,
    twitter_mentions
from `{{ target.project }}.{{ target.dataset }}.test_sentiment`

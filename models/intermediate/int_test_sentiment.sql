{{ config(
    materialized='view'
) }}

-- Intermediate aggregate: average sentiment per symbol
select
  symbol,
  avg(sentiment_score) as avg_sentiment_score,
  avg(news_volume) as avg_news_volume,
  avg(twitter_mentions) as avg_twitter_mentions
from {{ ref('stg_test_sentiment') }}
group by symbol
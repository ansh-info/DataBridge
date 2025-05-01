#!/usr/bin/env python3
"""
Generate synthetic static test datasets and load them into BigQuery.
"""
import os
import sys
import random
from datetime import date, datetime, timedelta

import pandas as pd
from google.cloud import bigquery

# Load environment variables from .env if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("[WARN] python-dotenv not installed; skipping .env loading")

# Ensure project root is in path for imports
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME


def get_symbols():
    """Retrieve list of stock symbols from environment."""
    symbols = os.getenv("STOCK_SYMBOLS", "")
    return [s.strip() for s in symbols.split(",") if s.strip()]


def generate_date_range(days=30):
    """Generate a list of dates from `days` ago until today."""
    end = date.today()
    start = end - timedelta(days=days)
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def generate_fundamentals(symbols, dates):
    """Generate synthetic fundamental metrics per symbol per date."""
    sectors = [
        "Technology",
        "Healthcare",
        "Financials",
        "Energy",
        "Consumer Discretionary",
        "Industrials",
        "Materials",
        "Utilities",
    ]
    rows = []
    for sym in symbols:
        base_eps = random.uniform(0.5, 5.0)
        base_pe = random.uniform(10, 50)
        base_mc = random.uniform(1e9, 5e12)
        sector = random.choice(sectors)
        for d in dates:
            eps = max(0, base_eps + random.uniform(-0.05, 0.05))
            pe = max(0, base_pe + random.uniform(-1, 1))
            mc = max(1e7, base_mc * random.uniform(0.99, 1.01))
            rows.append(
                {
                    "symbol": sym,
                    "date": d,
                    "eps": round(eps, 2),
                    "pe_ratio": round(pe, 2),
                    "market_cap": round(mc),
                    "sector": sector,
                }
            )
    return pd.DataFrame(rows)


def generate_technical(symbols, dates):
    """Generate synthetic technical indicators per symbol per date."""
    rows = []
    for sym in symbols:
        for d in dates:
            ma_10 = random.uniform(100, 300)
            ma_50 = random.uniform(100, 300)
            rsi = random.uniform(0, 100)
            macd = random.uniform(-5, 5)
            bb_lower = ma_10 - random.uniform(1, 10)
            bb_upper = ma_10 + random.uniform(1, 10)
            rows.append(
                {
                    "symbol": sym,
                    "date": d,
                    "ma_10": round(ma_10, 2),
                    "ma_50": round(ma_50, 2),
                    "rsi": round(rsi, 2),
                    "macd": round(macd, 2),
                    "bb_lower": round(bb_lower, 2),
                    "bb_upper": round(bb_upper, 2),
                }
            )
    return pd.DataFrame(rows)


def generate_sentiment(symbols, dates):
    """Generate synthetic sentiment metrics per symbol per date."""
    rows = []
    for sym in symbols:
        for d in dates:
            sentiment = random.uniform(-1, 1)
            news_vol = random.randint(0, 1000)
            twitter = random.randint(0, 10000)
            rows.append(
                {
                    "symbol": sym,
                    "date": d,
                    "sentiment_score": round(sentiment, 3),
                    "news_volume": news_vol,
                    "twitter_mentions": twitter,
                }
            )
    return pd.DataFrame(rows)


def generate_economic(dates):
    """Generate synthetic economic indicators per date (no symbol)."""
    rows = []
    for d in dates:
        inflation = random.uniform(-2, 4)
        unemployment = random.uniform(3, 10)
        gdp_growth = random.uniform(-1, 5)
        interest = random.uniform(0, 5)
        rows.append(
            {
                "date": d,
                "inflation_rate": round(inflation, 2),
                "unemployment_rate": round(unemployment, 2),
                "gdp_growth": round(gdp_growth, 2),
                "interest_rate": round(interest, 2),
            }
        )
    return pd.DataFrame(rows)
    
def generate_dividends(symbols, dates):
    """Generate synthetic dividend data per symbol per date."""
    rows = []
    for sym in symbols:
        for d in dates:
            # Randomly assign dividends (most days zero)
            if random.random() < 0.02:
                amt = round(random.uniform(0.1, 2.0), 2)
                yld = round(random.uniform(0.001, 0.05), 4)
            else:
                amt = 0.0
                yld = 0.0
            rows.append({
                "symbol": sym,
                "date": d,
                "dividend_amount": amt,
                "dividend_yield": yld,
            })
    return pd.DataFrame(rows)


def load_to_bq(df, table_name, truncate=True):
    """Load a Pandas DataFrame to BigQuery, optionally truncating the target table."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if truncate
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    # Ensure date column is datetime
    df["date"] = pd.to_datetime(df["date"])
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"[INFO] Loaded {len(df)} rows into {table_id}")


def run_pipeline(days: int = 30):
    """Main entrypoint: generate and load all synthetic test datasets."""
    print(f"[INFO] Generating test static datasets at {datetime.now()}")
    setup_gcp_auth()
    symbols = get_symbols()
    if not symbols:
        print("[ERROR] No STOCK_SYMBOLS found; set the STOCK_SYMBOLS env var.")
        return
    dates = generate_date_range(days)
    # Fundamentals
    df_fund = generate_fundamentals(symbols, dates)
    load_to_bq(df_fund, "test_fundamentals", truncate=True)
    # Technical indicators
    df_tech = generate_technical(symbols, dates)
    load_to_bq(df_tech, "test_technical_indicators", truncate=True)
    # Sentiment
    df_sent = generate_sentiment(symbols, dates)
    load_to_bq(df_sent, "test_sentiment", truncate=True)
    # Economic indicators
    df_econ = generate_economic(dates)
    load_to_bq(df_econ, "test_economic_indicators", truncate=True)
    # Dividends
    df_div = generate_dividends(symbols, dates)
    load_to_bq(df_div, "test_dividends", truncate=True)
    print("[INFO] Test static data pipeline complete.")


if __name__ == "__main__":
    run_pipeline()

import os
import sys

# Ensure project root is on sys.path, so modules can be imported when running this script directly
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from etl.alpha_vantage import fetch_intraday_data

setup_gcp_auth()

# Try fetching data from Alpha Vantage
data = fetch_intraday_data("AAPL")

# Preview keys in response (optional)
if data:
    print("Sample keys in response:", list(data.keys()))

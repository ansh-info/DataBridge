#!/usr/bin/env python3
"""
Standalone script to test Alpha Vantage API keys for rate limiting.
Reads ALPHA_VANTAGE_KEYS from .env (comma-separated) and checks each key.
"""
import os
import sys
import socket
import argparse
import urllib.request
import urllib.parse
import json
# Simple .env loader (if python-dotenv is unavailable)
# Locate the .env file in the project root (one directory above tests)
env_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, '.env')
)
# Load .env variables manually
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k, v)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Alpha Vantage diagnostics: network + rate limit")
parser.add_argument("--key", "-k", help="Test only this API key (overrides keys in .env)")
args = parser.parse_args()

# Gather API keys
if args.key:
    keys = [args.key.strip()]
else:
    api_keys_raw = os.getenv('ALPHA_VANTAGE_KEYS')
    if not api_keys_raw:
        print("ALPHA_VANTAGE_KEYS not set in .env file.")
        sys.exit(1)
    keys = [k.strip() for k in api_keys_raw.split(',') if k.strip()]
    if not keys:
        print("No API keys found in ALPHA_VANTAGE_KEYS.")
        sys.exit(1)

# Network diagnostics
base_url = "https://www.alphavantage.co/query"
host = urllib.parse.urlparse(base_url).hostname or "www.alphavantage.co"
print("=== Network Diagnostics ===")
try:
    ip = socket.gethostbyname(host)
    print(f"DNS resolution: {host} -> {ip}")
except Exception as e:
    print(f"DNS resolution failed for {host}: {e}")
try:
    sock = socket.create_connection((host, 443), timeout=5)
    sock.close()
    print(f"TCP connection to {host}:443 succeeded")
except Exception as e:
    print(f"TCP connection to {host}:443 failed: {e}")
print("=== End Network Diagnostics ===\n")

symbol = "AAPL"
interval = "1min"

rate_limited_keys = []
for key in keys:
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": key,
        "datatype": "json"
    }
    # Send request via urllib to avoid external dependencies
    try:
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            body = resp.read().decode('utf-8')
        data = json.loads(body)
    except Exception as e:
        print(f"Key {key}: Request failed: {e}")
        continue

    # Check for rate limit messages (Note or Information)
    if 'Note' in data:
        print(f"Key {key} is rate limited (Note): {data.get('Note')}")
        rate_limited_keys.append(key)
    elif 'Information' in data:
        info = data.get('Information')
        print(f"Key {key} is rate limited or invalid (Information): {info}")
        rate_limited_keys.append(key)
    elif 'Error Message' in data:
        print(f"Key {key} returned error: {data.get('Error Message')}")
    elif any(k.startswith('Time Series') for k in data):
        print(f"Key {key} is working.")
    else:
        print(f"Key {key} returned unexpected response: {data}")

total = len(keys)
limited = len(rate_limited_keys)
if limited == total:
    print("All API keys appear to be rate limited.")
    sys.exit(2)
else:
    print(f"{total - limited} keys available, {limited} rate limited.")
    sys.exit(0)
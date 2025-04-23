#!/bin/bash

# Start cron service
service cron start
echo "[INFO] Started cron service"

# Create logs directory if it doesn't exist
mkdir -p /app/logs
echo "[INFO] Ensured logs directory exists"

# Verify crontab is installed correctly
crontab -l
echo "[INFO] Displayed current crontab"

# Run the pipeline once immediately on startup
echo "[INFO] Running initial pipeline execution..."
cd /app && python streaming/realtime_stock_stream.py >>/app/logs/stock_pipeline_$(date +\%Y\%m\%d).log 2>&1
echo "[INFO] Initial run complete"

# Keep container running and follow logs
echo "[INFO] Container is now running. Following logs..."
tail -f /app/logs/*.log

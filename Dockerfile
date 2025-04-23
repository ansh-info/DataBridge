FROM python:3.12-slim

# Install cron and required system dependencies
RUN apt-get update && apt-get -y install cron default-jre && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create log directory
RUN mkdir -p /app/logs

# Copy crontab file
COPY crontab /etc/cron.d/stock-pipeline-cron
RUN chmod 0644 /etc/cron.d/stock-pipeline-cron

# Apply cron job
RUN crontab /etc/cron.d/stock-pipeline-cron

# Create entrypoint script
RUN echo "#!/bin/bash\nservice cron start\ntail -f /app/logs/*.log" > /app/run_stock_stream.sh
RUN chmod +x /app/run_stock_stream.sh

# Run cron in foreground
ENTRYPOINT ["/app/run_stock_stream.sh"]

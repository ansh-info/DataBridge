# Use official Python slim image and install Java runtime
FROM python:3.12-slim
RUN apt-get update && apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .


# Default entrypoint: run arbitrary Python scripts
ENTRYPOINT ["python"]
CMD ["--help"]

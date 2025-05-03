#!/usr/bin/env python3
"""
Kafka consumer for real-time stock data.

Reads JSON messages from a Kafka topic, deserializes them,
and writes each record into BigQuery.
"""
import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
import json
from google.cloud import bigquery

# Ensure project root is on Python path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME

def main():
    # Load environment variables
    load_dotenv()

    # Setup GCP credentials
    setup_gcp_auth()

    # Kafka configuration
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "realtime_stock_data")
    group = os.getenv("KAFKA_CONSUMER_GROUP", "stock-data-consumer")

    # Configure confluent-kafka consumer
    consumer_conf = {
        'bootstrap.servers': bootstrap,
        'group.id': group,
        'auto.offset.reset': 'earliest',
    }
    # Create topic if it does not exist
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({'bootstrap.servers': bootstrap})
        metadata = admin.list_topics(timeout=5)
        if topic not in metadata.topics:
            new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
            fs = admin.create_topics([new_topic])
            for t, f in fs.items():
                try:
                    f.result()
                    print(f"[INFO] Created Kafka topic: {t}")
                except Exception as e:
                    print(f"[WARN] Topic {t} creation failed or already exists: {e}")
    except Exception as e:
        print(f"[ERROR] Failed to create topic via AdminClient: {e}")
    # Instantiate and subscribe consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    # BigQuery client and load job config
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.streaming_stock_data"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
        write_disposition="WRITE_APPEND",
    )

    print(f"[INFO] Kafka consumer listening on topic '{topic}', writing to {table_id}")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Log and continue on non-fatal errors
                print(f"[ERROR] Consumer error: {msg.error()}", file=sys.stderr)
                continue

            # Deserialize the JSON message
            payload = msg.value().decode('utf-8')
            record = json.loads(payload)
            # Parse ISO timestamp string into datetime
            ts = record.get("timestamp")
            if isinstance(ts, str):
                record["timestamp"] = datetime.fromisoformat(ts)

            # Only select allowed fields for streaming_stock_data table
            allowed_fields = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
            clean_record = {k: record[k] for k in allowed_fields if k in record}

            # Load record into BigQuery as a one-row DataFrame
            import pandas as pd
            df = pd.DataFrame([clean_record])
            try:
                job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()
                print(f"[SUCCESS] Wrote record for {clean_record.get('symbol')} at {clean_record.get('timestamp')}")
            except Exception as e:
                print(f"[ERROR] Failed to write to BigQuery: {e}", file=sys.stderr)
    except KeyboardInterrupt:
        print("[INFO] Kafka consumer interrupted, shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
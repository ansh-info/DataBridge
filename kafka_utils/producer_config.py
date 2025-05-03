"""
Kafka producer configuration.
Provides a helper to get a KafkaProducer instance configured via environment variables.
"""
import os
from confluent_kafka import Producer

def get_kafka_producer():
    """
    Instantiate and return a KafkaProducer.

    Reads KAFKA_BOOTSTRAP_SERVERS from environment (comma-separated list),
    defaulting to 'localhost:9092'. Uses JSON serialization.
    """
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Create a confluent_kafka Producer using JSON serialization
    return Producer({
        'bootstrap.servers': servers
    })

def get_kafka_topic():
    """
    Return the default Kafka topic for streaming stock data.
    """
    return os.getenv("KAFKA_TOPIC", "realtime_stock_data")
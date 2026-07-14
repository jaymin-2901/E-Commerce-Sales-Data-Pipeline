# ============================================================
# kafka/producer.py
# Kafka Producer — Streams order events to Kafka topic
#
# What this does:
#   1. Initializes master data (customers, products, stores)
#   2. Inserts master data into PostgreSQL
#   3. Continuously generates order batches using Faker
#   4. Serializes each order to JSON
#   5. Sends to Kafka topic 'ecommerce_orders'
#   6. Repeats every BATCH_INTERVAL_SECS seconds
# ============================================================

import sys
import os
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG, GENERATOR_CONFIG
from data_generator.faker_generator import (
    initialize_master_data,
    generate_order_batch
)
from database.db_connector import DatabaseConnector


# ============================================================
# SERIALIZER
# Kafka only understands bytes — must convert Python dict → JSON → bytes
# ============================================================

def json_serializer(data: dict) -> bytes:
    """
    Serialize Python dict to JSON bytes for Kafka.
    Kafka messages must be bytes — not strings or dicts.

    Args:
        data: Python dictionary to serialize

    Returns:
        bytes: UTF-8 encoded JSON string
    """
    return json.dumps(data, default=str).encode('utf-8')
    # default=str handles datetime objects that aren't JSON serializable


# ============================================================
# DELIVERY CALLBACK
# Called by Kafka after message is acknowledged by broker
# ============================================================

def on_send_success(record_metadata):
    """
    Called when message is successfully delivered to Kafka broker.

    record_metadata contains:
    - topic: which topic it was sent to
    - partition: which partition it landed in
    - offset: position within that partition
    """
    logger.debug(
        f"✅ Message delivered → "
        f"Topic: {record_metadata.topic} | "
        f"Partition: {record_metadata.partition} | "
        f"Offset: {record_metadata.offset}"
    )


def on_send_error(exception):
    """Called when message delivery fails."""
    logger.error(f"❌ Message delivery failed: {exception}")


# ============================================================
# KAFKA PRODUCER CLASS
# ============================================================

class EcommerceProducer:
    """
    Kafka Producer for e-commerce order events.

    Responsibilities:
    - Connect to Kafka broker
    - Generate order events using Faker
    - Serialize and send to Kafka topic
    - Handle errors and retries
    """

    def __init__(self):
        self.producer = None
        self.db        = None
        self.topic     = KAFKA_CONFIG["topic_orders"]
        self._connect()

    def _connect(self):
        """
        Initialize Kafka producer with configuration.

        Key producer settings:
        - bootstrap_servers: Kafka broker address
        - value_serializer: converts dict → bytes automatically
        - acks='all': wait for ALL replicas to confirm (safest)
        - retries=3: retry 3 times on transient failures
        - batch_size: group small messages together for efficiency
        - linger_ms: wait up to 10ms to fill a batch before sending
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers  = KAFKA_CONFIG["bootstrap_servers"],
                value_serializer   = json_serializer,
                acks               = 'all',    # Wait for all replicas to confirm
                retries            = 3,         # Retry on failure
                batch_size         = 16384,     # 16KB batch size
                linger_ms          = 10,        # Wait 10ms to fill batch
                compression_type   = 'gzip',   # Compress messages
                max_block_ms       = 5000       # Timeout if broker unreachable
            )
            logger.success("✅ Kafka Producer connected successfully")
        except Exception as e:
            logger.error(f"❌ Kafka Producer connection failed: {e}")
            raise

    def _connect_db(self):
        """Initialize DB connection for master data insertion."""
        try:
            self.db = DatabaseConnector()
            logger.success("✅ Database connected for master data insertion")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def initialize_pipeline(self):
        """
        One-time setup:
        1. Generate master data (customers, products, stores)
        2. Insert master data into PostgreSQL
        """
        logger.info("🔄 Initializing pipeline master data...")

        # Generate master data using Faker
        customers, products, stores = initialize_master_data()

        # Connect to DB and insert master data
        self._connect_db()

        inserted_customers = self.db.insert_customers(customers)
        inserted_products  = self.db.insert_products(products)
        inserted_stores    = self.db.insert_stores(stores)

        logger.success(
            f"✅ Master data loaded → "
            f"Customers: {inserted_customers} | "
            f"Products: {inserted_products} | "
            f"Stores: {inserted_stores}"
        )

    def send_order_batch(self, batch_size: int = None) -> int:
        """
        Generate and send one batch of order events to Kafka.

        Flow:
        Faker → order dict → JSON serialize → Kafka topic

        Args:
            batch_size: Number of orders per batch

        Returns:
            int: Number of messages sent
        """
        if batch_size is None:
            batch_size = GENERATOR_CONFIG["orders_per_batch"]

        # Generate batch of order events
        orders = generate_order_batch(batch_size)
        sent_count = 0

        for order in orders:
            try:
                # Send to Kafka topic with order_id as message key
                # Using order_id as key ensures same customer's orders
                # go to the same partition (ordering guarantee)
                self.producer \
                    .send(
                        self.topic,
                        key   = order["order_id"].encode('utf-8'),
                        value = order
                    ) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

                sent_count += 1

            except KafkaError as e:
                logger.error(f"❌ Failed to send order {order['order_id']}: {e}")

        # Flush ensures all buffered messages are sent before returning
        self.producer.flush()
        return sent_count

    def run_continuous(self):
        """
        Main loop — continuously generates and sends order batches.
        Runs until manually stopped (Ctrl+C).

        Each iteration:
        1. Generate batch of orders
        2. Send to Kafka
        3. Wait BATCH_INTERVAL_SECS
        4. Repeat
        """
        interval = GENERATOR_CONFIG["batch_interval_secs"]
        batch_size = GENERATOR_CONFIG["orders_per_batch"]
        total_sent = 0
        batch_num  = 0

        logger.info(f"🚀 Starting continuous order stream...")
        logger.info(f"   Batch size : {batch_size} orders")
        logger.info(f"   Interval   : {interval} seconds")
        logger.info(f"   Topic      : {self.topic}")
        logger.info("   Press Ctrl+C to stop\n")

        try:
            while True:
                batch_num  += 1
                start_time  = time.time()

                # Send one batch
                sent = self.send_order_batch(batch_size)
                total_sent += sent

                elapsed = round(time.time() - start_time, 2)

                logger.info(
                    f"📤 Batch #{batch_num:04d} | "
                    f"Sent: {sent} orders | "
                    f"Total: {total_sent:,} | "
                    f"Time: {elapsed}s | "
                    f"{datetime.now().strftime('%H:%M:%S')}"
                )

                # Wait before next batch
                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info(f"\n🛑 Producer stopped by user")
            logger.info(f"   Total orders sent: {total_sent:,}")
        finally:
            self.close()

    def close(self):
        """Clean shutdown — flush remaining messages and close."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka Producer closed")
        if self.db:
            self.db.close_all()


# ============================================================
# ENTRY POINT
# python kafka/producer.py
# ============================================================

if __name__ == "__main__":
    print("=" * 55)
    print("  📤 ECOMMERCE KAFKA PRODUCER")
    print("=" * 55)

    producer = EcommerceProducer()

    # Step 1: Load master data into PostgreSQL
    producer.initialize_pipeline()

    # Step 2: Start streaming orders to Kafka
    producer.run_continuous()

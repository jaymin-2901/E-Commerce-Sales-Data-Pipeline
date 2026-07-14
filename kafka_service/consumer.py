# ============================================================
# kafka/consumer.py
# Kafka Consumer — Reads order events and loads to PostgreSQL
#
# What this does:
#   1. Subscribes to Kafka topic 'ecommerce_orders'
#   2. Reads messages in batches (poll-based)
#   3. Deserializes JSON bytes → Python dict
#   4. Sends raw data to Pandas Transformer (Phase 5)
#   5. Inserts cleaned data into PostgreSQL via DB Connector
#   6. Commits Kafka offset after successful DB insert
# ============================================================

import sys
import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG
from transforms.transformer import OrderTransformer
from database.db_connector import DatabaseConnector


# ============================================================
# DESERIALIZER
# Opposite of producer's serializer
# Converts bytes → Python dict
# ============================================================

def json_deserializer(data: bytes) -> dict:
    """
    Deserialize Kafka message bytes back to Python dict.

    Args:
        data: Raw bytes from Kafka message

    Returns:
        dict: Parsed Python dictionary
    """
    return json.loads(data.decode('utf-8'))


# ============================================================
# KAFKA CONSUMER CLASS
# ============================================================

class EcommerceConsumer:
    """
    Kafka Consumer for e-commerce order events.

    Responsibilities:
    - Subscribe to Kafka topic
    - Poll messages in batches
    - Transform raw data using Pandas
    - Insert cleaned data into PostgreSQL
    - Commit offsets only after successful DB insert
    """

    def __init__(self):
        self.consumer    = None
        self.transformer = OrderTransformer()
        self.db          = DatabaseConnector()
        self.topic       = KAFKA_CONFIG["topic_orders"]

        # Stats tracking
        self.total_consumed  = 0
        self.total_inserted  = 0
        self.total_failed    = 0
        self.total_duplicate = 0

        self._connect()

    def _connect(self):
        """
        Initialize Kafka consumer with configuration.

        Key consumer settings:
        - group_id: consumers in same group share partition load
        - auto_offset_reset='earliest': start from beginning if no saved offset
        - enable_auto_commit=False: we manually commit after DB insert (safer!)
        - max_poll_records: max messages per poll() call
        - session_timeout_ms: how long before broker considers consumer dead
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers    = KAFKA_CONFIG["bootstrap_servers"],
                group_id             = KAFKA_CONFIG["group_id"],
                value_deserializer   = json_deserializer,
                auto_offset_reset    = KAFKA_CONFIG["auto_offset_reset"],
                enable_auto_commit   = False,   # Manual commit for safety!
                max_poll_records     = 100,      # Process 100 messages per poll
                session_timeout_ms   = 30000,   # 30 second heartbeat timeout
                heartbeat_interval_ms= 10000,   # Send heartbeat every 10s
                fetch_max_bytes      = 52428800  # 50MB max fetch size
            )
            logger.success(f"✅ Kafka Consumer connected → Topic: {self.topic}")
        except Exception as e:
            logger.error(f"❌ Kafka Consumer connection failed: {e}")
            raise

    def process_batch(self, raw_messages: list) -> dict:
        """
        Process one batch of raw Kafka messages:
        1. Extract message values
        2. Transform using Pandas (clean, validate, enrich)
        3. Insert into PostgreSQL
        4. Return processing stats

        Args:
            raw_messages: List of raw Kafka ConsumerRecord objects

        Returns:
            dict: Processing statistics
        """
        if not raw_messages:
            return {"consumed": 0, "inserted": 0, "failed": 0}

        # Step 1: Extract message values (dict from JSON deserializer)
        raw_orders = [msg.value for msg in raw_messages]

        # Step 2: Transform using Pandas transformer
        transformed_orders, invalid_orders = self.transformer.transform(raw_orders)

        # Step 3: Insert valid orders into PostgreSQL
        inserted = 0
        failed   = 0

        if transformed_orders:
            try:
                inserted = self.db.insert_orders(transformed_orders)
            except Exception as e:
                logger.error(f"❌ DB insert failed: {e}")
                failed = len(transformed_orders)

        return {
            "consumed" : len(raw_messages),
            "inserted" : inserted,
            "invalid"  : len(invalid_orders),
            "failed"   : failed
        }

    def run_continuous(self):
        """
        Main consumer loop — polls Kafka continuously.

        Poll-based consumption:
        - consumer.poll(timeout_ms=1000) waits up to 1 second for messages
        - Returns dict: {TopicPartition → [ConsumerRecord, ...]}
        - We flatten all messages from all partitions into one list
        - Process batch → commit offset → repeat

        Why manual commit (enable_auto_commit=False)?
        - Auto-commit commits BEFORE processing → messages lost on crash
        - Manual commit commits AFTER DB insert → no data loss
        """
        logger.info("🚀 Consumer started — waiting for messages...")
        logger.info(f"   Topic    : {self.topic}")
        logger.info(f"   Group ID : {KAFKA_CONFIG['group_id']}")
        logger.info("   Press Ctrl+C to stop\n")

        poll_count = 0

        try:
            while True:
                # Poll Kafka for new messages (wait up to 1 second)
                message_pack = self.consumer.poll(timeout_ms=1000)

                if not message_pack:
                    # No messages available — wait and try again
                    continue

                poll_count += 1

                # Flatten messages from all partitions
                all_messages = []
                for tp, messages in message_pack.items():
                    all_messages.extend(messages)

                if not all_messages:
                    continue

                # Process this batch
                stats = self.process_batch(all_messages)

                # Update running totals
                self.total_consumed  += stats["consumed"]
                self.total_inserted  += stats["inserted"]
                self.total_failed    += stats["failed"]

                # ── CRITICAL: Commit offset AFTER successful DB insert ──
                # This means: "I've processed these messages, don't re-send"
                # If DB insert failed → don't commit → messages re-delivered
                if stats["inserted"] > 0:
                    self.consumer.commit()

                # Log progress every poll
                logger.info(
                    f"📥 Poll #{poll_count:04d} | "
                    f"Consumed: {stats['consumed']} | "
                    f"Inserted: {stats['inserted']} | "
                    f"Invalid: {stats.get('invalid', 0)} | "
                    f"Total: {self.total_inserted:,} | "
                    f"{datetime.now().strftime('%H:%M:%S')}"
                )

        except KeyboardInterrupt:
            logger.info("\n🛑 Consumer stopped by user")
            self._print_final_stats()
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
            raise
        finally:
            self.close()

    def _print_final_stats(self):
        """Print summary statistics on shutdown."""
        logger.info("\n" + "=" * 45)
        logger.info("  📊 CONSUMER FINAL STATISTICS")
        logger.info("=" * 45)
        logger.info(f"  Total consumed : {self.total_consumed:,}")
        logger.info(f"  Total inserted : {self.total_inserted:,}")
        logger.info(f"  Total failed   : {self.total_failed:,}")
        success_rate = (
            self.total_inserted / self.total_consumed * 100
            if self.total_consumed > 0 else 0
        )
        logger.info(f"  Success rate   : {success_rate:.1f}%")
        logger.info("=" * 45)

    def close(self):
        """Clean shutdown."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Consumer closed")
        if self.db:
            self.db.close_all()


# ============================================================
# ENTRY POINT
# python kafka/consumer.py
# ============================================================

if __name__ == "__main__":
    print("=" * 55)
    print("  📥 ECOMMERCE KAFKA CONSUMER")
    print("=" * 55)

    consumer = EcommerceConsumer()
    consumer.run_continuous()

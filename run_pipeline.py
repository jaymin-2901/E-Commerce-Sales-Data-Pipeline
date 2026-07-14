# ============================================================
# run_pipeline.py
# MASTER VERIFICATION SCRIPT
#
# Run this to verify ALL components are working correctly.
# Shows step-by-step status of every part of the pipeline.
#
# Usage:
#   python run_pipeline.py
# ============================================================

import sys
import os
import json
import time
import subprocess
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_step(step, description):
    print(f"\n{'─'*60}")
    print(f"  STEP {step}: {description}")
    print(f"{'─'*60}")


def print_ok(msg):
    print(f"  ✅ {msg}")


def print_fail(msg):
    print(f"  ❌ {msg}")


def print_info(msg):
    print(f"  ℹ️  {msg}")


# ============================================================
print_header("🛒 E-COMMERCE PIPELINE — VERIFICATION SCRIPT")
print(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Python    : {sys.version.split()[0]}")


# ============================================================
# STEP 1: CHECK IMPORTS
# ============================================================
print_step(1, "Checking Python Dependencies")

required = [
    ('faker',      'Faker'),
    ('kafka',      'KafkaProducer'),
    ('pandas',     'pandas'),
    ('psycopg2',   'psycopg2'),
    ('streamlit',  'streamlit'),
    ('plotly',     'plotly'),
    ('loguru',     'loguru'),
    ('sqlalchemy', 'sqlalchemy'),
]

all_imports_ok = True
for module, name in required:
    try:
        __import__(module)
        print_ok(f"{name} imported successfully")
    except ImportError:
        print_fail(f"{name} NOT installed — run: pip install -r requirements.txt")
        all_imports_ok = False

if not all_imports_ok:
    print("\n  ⚠️  Fix missing packages before continuing.")
    sys.exit(1)


# ============================================================
# STEP 2: CHECK DOCKER CONTAINERS
# ============================================================
print_step(2, "Checking Docker Containers")

try:
    result = subprocess.run(
        ['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'],
        capture_output=True, text=True, timeout=10
    )
    running = result.stdout.strip()

    containers = {
        'zookeeper'           : '❌ NOT running',
        'kafka'               : '❌ NOT running',
        'postgres_ecommerce'  : '❌ NOT running'
    }

    for line in running.split('\n'):
        parts = line.split('\t')
        if len(parts) >= 2:
            name, status = parts[0], parts[1]
            if name in containers:
                containers[name] = f"✅ {status}"

    for name, status in containers.items():
        print(f"  {status} — {name}")

    if any('NOT' in v for v in containers.values()):
        print("\n  ⚠️  Start Docker containers first:")
        print("      docker-compose up -d")
        print("      Wait 30 seconds, then run this script again.")
        sys.exit(1)

except FileNotFoundError:
    print_fail("Docker not found — install Docker Desktop first")
    sys.exit(1)
except Exception as e:
    print_fail(f"Docker check failed: {e}")


# ============================================================
# STEP 3: CHECK POSTGRESQL CONNECTION
# ============================================================
print_step(3, "Checking PostgreSQL Connection & Schema")

try:
    from database.db_connector import DatabaseConnector

    db = DatabaseConnector()

    # Health check
    if db.health_check():
        print_ok("PostgreSQL connection established")
    else:
        print_fail("PostgreSQL health check failed")
        sys.exit(1)

    # Check tables exist
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]

    expected_tables = ['customers', 'daily_summary', 'orders', 'products', 'stores']
    for table in expected_tables:
        if table in tables:
            print_ok(f"Table '{table}' exists")
        else:
            print_fail(f"Table '{table}' missing — schema not applied")

    # Check views
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.views
            WHERE table_schema = 'public'
        """)
        views = [row[0] for row in cur.fetchall()]

    print_ok(f"Views found: {len(views)} ({', '.join(views[:3])}...)")
    db.return_connection(conn)
    db.close_all()

except Exception as e:
    print_fail(f"PostgreSQL error: {e}")


# ============================================================
# STEP 4: CHECK KAFKA CONNECTION
# ============================================================
print_step(4, "Checking Kafka Connection")

try:
    from kafka import KafkaConsumer
    from config.config import KAFKA_CONFIG

    test_consumer = KafkaConsumer(
        bootstrap_servers    = KAFKA_CONFIG["bootstrap_servers"],
        consumer_timeout_ms  = 3000
    )
    topics = test_consumer.topics()
    test_consumer.close()

    print_ok(f"Kafka broker reachable")
    if KAFKA_CONFIG["topic_orders"] in topics:
        print_ok(f"Topic '{KAFKA_CONFIG['topic_orders']}' exists")
    else:
        print_info(f"Topic '{KAFKA_CONFIG['topic_orders']}' not yet created")
        print_info("It will be auto-created when producer first runs")

except Exception as e:
    print_fail(f"Kafka connection failed: {e}")
    print_info("Make sure Kafka container is running and healthy")


# ============================================================
# STEP 5: TEST FAKER DATA GENERATOR
# ============================================================
print_step(5, "Testing Faker Data Generator")

try:
    from data_generator.faker_generator import (
        initialize_master_data,
        generate_order_batch
    )

    customers, products, stores = initialize_master_data()
    print_ok(f"Generated {len(customers)} customers")
    print_ok(f"Generated {len(products)} products")
    print_ok(f"Generated {len(stores)} stores")

    batch = generate_order_batch(batch_size=5)
    print_ok(f"Generated test batch of {len(batch)} orders")

    # Show sample order
    sample = batch[0]
    print(f"\n  📦 Sample Order:")
    print(f"     order_id     : {sample['order_id'][:16]}...")
    print(f"     category     : {sample['category']}")
    print(f"     final_amount : ₹{sample['final_amount']:,.2f}")
    print(f"     payment      : {sample['payment_method']}")
    print(f"     status       : {sample['order_status']}")
    print(f"     city         : {sample['customer_city']}")

except Exception as e:
    print_fail(f"Faker generator error: {e}")


# ============================================================
# STEP 6: TEST PANDAS TRANSFORMER
# ============================================================
print_step(6, "Testing Pandas Transformer")

try:
    from transforms.transformer import OrderTransformer
    from data_generator.faker_generator import generate_order_batch

    transformer = OrderTransformer()
    raw_orders  = generate_order_batch(batch_size=20)

    valid, invalid = transformer.transform(raw_orders)

    print_ok(f"Transformed {len(raw_orders)} raw orders")
    print_ok(f"Valid orders   : {len(valid)}")
    print_info(f"Invalid orders : {len(invalid)} (expected ~0 for Faker data)")

    if valid:
        sample = valid[0]
        print_ok(f"Sample transformed order has {len(sample)} fields")

except Exception as e:
    print_fail(f"Transformer error: {e}")


# ============================================================
# STEP 7: MINI PIPELINE TEST (Producer → Consumer → DB)
# ============================================================
print_step(7, "Running Mini Pipeline Test (10 orders end-to-end)")

try:
    from kafka import KafkaProducer, KafkaConsumer
    from config.config import KAFKA_CONFIG
    from database.db_connector import DatabaseConnector
    from transforms.transformer import OrderTransformer
    from data_generator.faker_generator import generate_order_batch

    def serializer(data):
        return json.dumps(data, default=str).encode('utf-8')

    def deserializer(data):
        return json.loads(data.decode('utf-8'))

    db          = DatabaseConnector()
    transformer = OrderTransformer()

    # Get order count before test
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        count_before = cur.fetchone()[0]
    db.return_connection(conn)
    print_info(f"Orders in DB before test: {count_before:,}")

    # Step A: Produce 10 test orders
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_CONFIG["bootstrap_servers"],
        value_serializer  = serializer
    )
    test_orders = generate_order_batch(batch_size=10)
    for order in test_orders:
        producer.send(KAFKA_CONFIG["topic_orders"], value=order)
    producer.flush()
    producer.close()
    print_ok("10 test orders sent to Kafka")

    # Step B: Consume them immediately
    time.sleep(2)  # Give Kafka time to store messages
    consumer = KafkaConsumer(
        KAFKA_CONFIG["topic_orders"],
        bootstrap_servers    = KAFKA_CONFIG["bootstrap_servers"],
        group_id             = "verification_test_group",
        value_deserializer   = deserializer,
        auto_offset_reset    = "earliest",
        enable_auto_commit   = True,
        consumer_timeout_ms  = 5000
    )

    consumed = []
    for msg in consumer:
        consumed.append(msg.value)
        if len(consumed) >= 10:
            break
    consumer.close()

    print_ok(f"Consumed {len(consumed)} orders from Kafka")

    # Step C: Transform
    valid, invalid = transformer.transform(consumed)
    print_ok(f"Transformed: {len(valid)} valid orders")

    # Step D: Insert to PostgreSQL
    if valid:
        inserted = db.insert_orders(valid)
        print_ok(f"Inserted {inserted} orders into PostgreSQL")

    # Verify count increased
    conn = db.get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders")
        count_after = cur.fetchone()[0]
    db.return_connection(conn)

    diff = count_after - count_before
    print_ok(f"Orders in DB after test: {count_after:,} (+{diff})")

    if diff > 0:
        print_ok("✅ MINI PIPELINE TEST PASSED — Data flows end-to-end!")
    else:
        print_info("Orders may already exist (ON CONFLICT DO NOTHING)")

    db.close_all()

except Exception as e:
    print_fail(f"Mini pipeline test failed: {e}")


# ============================================================
# STEP 8: CHECK DASHBOARD DEPENDENCIES
# ============================================================
print_step(8, "Checking Dashboard Dependencies")

try:
    import streamlit
    import plotly
    import altair

    print_ok(f"Streamlit {streamlit.__version__}")
    print_ok(f"Plotly    {plotly.__version__}")
    print_ok("Dashboard is ready to run")
    print_info("Start with: streamlit run dashboard/app.py")
    print_info("Opens at  : http://localhost:8501")

except Exception as e:
    print_fail(f"Dashboard dependency error: {e}")


# ============================================================
# FINAL SUMMARY
# ============================================================
print_header("📊 VERIFICATION COMPLETE")

print("""
  HOW TO RUN THE FULL PIPELINE:
  ─────────────────────────────
  Terminal 1 → py kafka_service/producer.py
  Terminal 2 → py kafka_service/consumer.py
  Terminal 3 → streamlit run dashboard/app.py

  Then open  → http://localhost:8501

  WHAT YOU SHOULD SEE:
  ─────────────────────────────
  • Producer sends 50 orders every 10 seconds
  • Consumer inserts them into PostgreSQL
  • Dashboard shows live charts updating in real-time
  • KPI cards show growing order count and revenue

  STOP EVERYTHING:
  ─────────────────────────────
  • Press Ctrl+C in each terminal
  • docker-compose down
""")

print(f"  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

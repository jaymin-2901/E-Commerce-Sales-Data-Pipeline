# ============================================================
# data_generator/faker_generator.py
# Generates realistic fake e-commerce data using Faker library
#
# What this file does:
#   - Generates Customers, Products, Stores (master data)
#   - Generates continuous Order events (transactional data)
#   - Returns data as Python dicts ready for Kafka / PostgreSQL
# ============================================================

import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import sys
import os

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    GENERATOR_CONFIG,
    INDIAN_CITIES,
    REGIONS,
    PRODUCT_CATEGORIES,
    PAYMENT_METHODS,
    ORDER_STATUSES
)

# ── Initialize Faker ─────────────────────────────────────────
# Using 'en_IN' locale for Indian names and addresses
fake = Faker('en_IN')
Faker.seed(42)       # Seed for reproducibility
random.seed(42)

# ── In-memory master data stores ────────────────────────────
# These are generated ONCE and reused for order generation
# (simulates a real DB lookup)
CUSTOMERS = []
PRODUCTS  = []
STORES    = []


# ============================================================
# MASTER DATA GENERATORS
# These run once at startup to populate base tables
# ============================================================

def generate_customer():
    """
    Generate one fake customer record.

    Returns:
        dict: Customer data matching the PostgreSQL customers table schema
    """
    city   = random.choice(INDIAN_CITIES)
    region = REGIONS.get(city, "Central")

    return {
        "customer_id"  : str(uuid.uuid4()),          # Unique ID
        "name"         : fake.name(),                 # Indian name
        "email"        : fake.email(),                # Fake email
        "phone"        : fake.phone_number(),         # Indian phone
        "city"         : city,
        "state"        : fake.state(),
        "region"       : region,
        "age"          : random.randint(18, 65),
        "gender"       : random.choice(["Male", "Female", "Other"]),
        "created_at"   : fake.date_time_between(
                            start_date="-2y",
                            end_date="now"
                         ).isoformat()
    }


def generate_product():
    """
    Generate one fake product record.

    Returns:
        dict: Product data matching PostgreSQL products table schema
    """
    category = random.choice(PRODUCT_CATEGORIES)

    # Price ranges vary by category — makes data realistic
    price_ranges = {
        "Electronics"    : (500,   80000),
        "Fashion"        : (200,   10000),
        "Home & Kitchen" : (300,   25000),
        "Sports"         : (500,   15000),
        "Books"          : (100,   2000),
        "Beauty"         : (150,   5000),
        "Toys"           : (200,   8000),
        "Grocery"        : (50,    2000)
    }

    min_price, max_price = price_ranges.get(category, (100, 5000))
    price = round(random.uniform(min_price, max_price), 2)

    # Discount between 0% and 40%
    discount_pct = random.choice([0, 5, 10, 15, 20, 25, 30, 40])
    discounted_price = round(price * (1 - discount_pct / 100), 2)

    return {
        "product_id"      : str(uuid.uuid4()),
        "name"            : f"{fake.word().capitalize()} {category} {fake.word().capitalize()}",
        "category"        : category,
        "brand"           : fake.company(),
        "original_price"  : price,
        "discounted_price": discounted_price,
        "discount_pct"    : discount_pct,
        "rating"          : round(random.uniform(1.0, 5.0), 1),
        "stock_quantity"  : random.randint(0, 1000),
        "created_at"      : datetime.now().isoformat()
    }


def generate_store():
    """
    Generate one fake store record.

    Returns:
        dict: Store data matching PostgreSQL stores table schema
    """
    city   = random.choice(INDIAN_CITIES)
    region = REGIONS.get(city, "Central")

    return {
        "store_id"  : str(uuid.uuid4()),
        "store_name": f"{city} {fake.word().capitalize()} Store",
        "city"      : city,
        "region"    : region,
        "store_type": random.choice(["Online", "Offline", "Hybrid"]),
        "created_at": fake.date_time_between(
                        start_date="-3y",
                        end_date="-1y"
                      ).isoformat()
    }


# ============================================================
# MASTER DATA INITIALIZATION
# ============================================================

def initialize_master_data():
    """
    Generate all master data (customers, products, stores).
    Called ONCE at pipeline startup.

    Returns:
        tuple: (customers list, products list, stores list)
    """
    global CUSTOMERS, PRODUCTS, STORES

    print("🔄 Generating master data...")

    # Generate customers
    CUSTOMERS = [
        generate_customer()
        for _ in range(GENERATOR_CONFIG["num_customers"])
    ]
    print(f"   ✅ {len(CUSTOMERS)} customers generated")

    # Generate products
    PRODUCTS = [
        generate_product()
        for _ in range(GENERATOR_CONFIG["num_products"])
    ]
    print(f"   ✅ {len(PRODUCTS)} products generated")

    # Generate stores
    STORES = [
        generate_store()
        for _ in range(GENERATOR_CONFIG["num_stores"])
    ]
    print(f"   ✅ {len(STORES)} stores generated")

    return CUSTOMERS, PRODUCTS, STORES


# ============================================================
# ORDER EVENT GENERATOR (Transactional Data)
# This runs continuously to simulate real-time orders
# ============================================================

def generate_order_event():
    """
    Generate one fake order event.
    An order event is what gets streamed through Kafka.

    This simulates a customer placing an order on the platform
    in real-time — like an event from a live e-commerce website.

    Returns:
        dict: Complete order event ready for Kafka serialization
    """
    # Pick random customer, product, store
    customer = random.choice(CUSTOMERS)
    product  = random.choice(PRODUCTS)
    store    = random.choice(STORES)

    # Quantity ordered
    quantity = random.randint(1, 5)

    # Calculate revenue
    unit_price   = product["discounted_price"]
    total_amount = round(unit_price * quantity, 2)

    # Apply tax (GST 18% for most categories)
    gst_rate   = 0.18
    tax_amount = round(total_amount * gst_rate, 2)
    final_amount = round(total_amount + tax_amount, 2)

    # Random order timestamp (within last 7 days for realism)
    order_time = datetime.now() - timedelta(
        days=random.randint(0, 7),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    return {
        # ── Order Identity ───────────────────────────────────
        "order_id"       : str(uuid.uuid4()),
        "event_timestamp": datetime.now().isoformat(),  # When event was created
        "order_timestamp": order_time.isoformat(),       # When order was placed

        # ── Customer Info ────────────────────────────────────
        "customer_id"    : customer["customer_id"],
        "customer_name"  : customer["name"],
        "customer_city"  : customer["city"],
        "customer_region": customer["region"],
        "customer_age"   : customer["age"],
        "customer_gender": customer["gender"],

        # ── Product Info ─────────────────────────────────────
        "product_id"     : product["product_id"],
        "product_name"   : product["name"],
        "category"       : product["category"],
        "brand"          : product["brand"],
        "unit_price"     : unit_price,

        # ── Store Info ───────────────────────────────────────
        "store_id"       : store["store_id"],
        "store_name"     : store["store_name"],
        "store_type"     : store["store_type"],
        "store_city"     : store["city"],
        "store_region"   : store["region"],

        # ── Order Details ────────────────────────────────────
        "quantity"       : quantity,
        "total_amount"   : total_amount,
        "tax_amount"     : tax_amount,
        "final_amount"   : final_amount,
        "payment_method" : random.choice(PAYMENT_METHODS),
        "order_status"   : random.choice(ORDER_STATUSES),

        # ── Metadata ─────────────────────────────────────────
        "is_first_order" : random.random() < 0.2,   # 20% chance first order
        "device_type"    : random.choice(["Mobile", "Desktop", "Tablet"]),
        "source"         : random.choice(["App", "Website", "Social Media"])
    }


def generate_order_batch(batch_size=None):
    """
    Generate a batch of order events.
    This is what the Kafka Producer sends in one go.

    Args:
        batch_size (int): Number of orders per batch.
                          Defaults to GENERATOR_CONFIG value.

    Returns:
        list: List of order event dicts
    """
    if batch_size is None:
        batch_size = GENERATOR_CONFIG["orders_per_batch"]

    return [generate_order_event() for _ in range(batch_size)]


# ============================================================
# STANDALONE TEST — Run this file directly to verify output
# python data_generator/faker_generator.py
# ============================================================

if __name__ == "__main__":
    print("=" * 55)
    print("  🧪 FAKER DATA GENERATOR — TEST RUN")
    print("=" * 55)

    # Initialize master data
    customers, products, stores = initialize_master_data()

    print("\n📋 Sample Customer:")
    import json
    print(json.dumps(customers[0], indent=2))

    print("\n📦 Sample Product:")
    print(json.dumps(products[0], indent=2))

    print("\n🏪 Sample Store:")
    print(json.dumps(stores[0], indent=2))

    print("\n🛒 Sample Order Event:")
    order = generate_order_event()
    print(json.dumps(order, indent=2))

    print("\n📦 Sample Order Batch (3 orders):")
    batch = generate_order_batch(batch_size=3)
    for i, o in enumerate(batch, 1):
        print(f"\n  Order {i}: {o['order_id'][:8]}... | "
              f"{o['category']} | ₹{o['final_amount']} | "
              f"{o['order_status']} | {o['payment_method']}")

    print(f"\n✅ Generator working correctly!")
    print(f"   Ready to stream to Kafka.")

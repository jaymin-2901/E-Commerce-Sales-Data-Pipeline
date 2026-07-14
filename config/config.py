# ============================================================
# config/config.py
# Central Configuration File for E-Commerce Data Pipeline
# All settings in ONE place — easy to change, easy to maintain
# ============================================================

# ── PostgreSQL Database Config ──────────────────────────────
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "database": "ecommerce_db",
    "user"    : "postgres",
    "password": "postgres123"
}

# ── Kafka Config ────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",   # Kafka broker address
    "topic_orders"     : "ecommerce_orders", # Topic name for order events
    "group_id"         : "ecommerce_group",  # Consumer group ID
    "auto_offset_reset": "earliest"          # Read from beginning if no offset
}

# ── Data Generator Config ───────────────────────────────────
GENERATOR_CONFIG = {
    "num_customers"       : 500,    # Total fake customers to generate
    "num_products"        : 100,    # Total fake products to generate
    "num_stores"          : 20,     # Total fake stores to generate
    "orders_per_batch"    : 50,     # Orders to generate per Kafka batch
    "batch_interval_secs" : 10      # Seconds between each batch
}

# ── Airflow Config ──────────────────────────────────────────
AIRFLOW_CONFIG = {
    "schedule_interval": "@hourly",  # Run pipeline every hour
    "dag_id"           : "ecommerce_pipeline_dag",
    "owner"            : "jaymin"
}

# ── Streamlit Dashboard Config ──────────────────────────────
DASHBOARD_CONFIG = {
    "title"          : "E-Commerce Sales Analytics Dashboard",
    "refresh_seconds": 30,    # Auto-refresh every 30 seconds
    "page_icon"      : "🛒"
}

# ── Indian Cities & Regions ─────────────────────────────────
# Using Indian context makes the project more realistic & unique
INDIAN_CITIES = [
    "Ahmedabad", "Mumbai", "Delhi", "Bangalore", "Chennai",
    "Hyderabad", "Pune", "Kolkata", "Jaipur", "Surat",
    "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane"
]

REGIONS = {
    "Ahmedabad": "West", "Mumbai"  : "West",   "Surat"    : "West",
    "Delhi"    : "North","Jaipur"  : "North",  "Lucknow"  : "North",
    "Kanpur"   : "North","Bangalore": "South",  "Chennai"  : "South",
    "Hyderabad": "South","Kolkata" : "East",   "Nagpur"   : "Central",
    "Pune"     : "West", "Indore"  : "Central","Thane"    : "West"
}

# ── Product Categories ──────────────────────────────────────
PRODUCT_CATEGORIES = [
    "Electronics", "Fashion", "Home & Kitchen",
    "Sports", "Books", "Beauty", "Toys", "Grocery"
]

# ── Payment Methods ─────────────────────────────────────────
PAYMENT_METHODS = [
    "UPI", "Credit Card", "Debit Card",
    "Net Banking", "Cash on Delivery", "EMI"
]

# ── Order Statuses ──────────────────────────────────────────
ORDER_STATUSES = [
    "Placed", "Confirmed", "Shipped",
    "Delivered", "Cancelled", "Returned"
]

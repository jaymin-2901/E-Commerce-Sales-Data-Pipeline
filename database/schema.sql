-- ============================================================
-- database/schema.sql
-- PostgreSQL Schema for E-Commerce Data Pipeline
--
-- Tables:
--   1. customers     → master customer data
--   2. products      → master product catalog
--   3. stores        → master store data
--   4. orders        → transactional order records
--   5. daily_summary → aggregated daily analytics (pre-computed)
--
-- Run this manually:
--   docker exec -it postgres_ecommerce psql -U postgres -d ecommerce_db -f /schema.sql
-- OR it runs automatically via docker-compose volume mount
-- ============================================================


-- ============================================================
-- CREATE DATABASE (if running manually outside Docker)
-- ============================================================
-- CREATE DATABASE ecommerce_db;
-- \c ecommerce_db;


-- ============================================================
-- DROP TABLES (clean slate on re-run)
-- Order matters: drop child tables before parent tables
-- ============================================================
DROP TABLE IF EXISTS daily_summary CASCADE;
DROP TABLE IF EXISTS orders       CASCADE;
DROP TABLE IF EXISTS stores       CASCADE;
DROP TABLE IF EXISTS products     CASCADE;
DROP TABLE IF EXISTS customers    CASCADE;


-- ============================================================
-- TABLE 1: CUSTOMERS (Master Data)
-- ============================================================
CREATE TABLE customers (
    customer_id   UUID         PRIMARY KEY,           -- Unique customer ID
    name          VARCHAR(100) NOT NULL,
    email         VARCHAR(150) UNIQUE,                -- No duplicate emails
    phone         VARCHAR(20),
    city          VARCHAR(50),
    state         VARCHAR(50),
    region        VARCHAR(20),                        -- North/South/East/West/Central
    age           INTEGER      CHECK (age BETWEEN 0 AND 120),
    gender        VARCHAR(10)  CHECK (gender IN ('Male','Female','Other')),
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Index on region for fast regional analytics queries
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_customers_city   ON customers(city);

COMMENT ON TABLE customers IS 'Master table storing all customer profiles';


-- ============================================================
-- TABLE 2: PRODUCTS (Master Data)
-- ============================================================
CREATE TABLE products (
    product_id        UUID           PRIMARY KEY,
    name              VARCHAR(200)   NOT NULL,
    category          VARCHAR(50)    NOT NULL,
    brand             VARCHAR(100),
    original_price    NUMERIC(12,2)  CHECK (original_price >= 0),
    discounted_price  NUMERIC(12,2)  CHECK (discounted_price >= 0),
    discount_pct      INTEGER        CHECK (discount_pct BETWEEN 0 AND 100),
    rating            NUMERIC(3,1)   CHECK (rating BETWEEN 1.0 AND 5.0),
    stock_quantity    INTEGER        DEFAULT 0,
    created_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Index on category for fast category-wise analytics
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand    ON products(brand);

COMMENT ON TABLE products IS 'Master table storing product catalog';


-- ============================================================
-- TABLE 3: STORES (Master Data)
-- ============================================================
CREATE TABLE stores (
    store_id    UUID         PRIMARY KEY,
    store_name  VARCHAR(150) NOT NULL,
    city        VARCHAR(50),
    region      VARCHAR(20),
    store_type  VARCHAR(20)  CHECK (store_type IN ('Online','Offline','Hybrid')),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stores_region ON stores(region);
CREATE INDEX idx_stores_type   ON stores(store_type);

COMMENT ON TABLE stores IS 'Master table storing store locations and types';


-- ============================================================
-- TABLE 4: ORDERS (Transactional Data — Core Table)
-- This is the MAIN table that Kafka consumer populates
-- ============================================================
CREATE TABLE orders (
    -- ── Primary Key ─────────────────────────────────────────
    order_id          UUID          PRIMARY KEY,

    -- ── Timestamps ──────────────────────────────────────────
    order_timestamp   TIMESTAMP     NOT NULL,          -- When customer placed order
    event_timestamp   TIMESTAMP     NOT NULL,          -- When Kafka event was created
    ingested_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP, -- When loaded to DB

    -- ── Foreign Keys (linking to master tables) ──────────────
    customer_id       UUID          REFERENCES customers(customer_id),
    product_id        UUID          REFERENCES products(product_id),
    store_id          UUID          REFERENCES stores(store_id),

    -- ── Denormalized fields (for fast analytics) ─────────────
    -- We duplicate some fields from master tables here
    -- to avoid expensive JOINs on every dashboard query
    customer_city     VARCHAR(50),
    customer_region   VARCHAR(20),
    customer_age      INTEGER,
    customer_gender   VARCHAR(10),

    product_name      VARCHAR(200),
    category          VARCHAR(50),
    brand             VARCHAR(100),

    store_type        VARCHAR(20),
    store_city        VARCHAR(50),
    store_region      VARCHAR(20),

    -- ── Order Details ────────────────────────────────────────
    quantity          INTEGER       NOT NULL  CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL  CHECK (unit_price >= 0),
    total_amount      NUMERIC(12,2) NOT NULL,
    tax_amount        NUMERIC(12,2) DEFAULT 0,
    final_amount      NUMERIC(12,2) NOT NULL,

    -- ── Payment & Status ─────────────────────────────────────
    payment_method    VARCHAR(30),
    order_status      VARCHAR(20),

    -- ── Metadata ─────────────────────────────────────────────
    is_first_order    BOOLEAN       DEFAULT FALSE,
    device_type       VARCHAR(20),
    source            VARCHAR(30)
);

-- ── Indexes for fast dashboard queries ───────────────────────
CREATE INDEX idx_orders_timestamp     ON orders(order_timestamp);
CREATE INDEX idx_orders_customer      ON orders(customer_id);
CREATE INDEX idx_orders_product       ON orders(product_id);
CREATE INDEX idx_orders_store         ON orders(store_id);
CREATE INDEX idx_orders_category      ON orders(category);
CREATE INDEX idx_orders_region        ON orders(customer_region);
CREATE INDEX idx_orders_status        ON orders(order_status);
CREATE INDEX idx_orders_payment       ON orders(payment_method);

-- Composite index for time-based regional analytics
CREATE INDEX idx_orders_time_region   ON orders(order_timestamp, customer_region);

COMMENT ON TABLE orders IS 'Core transactional table populated by Kafka consumer';


-- ============================================================
-- TABLE 5: DAILY SUMMARY (Pre-aggregated Analytics)
-- Airflow DAG populates this every hour
-- Dashboard reads from this for fast performance
-- ============================================================
CREATE TABLE daily_summary (
    summary_id        SERIAL        PRIMARY KEY,
    summary_date      DATE          NOT NULL,
    category          VARCHAR(50),
    region            VARCHAR(20),

    -- ── Revenue Metrics ──────────────────────────────────────
    total_orders      INTEGER       DEFAULT 0,
    total_revenue     NUMERIC(15,2) DEFAULT 0,
    total_tax         NUMERIC(15,2) DEFAULT 0,
    avg_order_value   NUMERIC(12,2) DEFAULT 0,

    -- ── Volume Metrics ───────────────────────────────────────
    total_quantity    INTEGER       DEFAULT 0,
    unique_customers  INTEGER       DEFAULT 0,
    new_customers     INTEGER       DEFAULT 0,  -- is_first_order = TRUE

    -- ── Status Breakdown ─────────────────────────────────────
    delivered_count   INTEGER       DEFAULT 0,
    cancelled_count   INTEGER       DEFAULT 0,
    returned_count    INTEGER       DEFAULT 0,

    -- ── Payment Breakdown ────────────────────────────────────
    upi_count         INTEGER       DEFAULT 0,
    card_count        INTEGER       DEFAULT 0,
    cod_count         INTEGER       DEFAULT 0,

    -- ── Metadata ─────────────────────────────────────────────
    computed_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint: one row per date+category+region combo
    UNIQUE(summary_date, category, region)
);

CREATE INDEX idx_summary_date     ON daily_summary(summary_date);
CREATE INDEX idx_summary_category ON daily_summary(category);
CREATE INDEX idx_summary_region   ON daily_summary(region);

COMMENT ON TABLE daily_summary IS 'Pre-aggregated daily metrics computed by Airflow DAG';


-- ============================================================
-- USEFUL ANALYTICS VIEWS
-- These are pre-defined SQL queries stored as views
-- Dashboard and Airflow can query these directly
-- ============================================================

-- View 1: Revenue by category (last 30 days)
CREATE OR REPLACE VIEW vw_revenue_by_category AS
SELECT
    category,
    COUNT(*)                        AS total_orders,
    SUM(final_amount)               AS total_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value,
    SUM(quantity)                   AS total_items_sold
FROM orders
WHERE order_timestamp >= NOW() - INTERVAL '30 days'
  AND order_status NOT IN ('Cancelled', 'Returned')
GROUP BY category
ORDER BY total_revenue DESC;

-- View 2: Revenue by region (last 30 days)
CREATE OR REPLACE VIEW vw_revenue_by_region AS
SELECT
    customer_region                 AS region,
    COUNT(*)                        AS total_orders,
    SUM(final_amount)               AS total_revenue,
    COUNT(DISTINCT customer_id)     AS unique_customers
FROM orders
WHERE order_timestamp >= NOW() - INTERVAL '30 days'
GROUP BY customer_region
ORDER BY total_revenue DESC;

-- View 3: Daily revenue trend (last 14 days)
CREATE OR REPLACE VIEW vw_daily_revenue_trend AS
SELECT
    DATE(order_timestamp)           AS order_date,
    COUNT(*)                        AS total_orders,
    SUM(final_amount)               AS daily_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value
FROM orders
WHERE order_timestamp >= NOW() - INTERVAL '14 days'
GROUP BY DATE(order_timestamp)
ORDER BY order_date;

-- View 4: Top 10 products by revenue
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    product_name,
    category,
    brand,
    COUNT(*)                        AS times_ordered,
    SUM(quantity)                   AS total_qty_sold,
    SUM(final_amount)               AS total_revenue
FROM orders
WHERE order_status NOT IN ('Cancelled', 'Returned')
GROUP BY product_name, category, brand
ORDER BY total_revenue DESC
LIMIT 10;

-- View 5: Payment method distribution
CREATE OR REPLACE VIEW vw_payment_distribution AS
SELECT
    payment_method,
    COUNT(*)                        AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM orders
GROUP BY payment_method
ORDER BY order_count DESC;

-- ============================================================
-- VERIFY SCHEMA CREATION
-- ============================================================
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

# ============================================================
# database/db_connector.py
# PostgreSQL Database Connector
#
# Handles:
#   - Connection management (with pooling)
#   - Inserting master data (customers, products, stores)
#   - Inserting transformed order records
#   - Querying analytics data for dashboard
#   - Upsert operations (insert or update)
# ============================================================

import sys
import os
import json
import psycopg2
import psycopg2.extras   # For execute_values (bulk insert)
from psycopg2 import pool
import pandas as pd
from loguru import logger
from datetime import datetime, date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DB_CONFIG


# ============================================================
# CONNECTION POOL
# Pool maintains multiple open connections → reuses them
# Much faster than opening a new connection per query
# ============================================================

class DatabaseConnector:
    """
    PostgreSQL connector with connection pooling.

    Connection pooling explained:
    - Without pool: every query opens + closes a connection (slow, expensive)
    - With pool: connections are kept open and reused (fast, efficient)
    - minconn=2: always keep 2 connections ready
    - maxconn=10: allow up to 10 simultaneous connections
    """

    def __init__(self):
        self._pool = None
        self._initialize_pool()

    def _initialize_pool(self):
        """Create connection pool on startup."""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn = 2,
                maxconn = 10,
                host     = DB_CONFIG["host"],
                port     = DB_CONFIG["port"],
                database = DB_CONFIG["database"],
                user     = DB_CONFIG["user"],
                password = DB_CONFIG["password"]
            )
            logger.success("✅ PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool."""
        return self._pool.getconn()

    def return_connection(self, conn):
        """Return connection back to pool after use."""
        self._pool.putconn(conn)

    def close_all(self):
        """Close all connections in pool (call on shutdown)."""
        self._pool.closeall()
        logger.info("All DB connections closed")


    # ============================================================
    # MASTER DATA OPERATIONS
    # ============================================================

    def insert_customers(self, customers: list) -> int:
        """
        Bulk insert customers into PostgreSQL.
        Uses ON CONFLICT DO NOTHING to handle duplicates safely.

        Args:
            customers: List of customer dicts from Faker generator

        Returns:
            int: Number of rows inserted
        """
        if not customers:
            return 0

        sql = """
            INSERT INTO customers
                (customer_id, name, email, phone, city, state,
                 region, age, gender, created_at)
            VALUES %s
            ON CONFLICT (customer_id) DO NOTHING
        """

        # Build list of tuples for execute_values
        values = [
            (
                c["customer_id"], c["name"], c["email"],
                c["phone"], c["city"], c["state"],
                c["region"], c["age"], c["gender"],
                c["created_at"]
            )
            for c in customers
        ]

        return self._bulk_insert(sql, values, "customers")


    def insert_products(self, products: list) -> int:
        """Bulk insert products into PostgreSQL."""
        if not products:
            return 0

        sql = """
            INSERT INTO products
                (product_id, name, category, brand, original_price,
                 discounted_price, discount_pct, rating, stock_quantity)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """

        values = [
            (
                p["product_id"], p["name"], p["category"],
                p["brand"], p["original_price"], p["discounted_price"],
                p["discount_pct"], p["rating"], p["stock_quantity"]
            )
            for p in products
        ]

        return self._bulk_insert(sql, values, "products")


    def insert_stores(self, stores: list) -> int:
        """Bulk insert stores into PostgreSQL."""
        if not stores:
            return 0

        sql = """
            INSERT INTO stores
                (store_id, store_name, city, region, store_type)
            VALUES %s
            ON CONFLICT (store_id) DO NOTHING
        """

        values = [
            (
                s["store_id"], s["store_name"], s["city"],
                s["region"], s["store_type"]
            )
            for s in stores
        ]

        return self._bulk_insert(sql, values, "stores")


    # ============================================================
    # ORDER OPERATIONS (Core Pipeline)
    # ============================================================

    def insert_orders(self, orders: list) -> int:
        """
        Bulk insert transformed order records into PostgreSQL.
        Called by Kafka consumer after Pandas transformation.

        Args:
            orders: List of transformed order dicts

        Returns:
            int: Number of rows inserted
        """
        if not orders:
            return 0

        sql = """
            INSERT INTO orders (
                order_id, order_timestamp, event_timestamp,
                customer_id, product_id, store_id,
                customer_city, customer_region, customer_age, customer_gender,
                product_name, category, brand,
                store_type, store_city, store_region,
                quantity, unit_price, total_amount, tax_amount, final_amount,
                payment_method, order_status,
                is_first_order, device_type, source
            )
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """

        values = [
            (
                o["order_id"],
                o["order_timestamp"],
                o["event_timestamp"],
                o.get("customer_id"),
                o.get("product_id"),
                o.get("store_id"),
                o.get("customer_city"),
                o.get("customer_region"),
                o.get("customer_age"),
                o.get("customer_gender"),
                o.get("product_name"),
                o.get("category"),
                o.get("brand"),
                o.get("store_type"),
                o.get("store_city"),
                o.get("store_region"),
                o.get("quantity"),
                o.get("unit_price"),
                o.get("total_amount"),
                o.get("tax_amount"),
                o.get("final_amount"),
                o.get("payment_method"),
                o.get("order_status"),
                o.get("is_first_order", False),
                o.get("device_type"),
                o.get("source")
            )
            for o in orders
        ]

        count = self._bulk_insert(sql, values, "orders")
        logger.info(f"📥 Inserted {count} orders into PostgreSQL")
        return count


    # ============================================================
    # ANALYTICS QUERIES (Used by Streamlit Dashboard)
    # ============================================================

    def get_total_stats(self) -> dict:
        """Get high-level KPI stats for dashboard header."""
        sql = """
            SELECT
                COUNT(*)                        AS total_orders,
                COALESCE(SUM(final_amount), 0)  AS total_revenue,
                COALESCE(AVG(final_amount), 0)  AS avg_order_value,
                COUNT(DISTINCT customer_id)     AS unique_customers
            FROM orders
            WHERE order_status NOT IN ('Cancelled', 'Returned')
        """
        result = self._fetch_one(sql)
        return {
            "total_orders"    : int(result[0] or 0),
            "total_revenue"   : float(result[1] or 0),
            "avg_order_value" : float(result[2] or 0),
            "unique_customers": int(result[3] or 0)
        }


    def get_revenue_by_category(self) -> pd.DataFrame:
        """Revenue breakdown by product category."""
        sql = """SELECT * FROM vw_revenue_by_category"""
        return self._fetch_dataframe(sql)


    def get_revenue_by_region(self) -> pd.DataFrame:
        """Revenue breakdown by customer region."""
        sql = """SELECT * FROM vw_revenue_by_region"""
        return self._fetch_dataframe(sql)


    def get_daily_revenue_trend(self) -> pd.DataFrame:
        """Daily revenue trend for last 14 days."""
        sql = """SELECT * FROM vw_daily_revenue_trend"""
        return self._fetch_dataframe(sql)


    def get_top_products(self) -> pd.DataFrame:
        """Top 10 products by revenue."""
        sql = """SELECT * FROM vw_top_products"""
        return self._fetch_dataframe(sql)


    def get_payment_distribution(self) -> pd.DataFrame:
        """Payment method breakdown."""
        sql = """SELECT * FROM vw_payment_distribution"""
        return self._fetch_dataframe(sql)


    def get_recent_orders(self, limit: int = 20) -> pd.DataFrame:
        """Get most recent orders for live feed table."""
        sql = f"""
            SELECT
                order_id,
                order_timestamp,
                customer_city,
                category,
                final_amount,
                payment_method,
                order_status
            FROM orders
            ORDER BY order_timestamp DESC
            LIMIT {limit}
        """
        return self._fetch_dataframe(sql)


    def get_hourly_orders_today(self) -> pd.DataFrame:
        """Orders count per hour for today."""
        sql = """
            SELECT
                EXTRACT(HOUR FROM order_timestamp)  AS hour,
                COUNT(*)                            AS order_count,
                SUM(final_amount)                   AS revenue
            FROM orders
            WHERE DATE(order_timestamp) = CURRENT_DATE
            GROUP BY EXTRACT(HOUR FROM order_timestamp)
            ORDER BY hour
        """
        return self._fetch_dataframe(sql)


    # ============================================================
    # DAILY SUMMARY UPSERT (Called by Airflow DAG)
    # ============================================================

    def upsert_daily_summary(self, summary_date: date = None):
        """
        Compute and upsert daily summary aggregations.
        Airflow calls this every hour.

        UPSERT = INSERT + UPDATE:
        - If row exists for that date → UPDATE it
        - If not → INSERT new row
        """
        if summary_date is None:
            summary_date = date.today()

        sql = """
            INSERT INTO daily_summary (
                summary_date, category, region,
                total_orders, total_revenue, total_tax,
                avg_order_value, total_quantity, unique_customers,
                new_customers, delivered_count, cancelled_count,
                returned_count, upi_count, card_count, cod_count
            )
            SELECT
                DATE(order_timestamp)               AS summary_date,
                category,
                customer_region                     AS region,
                COUNT(*)                            AS total_orders,
                SUM(final_amount)                   AS total_revenue,
                SUM(tax_amount)                     AS total_tax,
                ROUND(AVG(final_amount), 2)         AS avg_order_value,
                SUM(quantity)                       AS total_quantity,
                COUNT(DISTINCT customer_id)         AS unique_customers,
                SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END) AS new_customers,
                SUM(CASE WHEN order_status='Delivered'  THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN order_status='Cancelled'  THEN 1 ELSE 0 END) AS cancelled_count,
                SUM(CASE WHEN order_status='Returned'   THEN 1 ELSE 0 END) AS returned_count,
                SUM(CASE WHEN payment_method='UPI'              THEN 1 ELSE 0 END) AS upi_count,
                SUM(CASE WHEN payment_method IN ('Credit Card','Debit Card') THEN 1 ELSE 0 END) AS card_count,
                SUM(CASE WHEN payment_method='Cash on Delivery' THEN 1 ELSE 0 END) AS cod_count
            FROM orders
            WHERE DATE(order_timestamp) = %s
            GROUP BY DATE(order_timestamp), category, customer_region
            ON CONFLICT (summary_date, category, region)
            DO UPDATE SET
                total_orders     = EXCLUDED.total_orders,
                total_revenue    = EXCLUDED.total_revenue,
                total_tax        = EXCLUDED.total_tax,
                avg_order_value  = EXCLUDED.avg_order_value,
                total_quantity   = EXCLUDED.total_quantity,
                unique_customers = EXCLUDED.unique_customers,
                new_customers    = EXCLUDED.new_customers,
                delivered_count  = EXCLUDED.delivered_count,
                cancelled_count  = EXCLUDED.cancelled_count,
                returned_count   = EXCLUDED.returned_count,
                upi_count        = EXCLUDED.upi_count,
                card_count       = EXCLUDED.card_count,
                cod_count        = EXCLUDED.cod_count,
                computed_at      = CURRENT_TIMESTAMP
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (summary_date,))
                conn.commit()
                logger.success(f"✅ Daily summary upserted for {summary_date}")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Daily summary upsert failed: {e}")
            raise
        finally:
            self.return_connection(conn)


    # ============================================================
    # INTERNAL UTILITY METHODS
    # ============================================================

    def _bulk_insert(self, sql: str, values: list, table_name: str) -> int:
        """
        Execute a bulk INSERT using psycopg2.extras.execute_values.
        Much faster than inserting row by row.

        execute_values sends all rows in ONE query instead of N queries.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, sql, values,
                    page_size=100   # Send 100 rows per DB round-trip
                )
                count = cur.rowcount
                conn.commit()
                logger.debug(f"   Inserted {count} rows into {table_name}")
                return count if count != -1 else len(values)
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Bulk insert into {table_name} failed: {e}")
            raise
        finally:
            self.return_connection(conn)


    def _fetch_one(self, sql: str, params=None):
        """Execute SELECT and return single row."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()
        finally:
            self.return_connection(conn)


    def _fetch_dataframe(self, sql: str, params=None) -> pd.DataFrame:
        """
        Execute SELECT and return result as Pandas DataFrame.
        Used by Streamlit dashboard for easy visualization.
        """
        conn = self.get_connection()
        try:
            return pd.read_sql_query(sql, conn, params=params)
        finally:
            self.return_connection(conn)


    def health_check(self) -> bool:
        """Verify DB connection is alive."""
        try:
            result = self._fetch_one("SELECT 1")
            return result[0] == 1
        except Exception:
            return False


# ============================================================
# STANDALONE TEST
# python database/db_connector.py
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  🧪 DB CONNECTOR — TEST RUN")
    print("=" * 50)

    db = DatabaseConnector()

    print(f"\n🔍 Health Check: {db.health_check()}")

    print("\n📊 Total Stats:")
    stats = db.get_total_stats()
    for k, v in stats.items():
        print(f"   {k}: {v}")

    db.close_all()
    print("\n✅ DB Connector working correctly!")

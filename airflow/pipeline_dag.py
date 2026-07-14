# ============================================================
# airflow/pipeline_dag.py
# Apache Airflow DAG — E-Commerce Pipeline Orchestrator
#
# What this DAG does:
#   1. Checks Kafka & PostgreSQL are healthy (sensors)
#   2. Triggers Kafka consumer to process pending messages
#   3. Runs data quality checks on orders table
#   4. Computes and upserts daily summary aggregations
#   5. Sends pipeline success/failure notification (log)
#
# Schedule: Every hour (@hourly)
# DAG ID  : ecommerce_pipeline_dag
#
# How to run:
#   1. Start Airflow: airflow standalone
#   2. Open http://localhost:8080
#   3. Enable DAG: ecommerce_pipeline_dag
#   4. Trigger manually or wait for schedule
# ============================================================

from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python   import PythonOperator
from airflow.operators.bash     import BashOperator
from airflow.operators.empty    import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config       import AIRFLOW_CONFIG, KAFKA_CONFIG, DB_CONFIG
from database.db_connector import DatabaseConnector
from transforms.transformer import OrderTransformer


# ============================================================
# DAG DEFAULT ARGUMENTS
# Applied to every task in the DAG unless overridden
# ============================================================

default_args = {
    "owner"            : AIRFLOW_CONFIG["owner"],
    "depends_on_past"  : False,      # Each run is independent
    "start_date"       : datetime(2026, 1, 1),
    "email_on_failure" : False,       # Set True + add email in production
    "email_on_retry"   : False,
    "retries"          : 2,           # Retry failed task 2 times
    "retry_delay"      : timedelta(minutes=2),  # Wait 2 min before retry
    "execution_timeout": timedelta(minutes=30), # Task timeout
}


# ============================================================
# TASK FUNCTIONS
# Each function = one task node in the DAG
# ============================================================

# ── Task 1: Check PostgreSQL Health ─────────────────────────
def check_postgres_health(**context):
    """
    Verifies PostgreSQL is reachable and ecommerce_db is accessible.
    If this fails → entire DAG stops (upstream failure).
    """
    logger.info("🔍 Task 1: Checking PostgreSQL health...")

    db = DatabaseConnector()
    is_healthy = db.health_check()
    db.close_all()

    if not is_healthy:
        raise Exception("❌ PostgreSQL health check FAILED — DB unreachable!")

    logger.success("✅ PostgreSQL is healthy and reachable")

    # Push result to XCom so downstream tasks can access it
    context['ti'].xcom_push(key='postgres_status', value='healthy')
    return "postgres_healthy"


# ── Task 2: Check Kafka Health ───────────────────────────────
def check_kafka_health(**context):
    """
    Verifies Kafka broker is reachable.
    Uses kafka-python's KafkaAdminClient for health check.
    """
    logger.info("🔍 Task 2: Checking Kafka health...")

    try:
        from kafka_service import KafkaConsumer
        from kafka_service.errors import NoBrokersAvailable

        # Try connecting — if no brokers → exception raised
        test_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            consumer_timeout_ms=5000
        )
        test_consumer.topics()   # Fetch topics list → confirms broker is alive
        test_consumer.close()

        logger.success("✅ Kafka broker is healthy and reachable")
        context['ti'].xcom_push(key='kafka_status', value='healthy')
        return "kafka_healthy"

    except Exception as e:
        raise Exception(f"❌ Kafka health check FAILED: {e}")


# ── Task 3: Process Kafka Messages ───────────────────────────
def process_kafka_messages(**context):
    """
    Runs Kafka consumer for a fixed duration (5 minutes).
    Reads pending messages → transforms → loads to PostgreSQL.

    Why time-limited?
    - DAG tasks must complete and return control to Airflow
    - We run consumer for 5 min per hour to drain the queue
    - In production, consumer would run as a separate always-on service
    """
    logger.info("🔄 Task 3: Processing Kafka messages (5 min window)...")

    from kafka_service import KafkaConsumer
    import json
    import time

    def json_deserializer(data):
        return json.loads(data.decode('utf-8'))

    db = DatabaseConnector()
    transformer = OrderTransformer()

    consumer = KafkaConsumer(
        KAFKA_CONFIG["topic_orders"],
        bootstrap_servers  = KAFKA_CONFIG["bootstrap_servers"],
        group_id           = KAFKA_CONFIG["group_id"] + "_airflow",
        value_deserializer = json_deserializer,
        auto_offset_reset  = "earliest",
        enable_auto_commit = False,
        max_poll_records   = 200,
        consumer_timeout_ms= 5000   # Stop if no messages for 5 seconds
    )

    total_consumed = 0
    total_inserted = 0
    start_time     = time.time()
    max_duration   = 300  # 5 minutes

    try:
        while time.time() - start_time < max_duration:
            message_pack = consumer.poll(timeout_ms=5000)

            if not message_pack:
                logger.info("   No new messages — Kafka queue is empty")
                break

            all_messages = []
            for tp, messages in message_pack.items():
                all_messages.extend(messages)

            if not all_messages:
                break

            raw_orders = [msg.value for msg in all_messages]
            transformed_orders, invalid = transformer.transform(raw_orders)

            if transformed_orders:
                inserted = db.insert_orders(transformed_orders)
                total_inserted += inserted
                consumer.commit()

            total_consumed += len(all_messages)

            logger.info(
                f"   Consumed: {total_consumed} | "
                f"Inserted: {total_inserted} | "
                f"Elapsed: {int(time.time()-start_time)}s"
            )

    finally:
        consumer.close()
        db.close_all()

    logger.success(
        f"✅ Kafka processing complete → "
        f"Consumed: {total_consumed} | Inserted: {total_inserted}"
    )

    # Push stats to XCom for downstream tasks
    context['ti'].xcom_push(key='orders_consumed', value=total_consumed)
    context['ti'].xcom_push(key='orders_inserted', value=total_inserted)

    return {
        "consumed": total_consumed,
        "inserted": total_inserted
    }


# ── Task 4: Data Quality Checks ──────────────────────────────
def run_data_quality_checks(**context):
    """
    Runs SQL-based data quality checks on the orders table.

    Checks performed:
    1. No null order_ids
    2. No negative amounts
    3. No future-dated orders
    4. Acceptable cancellation rate (< 40%)
    5. Orders exist for today

    If any check fails → raises exception → DAG marks task as FAILED
    """
    logger.info("🔍 Task 4: Running data quality checks...")

    db  = DatabaseConnector()
    conn = db.get_connection()
    failures = []

    try:
        with conn.cursor() as cur:

            # Check 1: No null order_ids
            cur.execute("SELECT COUNT(*) FROM orders WHERE order_id IS NULL")
            null_ids = cur.fetchone()[0]
            if null_ids > 0:
                failures.append(f"❌ {null_ids} orders have NULL order_id")
            else:
                logger.info("   ✅ Check 1: No NULL order_ids")

            # Check 2: No negative final amounts
            cur.execute("SELECT COUNT(*) FROM orders WHERE final_amount < 0")
            neg_amounts = cur.fetchone()[0]
            if neg_amounts > 0:
                failures.append(f"❌ {neg_amounts} orders have negative amounts")
            else:
                logger.info("   ✅ Check 2: No negative amounts")

            # Check 3: No future-dated orders (more than 1 day in future)
            cur.execute("""
                SELECT COUNT(*) FROM orders
                WHERE order_timestamp > NOW() + INTERVAL '1 day'
            """)
            future_orders = cur.fetchone()[0]
            if future_orders > 0:
                failures.append(f"❌ {future_orders} orders have future timestamps")
            else:
                logger.info("   ✅ Check 3: No future-dated orders")

            # Check 4: Cancellation rate < 40%
            cur.execute("""
                SELECT
                    ROUND(
                        COUNT(CASE WHEN order_status='Cancelled' THEN 1 END) * 100.0
                        / NULLIF(COUNT(*), 0),
                    2) AS cancel_rate
                FROM orders
                WHERE DATE(order_timestamp) = CURRENT_DATE
            """)
            cancel_rate = cur.fetchone()[0] or 0
            if float(cancel_rate) > 40:
                failures.append(
                    f"❌ Cancellation rate {cancel_rate}% exceeds 40% threshold"
                )
            else:
                logger.info(f"   ✅ Check 4: Cancellation rate OK ({cancel_rate}%)")

            # Check 5: Orders exist in DB
            cur.execute("SELECT COUNT(*) FROM orders")
            total_orders = cur.fetchone()[0]
            if total_orders == 0:
                failures.append("❌ Orders table is empty — pipeline may not be running")
            else:
                logger.info(f"   ✅ Check 5: {total_orders:,} total orders in DB")

    finally:
        db.return_connection(conn)
        db.close_all()

    # Raise exception if any check failed
    if failures:
        error_msg = "\n".join(failures)
        raise Exception(f"Data quality checks FAILED:\n{error_msg}")

    logger.success("✅ All data quality checks PASSED")

    # Push result to XCom
    context['ti'].xcom_push(key='dq_status', value='passed')
    return "dq_passed"


# ── Task 5: Compute Daily Summary ────────────────────────────
def compute_daily_summary(**context):
    """
    Aggregates today's order data into daily_summary table.
    Uses UPSERT so it can be run multiple times safely.

    Called every hour by Airflow → summary is always fresh.
    Streamlit dashboard reads from daily_summary for fast queries.
    """
    logger.info("🔄 Task 5: Computing daily summary aggregations...")

    from datetime import date

    db = DatabaseConnector()

    # Upsert today's summary
    db.upsert_daily_summary(summary_date=date.today())

    # Verify
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)          AS categories,
                    SUM(total_orders) AS total_orders,
                    SUM(total_revenue)AS total_revenue
                FROM daily_summary
                WHERE summary_date = CURRENT_DATE
            """)
            row = cur.fetchone()
            logger.success(
                f"✅ Daily summary computed → "
                f"Categories: {row[0]} | "
                f"Orders: {row[1]} | "
                f"Revenue: ₹{row[2]:,.2f}"
            )
            # Push stats to XCom
            context['ti'].xcom_push(key='summary_categories', value=row[0])
            context['ti'].xcom_push(key='summary_revenue',    value=float(row[2] or 0))

    finally:
        db.return_connection(conn)
        db.close_all()

    return "summary_computed"


# ── Task 6: Pipeline Success Notification ────────────────────
def notify_success(**context):
    """
    Logs pipeline success with key metrics from XCom.
    In production: send Slack/email notification here.

    XCom (Cross-Communication) allows tasks to share data.
    Here we pull stats from previous tasks.
    """
    ti = context['ti']

    # Pull stats from XCom (set by previous tasks)
    orders_consumed = ti.xcom_pull(
        task_ids='process_kafka_messages', key='orders_consumed'
    ) or 0
    orders_inserted = ti.xcom_pull(
        task_ids='process_kafka_messages', key='orders_inserted'
    ) or 0
    summary_revenue = ti.xcom_pull(
        task_ids='compute_daily_summary', key='summary_revenue'
    ) or 0

    run_id   = context['run_id']
    exec_date = context['execution_date']

    logger.success(f"""
╔══════════════════════════════════════════════╗
║   ✅ PIPELINE RUN SUCCESS                    ║
╠══════════════════════════════════════════════╣
║  Run ID    : {run_id[:30]}...
║  Exec Date : {exec_date}
║  Consumed  : {orders_consumed:,} messages
║  Inserted  : {orders_inserted:,} orders
║  Revenue   : ₹{summary_revenue:,.2f} today
╚══════════════════════════════════════════════╝
    """)

    return "pipeline_success"


# ── Task 7: Pipeline Failure Notification ────────────────────
def notify_failure(**context):
    """
    Called if ANY upstream task fails.
    TriggerRule.ONE_FAILED ensures this runs on any failure.
    In production: send PagerDuty/Slack alert here.
    """
    exception = context.get('exception')
    run_id    = context['run_id']

    logger.error(f"""
╔══════════════════════════════════════════════╗
║   ❌ PIPELINE RUN FAILED                     ║
╠══════════════════════════════════════════════╣
║  Run ID    : {run_id[:30]}...
║  Error     : {str(exception)[:40]}
║  Action    : Check Airflow logs for details
╚══════════════════════════════════════════════╝
    """)

    return "pipeline_failed"


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id          = AIRFLOW_CONFIG["dag_id"],
    default_args    = default_args,
    description     = "E-Commerce Sales Data Pipeline — Hourly ETL",
    schedule_interval = AIRFLOW_CONFIG["schedule_interval"],  # @hourly
    catchup         = False,    # Don't backfill missed runs
    max_active_runs = 1,        # Only 1 run at a time
    tags            = ["ecommerce", "kafka", "postgresql", "etl"],
) as dag:

    # ── Task Definitions ─────────────────────────────────────

    # Start dummy task (entry point)
    start = EmptyOperator(
        task_id="pipeline_start"
    )

    # Task 1: Check PostgreSQL
    check_postgres = PythonOperator(
        task_id         = "check_postgres_health",
        python_callable = check_postgres_health,
        provide_context = True,
    )

    # Task 2: Check Kafka
    check_kafka = PythonOperator(
        task_id         = "check_kafka_health",
        python_callable = check_kafka_health,
        provide_context = True,
    )

    # Task 3: Process Kafka Messages (core ETL task)
    process_kafka = PythonOperator(
        task_id         = "process_kafka_messages",
        python_callable = process_kafka_messages,
        provide_context = True,
    )

    # Task 4: Data Quality Checks
    dq_checks = PythonOperator(
        task_id         = "run_data_quality_checks",
        python_callable = run_data_quality_checks,
        provide_context = True,
    )

    # Task 5: Compute Daily Summary
    daily_summary = PythonOperator(
        task_id         = "compute_daily_summary",
        python_callable = compute_daily_summary,
        provide_context = True,
    )

    # Task 6: Success notification
    success_notify = PythonOperator(
        task_id         = "notify_success",
        python_callable = notify_success,
        provide_context = True,
    )

    # Task 7: Failure notification (runs if ANY task fails)
    failure_notify = PythonOperator(
        task_id         = "notify_failure",
        python_callable = notify_failure,
        provide_context = True,
        trigger_rule    = TriggerRule.ONE_FAILED,  # Run on any upstream failure
    )

    # End dummy task
    end = EmptyOperator(
        task_id      = "pipeline_end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


    # ── DAG Task Dependencies (Execution Order) ──────────────
    #
    # Visual Flow:
    #
    #              ┌─→ check_postgres ─┐
    # start ───────┤                   ├──→ process_kafka
    #              └─→ check_kafka ────┘         │
    #                                            ↓
    #                                      dq_checks
    #                                            │
    #                                            ↓
    #                                      daily_summary
    #                                            │
    #                                            ↓
    #                                      success_notify
    #                                            │
    #                                            ↓
    #                                          end
    #                                           ↑
    #                                    failure_notify
    #                                  (on any task failure)

    # Health checks run in parallel after start
    start >> [check_postgres, check_kafka]

    # Both health checks must pass before processing Kafka
    [check_postgres, check_kafka] >> process_kafka

    # Sequential pipeline after Kafka processing
    process_kafka >> dq_checks >> daily_summary >> success_notify >> end

    # Failure handler triggered by any failed task
    [
        check_postgres,
        check_kafka,
        process_kafka,
        dq_checks,
        daily_summary
    ] >> failure_notify >> end

#!/bin/bash
# ============================================================
# airflow/setup_airflow.sh
# One-time Airflow setup script
#
# Run this ONCE before starting Airflow:
#   bash airflow/setup_airflow.sh
# ============================================================

echo "============================================"
echo "  Setting up Apache Airflow"
echo "============================================"

# Set Airflow home to project directory
export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins

echo "✅ Airflow home: $AIRFLOW_HOME"

# Initialize Airflow database (SQLite for local dev)
echo ""
echo "🔄 Initializing Airflow database..."
airflow db init

# Copy DAG file to Airflow dags folder
echo ""
echo "🔄 Copying DAG to Airflow..."
cp airflow/pipeline_dag.py $AIRFLOW_HOME/dags/

# Create admin user for Airflow web UI
echo ""
echo "🔄 Creating Airflow admin user..."
airflow users create \
    --username admin \
    --password admin123 \
    --firstname Jaymin \
    --lastname Chavda \
    --role Admin \
    --email jaymin29chavda@gmail.com

echo ""
echo "============================================"
echo "  ✅ Airflow setup complete!"
echo "============================================"
echo ""
echo "  Start Airflow with:"
echo "  airflow standalone"
echo ""
echo "  Then open: http://localhost:8080"
echo "  Username : admin"
echo "  Password : admin123"
echo "============================================"

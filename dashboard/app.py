# ============================================================
# dashboard/app.py
# Streamlit Analytics Dashboard — E-Commerce Sales Pipeline
#
# What this shows:
#   1. KPI Header    — Total Orders, Revenue, Avg Order Value, Customers
#   2. Revenue Trend — Daily revenue line chart (last 14 days)
#   3. Category Mix  — Bar + Pie charts by product category
#   4. Regional Map  — Revenue by Indian region
#   5. Hourly Pattern— Orders per hour heatmap
#   6. Top Products  — Bar chart of top 10 products
#   7. Payment Split — Donut chart of payment methods
#   8. Live Feed     — Real-time recent orders table
#   9. Order Status  — Funnel chart (placed→delivered)
#
# Run with:
#   streamlit run dashboard/app.py
# ============================================================

import sys
import os
import time
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config    import DASHBOARD_CONFIG
from database.db_connector import DatabaseConnector


# ============================================================
# PAGE CONFIGURATION
# Must be first Streamlit command
# ============================================================

st.set_page_config(
    page_title = DASHBOARD_CONFIG["title"],
    page_icon  = DASHBOARD_CONFIG["page_icon"],
    layout     = "wide",         # Use full screen width
    initial_sidebar_state = "expanded"
)


# ============================================================
# CUSTOM CSS — Premium Dark Theme
# ============================================================

st.markdown("""
<style>
    /* Dark background */
    .stApp {
        background-color: #0e1117;
        color: #ffffff;
    }

    /* KPI Card styling */
    .kpi-card {
        background: linear-gradient(135deg, #1e2130, #252a3a);
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #4fc3f7;
        margin: 8px 0;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #9aa0b4;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .kpi-delta {
        font-size: 0.9rem;
        color: #66bb6a;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #4fc3f7;
        border-left: 3px solid #4fc3f7;
        padding-left: 10px;
        margin: 20px 0 10px 0;
    }

    /* Live badge */
    .live-badge {
        background: #e53935;
        color: white;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.75rem;
        font-weight: bold;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0%   { opacity: 1; }
        50%  { opacity: 0.5; }
        100% { opacity: 1; }
    }

    /* Sidebar */
    .css-1d391kg { background-color: #141824; }

    /* Hide default Streamlit menu */
    #MainMenu { visibility: hidden; }
    footer     { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATABASE CONNECTION (cached — reuses connection across reruns)
# ============================================================

@st.cache_resource
def get_db():
    """
    Cache DB connection across Streamlit reruns.
    st.cache_resource: created once, shared across all users/sessions.
    """
    return DatabaseConnector()


# ============================================================
# DATA FETCHING FUNCTIONS (cached with TTL)
# ============================================================

@st.cache_data(ttl=30)   # Refresh every 30 seconds
def fetch_total_stats():
    db = get_db()
    return db.get_total_stats()

@st.cache_data(ttl=30)
def fetch_revenue_by_category():
    db = get_db()
    return db.get_revenue_by_category()

@st.cache_data(ttl=30)
def fetch_revenue_by_region():
    db = get_db()
    return db.get_revenue_by_region()

@st.cache_data(ttl=30)
def fetch_daily_revenue_trend():
    db = get_db()
    return db.get_daily_revenue_trend()

@st.cache_data(ttl=30)
def fetch_top_products():
    db = get_db()
    return db.get_top_products()

@st.cache_data(ttl=30)
def fetch_payment_distribution():
    db = get_db()
    return db.get_payment_distribution()

@st.cache_data(ttl=10)   # Live feed refreshes every 10 seconds
def fetch_recent_orders():
    db = get_db()
    return db.get_recent_orders(limit=15)

@st.cache_data(ttl=30)
def fetch_hourly_orders():
    db = get_db()
    return db.get_hourly_orders_today()


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def format_currency(value: float) -> str:
    """Format number as Indian currency with ₹ symbol."""
    if value >= 10_000_000:   # 1 Crore+
        return f"₹{value/10_000_000:.1f}Cr"
    elif value >= 100_000:    # 1 Lakh+
        return f"₹{value/100_000:.1f}L"
    elif value >= 1000:
        return f"₹{value/1000:.1f}K"
    else:
        return f"₹{value:.0f}"

def format_number(value: int) -> str:
    """Format large number with K/L/Cr suffix."""
    if value >= 100_000:
        return f"{value/100_000:.1f}L"
    elif value >= 1000:
        return f"{value/1000:.1f}K"
    else:
        return str(value)

# Plotly dark theme template
PLOT_THEME = {
    "template": "plotly_dark",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(30,33,48,0.8)",
    "font": {
        "color": "#c9cdd4",
        "family": "Inter, sans-serif"
    },
    "margin": {
        "l": 20,
        "r": 20,
        "t": 40,
        "b": 20
    },
    "legend": {
        "bgcolor": "rgba(0,0,0,0)",
        "font": {
            "color": "#c9cdd4"
        }
    },
    "hovermode": "x unified"
}

COLOR_PALETTE = [
    "#4fc3f7", "#66bb6a", "#ffa726", "#ef5350",
    "#ab47bc", "#26c6da", "#ffee58", "#ff7043"
]


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.markdown("## 🛒 E-Commerce Pipeline")
    st.markdown("---")

    st.markdown("### ⚙️ Dashboard Settings")

    auto_refresh = st.toggle("Auto Refresh (30s)", value=True)
    show_raw     = st.toggle("Show Raw Data Tables", value=False)

    st.markdown("---")
    st.markdown("### 📊 Pipeline Status")

    try:
        db_status = get_db().health_check()
        if db_status:
            st.success("🟢 PostgreSQL: Connected")
        else:
            st.error("🔴 PostgreSQL: Disconnected")
    except:
        st.error("🔴 PostgreSQL: Disconnected")

    st.info("🟡 Kafka: Streaming")
    st.success("🟢 Airflow: Scheduled (@hourly)")

    st.markdown("---")
    st.markdown("### 🔗 Pipeline Stack")
    st.markdown("""
    - 🎲 **Faker** — Data Generation
    - 📨 **Apache Kafka** — Streaming
    - 🐼 **Pandas** — Transformation
    - 🗄️ **PostgreSQL** — Storage
    - 🌬️ **Apache Airflow** — Orchestration
    - 📊 **Streamlit** — Dashboard
    """)

    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")


# ============================================================
# MAIN DASHBOARD
# ============================================================

# ── Header ───────────────────────────────────────────────────
col_title, col_badge = st.columns([6, 1])
with col_title:
    st.markdown("# 🛒 E-Commerce Sales Analytics")
    st.markdown("**Real-Time Data Pipeline Dashboard** — Kafka → PostgreSQL → Streamlit")
with col_badge:
    st.markdown('<br><span class="live-badge">● LIVE</span>', unsafe_allow_html=True)

st.markdown("---")


# ============================================================
# SECTION 1: KPI CARDS
# ============================================================

st.markdown('<p class="section-header">📈 Key Performance Indicators</p>',
            unsafe_allow_html=True)

try:
    stats = fetch_total_stats()

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    with kpi1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Orders</div>
            <div class="kpi-value">{format_number(stats['total_orders'])}</div>
            <div class="kpi-delta">↑ Live updating</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Revenue</div>
            <div class="kpi-value">{format_currency(stats['total_revenue'])}</div>
            <div class="kpi-delta">↑ Excl. Cancelled</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Avg Order Value</div>
            <div class="kpi-value">{format_currency(stats['avg_order_value'])}</div>
            <div class="kpi-delta">Per transaction</div>
        </div>
        """, unsafe_allow_html=True)

    with kpi4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Unique Customers</div>
            <div class="kpi-value">{format_number(stats['unique_customers'])}</div>
            <div class="kpi-delta">Active buyers</div>
        </div>
        """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ Could not load KPIs: {e}")

st.markdown("<br>", unsafe_allow_html=True)


# ============================================================
# SECTION 2: REVENUE TREND + HOURLY PATTERN
# ============================================================

st.markdown('<p class="section-header">📉 Revenue Trends</p>',
            unsafe_allow_html=True)

col_trend, col_hourly = st.columns([3, 2])

with col_trend:
    try:
        df_trend = fetch_daily_revenue_trend()

        if not df_trend.empty:
            fig_trend = go.Figure()

            # Revenue bars
            fig_trend.add_trace(go.Bar(
                x    = df_trend['order_date'],
                y    = df_trend['daily_revenue'],
                name = 'Daily Revenue',
                marker_color = '#4fc3f7',
                opacity      = 0.8
            ))

            # Order count line (secondary axis)
            fig_trend.add_trace(go.Scatter(
                x    = df_trend['order_date'],
                y    = df_trend['total_orders'],
                name = 'Order Count',
                mode = 'lines+markers',
                line = dict(color='#ffa726', width=2),
                yaxis= 'y2'
            ))
             
            theme = {k: v for k, v in PLOT_THEME.items() if k != "legend"}
            fig_trend.update_layout(
                title="Daily Revenue & Order Count (Last 14 Days)",
                yaxis=dict(title="Revenue (₹)", gridcolor="#2d3250"),
                yaxis2=dict(title="Orders", overlaying="y", side="right"),
                xaxis=dict(gridcolor="#2d3250"),
                legend=dict(
                    x=0,
                    y=1,
                    bgcolor="rgba(0,0,0,0)"
                ),
                height=320,
                **theme
            )

            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("📊 Waiting for data...")

    except Exception as e:
        st.error(f"❌ Revenue trend error: {e}")

with col_hourly:
    try:
        df_hourly = fetch_hourly_orders()

        if not df_hourly.empty:
            fig_hourly = go.Figure(go.Bar(
                x            = df_hourly['hour'],
                y            = df_hourly['order_count'],
                marker_color = COLOR_PALETTE,
                text         = df_hourly['order_count'],
                textposition = 'outside'
            ))

            fig_hourly.update_layout(
                title  = "Orders by Hour (Today)",
                xaxis  = dict(title='Hour of Day', gridcolor='#2d3250',
                              tickmode='linear'),
                yaxis  = dict(title='Orders', gridcolor='#2d3250'),
                height = 320,
                **PLOT_THEME
            )

            st.plotly_chart(fig_hourly, use_container_width=True)
        else:
            st.info("📊 No orders today yet...")

    except Exception as e:
        st.error(f"❌ Hourly chart error: {e}")


# ============================================================
# SECTION 3: CATEGORY & REGION ANALYSIS
# ============================================================

st.markdown('<p class="section-header">🏷️ Category & Regional Performance</p>',
            unsafe_allow_html=True)

col_cat_bar, col_cat_pie, col_region = st.columns([2, 1.5, 1.5])

with col_cat_bar:
    try:
        df_cat = fetch_revenue_by_category()

        if not df_cat.empty:
            fig_cat = go.Figure(go.Bar(
                x            = df_cat['total_revenue'],
                y            = df_cat['category'],
                orientation  = 'h',
                marker_color = COLOR_PALETTE[:len(df_cat)],
                text         = df_cat['total_revenue'].apply(format_currency),
                textposition = 'outside'
            ))

            fig_cat.update_layout(
                title  = "Revenue by Category",
                xaxis  = dict(title='Revenue (₹)', gridcolor='#2d3250'),
                yaxis  = dict(gridcolor='#2d3250', autorange='reversed'),
                height = 320,
                **PLOT_THEME
            )

            st.plotly_chart(fig_cat, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Category chart error: {e}")

with col_cat_pie:
    try:
        df_cat = fetch_revenue_by_category()

        if not df_cat.empty:
            fig_pie = go.Figure(go.Pie(
                labels   = df_cat['category'],
                values   = df_cat['total_orders'],
                hole     = 0.45,
                marker   = dict(colors=COLOR_PALETTE),
                textinfo = 'percent',
                hoverinfo= 'label+value+percent'
            ))

            theme = {
                k: v
                for k, v in PLOT_THEME.items()
                if k not in ("xaxis", "yaxis", "legend")
            }

            fig_pie.update_layout(
                    title="Order Share by Category",
                    height=320,
                    showlegend=True,
                    legend=dict(
                        font=dict(size=9),
                        bgcolor="rgba(0,0,0,0)"
                    ),
                **theme
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Category pie error: {e}")

with col_region:
    try:
        df_region = fetch_revenue_by_region()

        if not df_region.empty:
            fig_region = go.Figure(go.Bar(
                x            = df_region['total_revenue'],
                y            = df_region['region'],
                orientation  = 'h',
                marker_color = ["#4fc3f7","#66bb6a","#ffa726","#ef5350","#ab47bc"],
                text         = df_region['total_revenue'].apply(format_currency),
                textposition = 'outside'
            ))

            fig_region.update_layout(
                title  = "Revenue by Region",
                xaxis  = dict(title='Revenue (₹)', gridcolor='#2d3250'),
                yaxis  = dict(gridcolor='#2d3250'),
                height = 320,
                **PLOT_THEME
            )

            st.plotly_chart(fig_region, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Region chart error: {e}")


# ============================================================
# SECTION 4: TOP PRODUCTS & PAYMENT METHODS
# ============================================================

st.markdown('<p class="section-header">🏆 Top Products & Payment Insights</p>',
            unsafe_allow_html=True)

col_products, col_payment = st.columns([3, 2])

with col_products:
    try:
        df_products = fetch_top_products()

        if not df_products.empty:
            fig_products = go.Figure(go.Bar(
                x            = df_products['total_revenue'],
                y            = df_products['product_name'].str[:30],
                orientation  = 'h',
                marker_color = '#4fc3f7',
                opacity      = 0.85,
                text         = df_products['total_revenue'].apply(format_currency),
                textposition = 'outside'
            ))

            fig_products.update_layout(
                title  = "Top 10 Products by Revenue",
                xaxis  = dict(title='Revenue (₹)', gridcolor='#2d3250'),
                yaxis  = dict(autorange='reversed', gridcolor='#2d3250'),
                height = 380,
                **PLOT_THEME
            )

            st.plotly_chart(fig_products, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Top products error: {e}")

with col_payment:
    try:
        df_payment = fetch_payment_distribution()

        if not df_payment.empty:
            # Donut chart for payment methods
            fig_payment = go.Figure(go.Pie(
                labels   = df_payment['payment_method'],
                values   = df_payment['order_count'],
                hole     = 0.55,
                marker   = dict(colors=COLOR_PALETTE),
                textinfo = 'label+percent',
                textfont = dict(size=10),
                hovertemplate = "<b>%{label}</b><br>Orders: %{value}<br>Share: %{percent}<extra></extra>"
            ))

            # Center annotation
            fig_payment.add_annotation(
                text     = "Payment<br>Methods",
                x=0.5, y=0.5,
                font     = dict(size=13, color='#9aa0b4'),
                showarrow= False
            )

            fig_payment.update_layout(
                title  = "Payment Method Distribution",
                height = 380,
                showlegend = False,
                **{k:v for k,v in PLOT_THEME.items()
                   if k not in ['xaxis','yaxis']}
            )

            st.plotly_chart(fig_payment, use_container_width=True)

            # Show table below donut
            st.dataframe(
                df_payment.rename(columns={
                    'payment_method': 'Method',
                    'order_count'   : 'Orders',
                    'percentage'    : 'Share %'
                }),
                hide_index    = True,
                use_container_width = True
            )

    except Exception as e:
        st.error(f"❌ Payment chart error: {e}")


# ============================================================
# SECTION 5: LIVE ORDER FEED
# ============================================================

st.markdown('<p class="section-header">⚡ Live Order Feed <span class="live-badge">LIVE</span></p>',
            unsafe_allow_html=True)

try:
    df_recent = fetch_recent_orders()

    if not df_recent.empty:
        # Format columns for display
        df_display = df_recent.copy()
        df_display['order_id']       = df_display['order_id'].str[:8] + "..."
        df_display['final_amount']   = df_display['final_amount'].apply(
            lambda x: f"₹{x:,.2f}"
        )
        df_display['order_timestamp'] = pd.to_datetime(
            df_display['order_timestamp']
        ).dt.strftime('%d %b %H:%M')

        # Color code order status
        def style_status(val):
            colors = {
                'Delivered' : 'color: #66bb6a',
                'Cancelled' : 'color: #ef5350',
                'Returned'  : 'color: #ffa726',
                'Shipped'   : 'color: #4fc3f7',
                'Placed'    : 'color: #ab47bc',
                'Confirmed' : 'color: #26c6da'
            }
            return colors.get(val, '')

        df_display = df_display.rename(columns={
            'order_id'       : 'Order ID',
            'order_timestamp': 'Time',
            'customer_city'  : 'City',
            'category'       : 'Category',
            'final_amount'   : 'Amount',
            'payment_method' : 'Payment',
            'order_status'   : 'Status'
        })

        st.dataframe(
            df_display.style.applymap(
                style_status, subset=['Status']
            ),
            use_container_width = True,
            hide_index          = True,
            height              = 420
        )
    else:
        st.info("⏳ Waiting for orders from Kafka pipeline...")

except Exception as e:
    st.error(f"❌ Live feed error: {e}")


# ============================================================
# SECTION 6: RAW DATA (Optional — toggled in sidebar)
# ============================================================

if show_raw:
    st.markdown("---")
    st.markdown('<p class="section-header">🗃️ Raw Data Tables</p>',
                unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Category Data", "Region Data", "Top Products"])

    with tab1:
        df = fetch_revenue_by_category()
        st.dataframe(df, use_container_width=True)

    with tab2:
        df = fetch_revenue_by_region()
        st.dataframe(df, use_container_width=True)

    with tab3:
        df = fetch_top_products()
        st.dataframe(df, use_container_width=True)


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.markdown("""
<div style='text-align:center; color:#4a4f6a; font-size:0.8rem; padding:10px'>
    🛒 E-Commerce Sales Data Pipeline &nbsp;|&nbsp;
    Built with Faker · Kafka · Pandas · PostgreSQL · Airflow · Streamlit &nbsp;|&nbsp;
    Jaymin Chavda © 2026
</div>
""", unsafe_allow_html=True)


# ============================================================
# AUTO REFRESH
# ============================================================

if auto_refresh:
    time.sleep(DASHBOARD_CONFIG["refresh_seconds"])
    st.rerun()

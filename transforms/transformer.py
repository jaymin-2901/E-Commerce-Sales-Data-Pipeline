# ============================================================
# transforms/transformer.py
# Pandas Transformation Layer
#
# What this does:
#   1. Receives raw order dicts from Kafka Consumer
#   2. Loads into Pandas DataFrame
#   3. Validates, cleans, enriches data
#   4. Returns clean list of dicts ready for PostgreSQL
# ============================================================

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import ORDER_STATUSES, PAYMENT_METHODS, PRODUCT_CATEGORIES


class OrderTransformer:
    """
    Pandas-based transformation layer for raw order events.

    Pipeline:
    raw_orders (list of dicts)
        → Pandas DataFrame
        → validate columns
        → clean data types
        → remove duplicates
        → validate business rules
        → enrich data
        → return clean records
    """

    # Expected columns in every order event
    REQUIRED_COLUMNS = [
        'order_id', 'order_timestamp', 'customer_id',
        'product_id', 'store_id', 'quantity',
        'unit_price', 'final_amount', 'category', 'order_status'
    ]

    def transform(self, raw_orders: list) -> tuple:
        """
        Main transformation method.

        Args:
            raw_orders: List of raw order dicts from Kafka

        Returns:
            tuple: (valid_orders list, invalid_orders list)
        """
        if not raw_orders:
            return [], []

        logger.debug(f"🔄 Transforming {len(raw_orders)} raw orders...")

        # Step 1: Load into DataFrame
        df = pd.DataFrame(raw_orders)

        # Step 2: Run all transformations in sequence
        df = self._validate_columns(df)
        df, invalid_df = self._remove_duplicates(df)
        df = self._clean_data_types(df)
        df, rejected = self._validate_business_rules(df)
        df = self._enrich_data(df)

        # Combine invalid records
        all_invalid = []
        if not invalid_df.empty:
            all_invalid.extend(invalid_df.to_dict('records'))
        if not rejected.empty:
            all_invalid.extend(rejected.to_dict('records'))

        valid_orders = df.to_dict('records')

        logger.debug(
            f"✅ Transformation complete → "
            f"Valid: {len(valid_orders)} | "
            f"Invalid: {len(all_invalid)}"
        )

        return valid_orders, all_invalid

    def _validate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist. Fill missing with None."""
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None
                logger.warning(f"⚠️  Missing column '{col}' — filled with None")
        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> tuple:
        """Remove duplicate order_ids within same batch."""
        before = len(df)
        duplicates = df[df.duplicated(subset=['order_id'], keep='first')]
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        after = len(df)
        if before != after:
            logger.warning(f"⚠️  Removed {before - after} duplicate order_ids")
        return df, duplicates

    def _clean_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert and clean all column data types."""

        # ── Timestamps → datetime ────────────────────────────
        for col in ['order_timestamp', 'event_timestamp']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # ── Numeric columns ──────────────────────────────────
        numeric_cols = ['quantity', 'unit_price', 'total_amount',
                        'tax_amount', 'final_amount', 'customer_age']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # ── Round monetary values to 2 decimal places ────────
        for col in ['unit_price', 'total_amount', 'tax_amount', 'final_amount']:
            if col in df.columns:
                df[col] = df[col].round(2)

        # ── String columns → strip whitespace ────────────────
        str_cols = ['category', 'order_status', 'payment_method',
                    'customer_city', 'customer_region', 'store_type']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # ── Boolean ──────────────────────────────────────────
        if 'is_first_order' in df.columns:
            df['is_first_order'] = df['is_first_order'].astype(bool)

        return df

    def _validate_business_rules(self, df: pd.DataFrame) -> tuple:
        """
        Apply business validation rules.
        Rows failing validation are moved to rejected DataFrame.
        """
        mask_valid = pd.Series([True] * len(df), index=df.index)

        # Rule 1: quantity must be > 0
        if 'quantity' in df.columns:
            mask_valid &= df['quantity'].fillna(0) > 0

        # Rule 2: final_amount must be > 0
        if 'final_amount' in df.columns:
            mask_valid &= df['final_amount'].fillna(0) > 0

        # Rule 3: order_timestamp must not be null
        if 'order_timestamp' in df.columns:
            mask_valid &= df['order_timestamp'].notna()

        # Rule 4: order_id must not be null
        mask_valid &= df['order_id'].notna()

        # Rule 5: category must be a known category
        if 'category' in df.columns:
            mask_valid &= df['category'].isin(PRODUCT_CATEGORIES)

        rejected = df[~mask_valid]
        valid_df = df[mask_valid]

        if len(rejected) > 0:
            logger.warning(f"⚠️  Rejected {len(rejected)} orders (failed validation)")

        return valid_df, rejected

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add computed columns for richer analytics."""

        # Add ingestion timestamp
        df['ingested_at'] = datetime.now()

        # Revenue per unit (sanity check column)
        if 'final_amount' in df.columns and 'quantity' in df.columns:
            df['revenue_per_unit'] = (
                df['final_amount'] / df['quantity'].replace(0, np.nan)
            ).round(2)

        # Order hour (for hourly analytics)
        if 'order_timestamp' in df.columns:
            df['order_hour'] = pd.to_datetime(
                df['order_timestamp']
            ).dt.hour

        return df

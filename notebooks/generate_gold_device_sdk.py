#!/usr/bin/env python3
"""
Generate gold layer data for device SDK.
Reads silver layer and creates business-ready device data (one row per device).
This layer is used by the risk engine for device-based risk signals.
"""

import argparse
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Columns to include in gold (risk-relevant and business attributes).
# Only columns present in silver will be included.
GOLD_DEVICE_COLUMNS = [
    "subscriber_device_id",
    "query_timestamp",
    "subscriber_device_encryption",
    "subscriber_vpn_active",
    "subscriber_vpn_connected",
    "subscriber_selinux_status",
    "subscriber_device_model",
    "subscriber_os_version",
    "subscriber_device_ram",
    "subscriber_system_user",
    "subscriber_device_cores_count",
    "subscriber_location_lat",
    "subscriber_location_long",
    "subscriber_timezone",
    "subscriber_device_storage",
    "subscriber_device_board",
    "subscriber_device_product",
    "subscriber_os_device",
    "subscriber_language",
    "is_fraudulent",
    "fraud_type",
]


class GoldDeviceSDKGenerator:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema

    def load_silver_layer(self) -> pd.DataFrame:
        """Load silver layer data from catalog."""
        print("Loading silver layer data...")
        silver_data = spark.read.table(
            f"{self.catalog}.{self.schema}.silver_device_sdk"
        )
        return silver_data.toPandas()

    def create_gold_layer(self, silver_data: pd.DataFrame) -> pd.DataFrame:
        """Create gold layer: one row per device (latest snapshot), selected columns."""
        print("Creating gold layer data...")

        # One row per device: keep latest query_timestamp per device
        silver_sorted = silver_data.sort_values(
            "query_timestamp", ascending=False
        ).reset_index(drop=True)
        silver_dedup = silver_sorted.drop_duplicates(
            subset=["subscriber_device_id"], keep="first"
        )

        # Select only columns that exist in silver
        available = [c for c in GOLD_DEVICE_COLUMNS if c in silver_dedup.columns]
        gold_data = silver_dedup[available].copy()

        gold_data = self._apply_business_formatting(gold_data)

        print(f"Gold layer created with {len(gold_data)} records (one per device)")
        return gold_data

    def _apply_business_formatting(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply final business formatting and data quality checks."""
        print("Applying business formatting...")

        if "query_timestamp" in data.columns:
            data["query_timestamp"] = pd.to_datetime(data["query_timestamp"])

        numeric_cols = [
            "subscriber_device_ram",
            "subscriber_device_cores_count",
            "subscriber_device_storage",
            "subscriber_location_lat",
            "subscriber_location_long",
        ]
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")

        string_cols = [
            "subscriber_device_id",
            "subscriber_device_encryption",
            "subscriber_selinux_status",
            "subscriber_device_model",
            "subscriber_os_version",
            "subscriber_timezone",
            "subscriber_device_board",
            "subscriber_device_product",
            "subscriber_os_device",
            "subscriber_language",
            "fraud_type",
        ]
        for col in string_cols:
            if col in data.columns:
                data[col] = data[col].astype(str)

        bool_cols = [
            "subscriber_vpn_active",
            "subscriber_vpn_connected",
            "subscriber_system_user",
            "is_fraudulent",
        ]
        for col in bool_cols:
            if col in data.columns:
                data[col] = data[col].fillna(False).astype(bool)

        return data

    def save_gold_layer(self, gold_data: pd.DataFrame):
        """Save gold layer to catalog."""
        print(
            f"Saving gold layer to {self.catalog}.{self.schema}.gold_device_sdk..."
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(
            f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.gold_device_sdk"
        )
        spark_df = spark.createDataFrame(gold_data)
        spark_df.write.mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.schema}.gold_device_sdk"
        )
        print(f"Gold layer saved successfully with {len(gold_data)} records")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Generate gold layer for device SDK"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="telecommunications",
        help="Unity Catalog name (default: telecommunications)",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="fraud_data",
        help="Schema name (default: fraud_data)",
    )

    args = parser.parse_args()

    print("Gold Device SDK Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")

    generator = GoldDeviceSDKGenerator(
        catalog=args.catalog, schema=args.schema
    )

    silver_data = generator.load_silver_layer()
    gold_data = generator.create_gold_layer(silver_data)
    generator.save_gold_layer(gold_data)

    print("Gold device SDK layer generation complete!")


if __name__ == "__main__":
    main()

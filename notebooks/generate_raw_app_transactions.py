#!/usr/bin/env python3
from __future__ import annotations
"""
Generate raw unstructured app transaction data and save to a Unity Catalog Volume.
Output format: JSON Lines (NDJSON), partitioned by date.
"""

import argparse
import pandas as pd
from datetime import datetime

from pyspark.sql import SparkSession

# Import the existing generator to reuse its logic
from generate_bronze_app_transactions import BronzeAppTransactionsGenerator

spark = SparkSession.getActiveSession()


def ensure_volume_exists(catalog: str, schema: str, volume_name: str) -> None:
    """Create the volume if it does not exist."""
    full_volume = f"{catalog}.{schema}.{volume_name}"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_volume}")
    print(f"Volume ready: {full_volume}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate raw app transaction data and save to Volume as NDJSON"
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
    parser.add_argument(
        "--volume-name",
        type=str,
        default="raw_app_transactions",
        help="Volume name for raw files (default: raw_app_transactions)",
    )
    parser.add_argument(
        "--num-transactions",
        type=int,
        default=100000,
        help="Number of transactions to generate (default: 100000)",
    )
    args = parser.parse_args()

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume_name}"
    print("Raw App Transactions Generator")
    print("=" * 50)
    print(f"Target: {volume_path}")

    start_date = datetime(2024, 1, 1)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Ensure volume exists
    ensure_volume_exists(args.catalog, args.schema, args.volume_name)

    # Generate raw transaction data
    generator = BronzeAppTransactionsGenerator(
        catalog=args.catalog,
        schema=args.schema,
        num_transactions=args.num_transactions,
        fraud_rate=0.05,
        start_date=start_date,
        end_date=end_date,
    )
    raw_data = generator.generate_raw_transaction_data()

    # Add partition columns from transaction_timestamp
    raw_data["yyyy"] = raw_data["transaction_timestamp"].dt.strftime("%Y")
    raw_data["mm"] = raw_data["transaction_timestamp"].dt.strftime("%m")
    raw_data["dd"] = raw_data["transaction_timestamp"].dt.strftime("%d")

    # Write as JSON Lines (NDJSON) to Volume with date partitioning
    spark_df = spark.createDataFrame(raw_data)
    spark_df.write.mode("overwrite").partitionBy("yyyy", "mm", "dd").json(
        volume_path
    )

    print(f"\nRaw data written to {volume_path}")
    print(f"Date range: {start_date.date()} → {end_date.date()}")
    print(f"Total transactions: {len(raw_data)}")
    print("Format: JSON Lines (NDJSON), partitioned by yyyy/mm/dd")


if __name__ == "__main__":
    main()

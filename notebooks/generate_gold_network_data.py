#!/usr/bin/env python3
"""
Generate gold layer for network data: business/risk-ready subset with subscriber_device_id
for merging with Device SDK and Transactions. Output: gold_network_data.
"""

from __future__ import annotations

import argparse

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Columns to keep in gold (risk engine and analytics). Only include if present in silver.
GOLD_NETWORK_COLUMNS = [
    "event_id",
    "subscriber_device_id",
    "subscriber_id",
    "event_type",
    "event_subtype",
    "direction",
    "timestamp_start",
    "timestamp_end",
    "duration_sec",
    "status",
    "cell_id",
    "lac",
    "base_station_id",
    "geo_est_method",
    "geo_lat_est",
    "geo_lon_est",
    "geo_accuracy_m",
    "geo_country",
    "geo_region",
    "geo_city",
    "a_party_id",
    "b_party_id",
    "a_party_country",
    "b_party_country",
    "is_internal_call",
    "originating_switch_id",
    "terminating_switch_id",
    "home_mno_id",
    "visited_mno_id",
    "radio_access_technology",
    "session_id",
    "device_os",
    "device_model",
    "app_channel",
    "client_ip",
    "ip_geo_country",
    "charge_amount",
    "charge_currency",
    "rated_volume",
    "is_roaming",
    "roaming_zone",
    "auth_method",
    "account_segment",
]


def load_silver(catalog: str, schema: str) -> pd.DataFrame:
    df = spark.read.table(f"{catalog}.{schema}.silver_network_data")
    return df.toPandas()


def create_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in GOLD_NETWORK_COLUMNS if c in silver_df.columns]
    gold_df = silver_df[available].copy()
    gold_df["timestamp_start"] = pd.to_datetime(gold_df["timestamp_start"])
    if "timestamp_end" in gold_df.columns:
        gold_df["timestamp_end"] = pd.to_datetime(gold_df["timestamp_end"], errors="coerce")
    if "duration_sec" in gold_df.columns:
        gold_df["duration_sec"] = pd.to_numeric(gold_df["duration_sec"], errors="coerce").fillna(0).astype(int)
    if "geo_lat_est" in gold_df.columns:
        gold_df["geo_lat_est"] = pd.to_numeric(gold_df["geo_lat_est"], errors="coerce")
    if "geo_lon_est" in gold_df.columns:
        gold_df["geo_lon_est"] = pd.to_numeric(gold_df["geo_lon_est"], errors="coerce")
    if "charge_amount" in gold_df.columns:
        gold_df["charge_amount"] = pd.to_numeric(gold_df["charge_amount"], errors="coerce")
    if "rated_volume" in gold_df.columns:
        gold_df["rated_volume"] = pd.to_numeric(gold_df["rated_volume"], errors="coerce")
    return gold_df


def save_gold(gold_df: pd.DataFrame, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.gold_network_data")
    spark.createDataFrame(gold_df).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_network_data")
    print(f"Gold table saved: {catalog}.{schema}.gold_network_data with {len(gold_df):,} rows")


def main():
    parser = argparse.ArgumentParser(description="Generate gold network data")
    parser.add_argument("--catalog", type=str, default="telecommunications")
    parser.add_argument("--schema", type=str, default="fraud_data")
    args = parser.parse_args()

    print("Gold Network Data")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}.gold_network_data")

    silver_df = load_silver(args.catalog, args.schema)
    gold_df = create_gold(silver_df)
    save_gold(gold_df, args.catalog, args.schema)
    print("Done.")


if __name__ == "__main__":
    main()

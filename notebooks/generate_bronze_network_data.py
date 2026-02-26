#!/usr/bin/env python3
"""
Ingest raw network data from Volume into bronze layer.
Reads NDJSON from raw_network_data Volume, writes bronze_network_data Delta table.
Key columns: event_id, subscriber_device_id, timestamp_start; report = JSON of remaining fields.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

KEY_COLUMNS = ["event_id", "subscriber_device_id", "timestamp_start"]


def read_raw_from_volume(catalog: str, schema: str, volume_name: str = "raw_network_data"):
    path = f"/Volumes/{catalog}/{schema}/{volume_name}"
    df = spark.read.json(path)
    return df


def create_bronze_from_spark(spark_df) -> pd.DataFrame:
    """Convert to bronze: key columns + report JSON (for schema flexibility)."""
    all_cols = spark_df.columns
    partition_cols = ["yyyy", "mm", "dd"]
    report_cols = [c for c in all_cols if c not in KEY_COLUMNS and c not in partition_cols]
    if not report_cols:
        report_cols = [c for c in all_cols if c not in KEY_COLUMNS]
    struct_cols = [c for c in report_cols if c in spark_df.columns]
    if not struct_cols:
        struct_cols = [c for c in all_cols if c not in KEY_COLUMNS and c not in partition_cols]
    report_struct = F.struct(*[F.col(c).alias(c) for c in struct_cols])
    bronze = spark_df.select(
        F.col("event_id"),
        F.col("subscriber_device_id"),
        F.to_timestamp(F.col("timestamp_start")).alias("timestamp_start"),
        F.to_json(report_struct, options={"timestampFormat": "yyyy-MM-dd HH:mm:ss.SSS"}).alias("report"),
    ).distinct()
    return bronze.toPandas()


def create_bronze_table_from_raw_pandas(catalog: str, schema: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Alternative: when raw is already a pandas DataFrame, build bronze with report JSON."""
    key_df = raw_df[KEY_COLUMNS].copy()
    key_df["timestamp_start"] = pd.to_datetime(key_df["timestamp_start"])
    report_cols = [c for c in raw_df.columns if c not in KEY_COLUMNS and c not in ["yyyy", "mm", "dd"]]
    def to_report(row):
        d = {c: row[c] for c in report_cols if c in row}
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        return json.dumps(d, default=str)
    key_df["report"] = raw_df.apply(to_report, axis=1)
    return key_df


def save_bronze_from_spark(bronze_spark_df, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.bronze_network_data")
    bronze_spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_network_data")
    print(f"Bronze table saved: {catalog}.{schema}.bronze_network_data")


def main():
    parser = argparse.ArgumentParser(description="Ingest raw network data into bronze layer")
    parser.add_argument("--catalog", type=str, default="telecommunications")
    parser.add_argument("--schema", type=str, default="fraud_data")
    parser.add_argument("--source", type=str, default="volume")
    args = parser.parse_args()

    print("Bronze Network Data Ingest")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}.bronze_network_data")

    raw_df = read_raw_from_volume(args.catalog, args.schema)
    raw_count = raw_df.count()
    print(f"Read {raw_count:,} raw records from Volume")

    exclude = set(KEY_COLUMNS) | {"yyyy", "mm", "dd"}
    report_cols = [c for c in raw_df.columns if c not in exclude]
    report_struct = F.struct(*[F.col(c).alias(c) for c in report_cols])
    bronze_spark = raw_df.select(
        F.col("event_id"),
        F.col("subscriber_device_id"),
        F.to_timestamp(F.col("timestamp_start")).alias("timestamp_start"),
        F.to_json(report_struct, options={"timestampFormat": "yyyy-MM-dd HH:mm:ss.SSS"}).alias("report"),
    )

    save_bronze_from_spark(bronze_spark, args.catalog, args.schema)
    print(f"Done. Records: {bronze_spark.count():,}")


if __name__ == "__main__":
    main()

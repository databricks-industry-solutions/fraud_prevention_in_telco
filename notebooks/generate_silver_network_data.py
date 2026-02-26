#!/usr/bin/env python3
"""
Generate silver layer for network data: unpack bronze report, join cell_registry for geo,
set location abstractions, normalize types. Output: silver_network_data.
"""

from __future__ import annotations

import argparse
import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()


def load_bronze(catalog: str, schema: str) -> pd.DataFrame:
    df = spark.read.table(f"{catalog}.{schema}.bronze_network_data")
    return df.toPandas()


def load_cell_registry(catalog: str, schema: str) -> pd.DataFrame:
    df = spark.read.table(f"{catalog}.{schema}.cell_registry")
    return df.toPandas()


def unpack_report(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Unpack report JSON into columns and merge with key columns."""
    rows = []
    for _, row in bronze_df.iterrows():
        rec = {"event_id": row["event_id"], "subscriber_device_id": row["subscriber_device_id"], "timestamp_start": row["timestamp_start"]}
        try:
            rec.update(json.loads(row["report"]))
        except (TypeError, ValueError, KeyError):
            pass
        rows.append(rec)
    return pd.DataFrame(rows)


def apply_geo_from_cells(silver_df: pd.DataFrame, cell_registry: pd.DataFrame) -> pd.DataFrame:
    """Join cell_id to registry; set geo_est_method, geo_lat_est, geo_lon_est, geo_accuracy_m, geo_country/region/city."""
    merged = silver_df.merge(
        cell_registry[["cell_id", "bs_lat", "bs_lon", "nominal_radius_m", "env_type", "country", "region", "city"]].rename(
            columns={
                "bs_lat": "geo_lat_est",
                "bs_lon": "geo_lon_est",
                "nominal_radius_m": "geo_accuracy_m",
                "country": "geo_country",
                "region": "geo_region",
                "city": "geo_city",
            }
        ),
        on="cell_id",
        how="left",
    )
    merged["geo_est_method"] = "cell_id"
    merged["geo_source_priority"] = 1
    return merged


def normalize_silver(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize types and compute duration_sec where applicable."""
    if "timestamp_start" in silver_df.columns:
        silver_df["timestamp_start"] = pd.to_datetime(silver_df["timestamp_start"])
    if "timestamp_end" in silver_df.columns:
        silver_df["timestamp_end"] = pd.to_datetime(silver_df["timestamp_end"], errors="coerce")
    if "duration_sec" not in silver_df.columns and "timestamp_end" in silver_df.columns:
        silver_df["duration_sec"] = (
            (silver_df["timestamp_end"] - silver_df["timestamp_start"]).dt.total_seconds().fillna(0).astype(int)
        )
    if "geo_lat_est" in silver_df.columns:
        silver_df["geo_lat_est"] = pd.to_numeric(silver_df["geo_lat_est"], errors="coerce")
    if "geo_lon_est" in silver_df.columns:
        silver_df["geo_lon_est"] = pd.to_numeric(silver_df["geo_lon_est"], errors="coerce")
    if "geo_accuracy_m" in silver_df.columns:
        silver_df["geo_accuracy_m"] = pd.to_numeric(silver_df["geo_accuracy_m"], errors="coerce")
    return silver_df


def save_silver(silver_df: pd.DataFrame, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.silver_network_data")
    spark_df = spark.createDataFrame(silver_df)
    spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_network_data")
    print(f"Silver table saved: {catalog}.{schema}.silver_network_data with {len(silver_df):,} rows")


def main():
    parser = argparse.ArgumentParser(description="Generate silver network data with cell geo")
    parser.add_argument("--catalog", type=str, default="telecommunications")
    parser.add_argument("--schema", type=str, default="fraud_data")
    args = parser.parse_args()

    print("Silver Network Data")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}.silver_network_data")

    bronze_df = load_bronze(args.catalog, args.schema)
    cell_registry = load_cell_registry(args.catalog, args.schema)
    print(f"Loaded {len(bronze_df):,} bronze records, {len(cell_registry):,} cells")

    silver_df = unpack_report(bronze_df)
    silver_df = apply_geo_from_cells(silver_df, cell_registry)
    silver_df = normalize_silver(silver_df)
    save_silver(silver_df, args.catalog, args.schema)
    print("Done.")


if __name__ == "__main__":
    main()

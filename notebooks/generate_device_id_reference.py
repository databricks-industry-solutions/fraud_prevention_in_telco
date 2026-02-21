#!/usr/bin/env python3
"""
Generate a master list of device IDs for use across both Device SDK and Transaction pipelines.

This script creates a reference table that allows both pipelines to run independently
while ensuring device_id consistency for later joins between tables.

Output Table: {catalog}.{schema}.device_id_reference
Schema: device_id (string)
"""

import argparse
import pandas as pd
from pyspark.sql import SparkSession

def generate_device_id_reference(catalog: str = "telecommunications", 
                                   schema: str = "fraud_data", 
                                   num_devices: int = 10000):
    """
    Generate device ID reference table.
    
    This table serves as a master list that both pipelines can reference
    without depending on each other for device ID generation.
    
    Args:
        catalog: Unity Catalog name
        schema: Schema name
        num_devices: Number of device IDs to generate (default: 10,000)
    """
    print(f"Generating {num_devices:,} device IDs for reference table...")
    print(f"Target: {catalog}.{schema}.device_id_reference")
    
    # Generate sequential device IDs
    device_ids = [f'device_{i:06d}' for i in range(num_devices)]
    
    # Create DataFrame
    df = pd.DataFrame({'device_id': device_ids})
    
    # Save as reference table
    spark = SparkSession.getActiveSession()
    spark_df = spark.createDataFrame(df)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.device_id_reference")
    spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.device_id_reference")
    
    print(f"✓ Device ID reference table created successfully")
    print(f"  Table: {catalog}.{schema}.device_id_reference")
    print(f"  Records: {len(device_ids):,}")
    
    return device_ids

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Generate device ID reference table")
    parser.add_argument("--catalog", type=str, default="telecommunications", 
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    parser.add_argument("--num-devices", type=int, default=10000,
                       help="Number of device IDs to generate (default: 10000)")
    
    args = parser.parse_args()
    generate_device_id_reference(args.catalog, args.schema, args.num_devices)

if __name__ == "__main__":
    main()


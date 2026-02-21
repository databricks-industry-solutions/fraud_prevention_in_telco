#!/usr/bin/env python3
"""
Generate silver layer data for device SDK.
Reads bronze layer and creates cleaned, transformed data.
"""

import argparse
import pandas as pd
import json
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

class SilverDeviceSDKGenerator:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema
    
    def load_bronze_layer(self) -> pd.DataFrame:
        """Load bronze layer data from catalog"""
        print("Loading bronze layer data...")
        bronze_data = spark.read.table(f"{self.catalog}.{self.schema}.bronze_device_sdk")
        return bronze_data.toPandas()
    
    def create_silver_layer(self, bronze_data: pd.DataFrame) -> pd.DataFrame:
        """Create silver layer by unpacking bronze layer and applying transformations"""
        print("Creating silver layer data...")
        
        # Unpack JSON report
        silver_data = pd.DataFrame()
        silver_data['subscriber_device_id'] = bronze_data['device_id']
        silver_data['query_timestamp'] = bronze_data['query_timestamp']
        
        # Parse JSON reports
        reports = bronze_data['report'].apply(json.loads)
        report_df = pd.DataFrame(list(reports))
        
        # Combine key columns with unpacked report
        silver_data = pd.concat([silver_data, report_df], axis=1)
        
        # Apply data cleaning and transformations
        silver_data = self._apply_transformations(silver_data)
        
        print(f"Silver layer created with {len(silver_data)} records")
        return silver_data
    
    def _apply_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to create derived/calculated fields"""
        print("Applying transformations...")
        
        # Convert datetimes to ensure proper format
        data['query_timestamp'] = pd.to_datetime(data['query_timestamp'])

        if 'subscriber_location_lat' in data.columns:
            data['subscriber_location_lat'] = pd.to_numeric(
                data['subscriber_location_lat'], errors='coerce'
            ).astype(float)
        if 'subscriber_location_long' in data.columns:
            data['subscriber_location_long'] = pd.to_numeric(
                data['subscriber_location_long'], errors='coerce'
            ).astype(float)
        
        return data
    
    def save_silver_layer(self, silver_data: pd.DataFrame):
        """Save silver layer data to catalog"""
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.silver_device_sdk")
        spark_df = spark.createDataFrame(silver_data)
        spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.{self.schema}.silver_device_sdk")
        print(f"Silver layer saved to {self.catalog}.{self.schema}.silver_device_sdk")

def main():
    """Generate silver layer data for device SDK"""
    parser = argparse.ArgumentParser(description="Generate silver layer for device SDK")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    
    args = parser.parse_args()
    
    print("Silver Device SDK Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    
    generator = SilverDeviceSDKGenerator(catalog=args.catalog, schema=args.schema)
    
    # Load bronze layer
    bronze_data = generator.load_bronze_layer()
    
    # Create and save silver layer
    silver_data = generator.create_silver_layer(bronze_data)
    generator.save_silver_layer(silver_data)
    
    print(f"\nSilver layer generation completed!")
    print(f"Total devices: {len(silver_data)}")

if __name__ == "__main__":
    main()


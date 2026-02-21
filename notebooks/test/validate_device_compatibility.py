#!/usr/bin/env python3
"""
Validate device ID compatibility between Device SDK and Transaction pipelines.

This script checks that all device IDs used in transactions can be properly joined
with device SDK data, ensuring data integrity across the pipelines.

Run this after both pipelines complete to verify successful data generation.
"""

import argparse

from pyspark.sql import SparkSession


def validate_device_joins(catalog: str = "telecommunications", schema: str = "fraud_data"):
    """
    Validate that transaction device IDs exist in the reference table.
    
    Returns:
        bool: True if all device IDs are compatible, False otherwise
    """
    print("Validating device ID compatibility...")
    
    spark = SparkSession.getActiveSession()
    
    try:
        # Check if all transaction device_ids exist in reference
        ref_devices = spark.read.table(f"{catalog}.{schema}.device_id_reference")
        txn_devices = spark.sql(f"""
            SELECT DISTINCT subscriber_device_id as device_id 
            FROM {catalog}.{schema}.silver_app_transactions
        """)
        
        # Count missing devices
        missing_devices = txn_devices.join(
            ref_devices, 
            on="device_id", 
            how="left_anti"
        )
        
        missing_count = missing_devices.count()
        
        if missing_count > 0:
            print(f"⚠️  WARNING: {missing_count} transaction device IDs not in reference table")
            print("Sample of missing device IDs:")
            missing_devices.show(10, truncate=False)
            return False
        else:
            print("✓ All device IDs compatible for joining")
            
            # Show statistics
            total_ref = ref_devices.count()
            total_txn = txn_devices.count()
            print(f"  Reference table: {total_ref:,} devices")
            print(f"  Transaction table: {total_txn:,} unique device IDs")
            return True
            
    except Exception as e:
        print(f"✗ Error during validation: {e}")
        return False

def validate_device_sdk_table(catalog: str = "telecommunications", schema: str = "fraud_data"):
    """
    Validate that device SDK table device IDs match the reference table.
    
    Returns:
        bool: True if compatible, False otherwise
    """
    print("\nValidating device SDK table compatibility...")
    
    spark = SparkSession.getActiveSession()
    
    try:
        ref_devices = spark.read.table(f"{catalog}.{schema}.device_id_reference")
        sdk_devices = spark.sql(f"""
            SELECT DISTINCT subscriber_device_id as device_id 
            FROM {catalog}.{schema}.silver_device_sdk
        """)
        
        missing_devices = sdk_devices.join(
            ref_devices,
            on="device_id",
            how="left_anti"
        )
        
        missing_count = missing_devices.count()
        
        if missing_count > 0:
            print(f"⚠️  WARNING: {missing_count} SDK device IDs not in reference table")
            return False
        else:
            print("✓ All SDK device IDs compatible")
            return True
            
    except Exception as e:
        print(f"✗ Error validating device SDK: {e}")
        return False

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Validate device ID compatibility across pipelines")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Device ID Compatibility Validation")
    print("=" * 60)
    print(f"Target: {args.catalog}.{args.schema}\n")
    
    # Validate transaction compatibility
    txn_valid = validate_device_joins(catalog=args.catalog, schema=args.schema)
    
    # Validate SDK compatibility
    sdk_valid = validate_device_sdk_table(catalog=args.catalog, schema=args.schema)
    
    # Summary
    print("\n" + "=" * 60)
    if txn_valid and sdk_valid:
        print("✓ ALL VALIDATIONS PASSED")
        print("  Device IDs are compatible across all pipelines")
    else:
        print("✗ VALIDATION FAILED")
        print("  Some device IDs are missing - check the warnings above")
    print("=" * 60)

if __name__ == "__main__":
    main()


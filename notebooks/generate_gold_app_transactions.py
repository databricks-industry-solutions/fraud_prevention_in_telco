#!/usr/bin/env python3
"""
Generate gold layer data for app transactions.
Reads silver layer and creates business-ready data WITHOUT fraud labels.
This layer is for business analytics and reporting.
"""

import argparse
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

class GoldAppTransactionsGenerator:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema
    
    def load_silver_layer(self) -> pd.DataFrame:
        """Load silver layer data from catalog."""
        print("Loading silver layer data...")
        silver_data = spark.read.table(f"{self.catalog}.{self.schema}.silver_app_transactions")
        return silver_data.toPandas()
    
    def create_gold_layer(self, silver_data: pd.DataFrame) -> pd.DataFrame:
        """Create gold layer by selecting and formatting business-ready columns."""
        print("Creating gold layer data...")
        
        # Select and rename columns for business schema
        gold_data = pd.DataFrame()
        
        # Core transaction identifiers
        gold_data['transaction_id'] = silver_data['transaction_id']
        gold_data['transaction_timestamp'] = silver_data['transaction_timestamp']
        
        # Business context
        gold_data['transaction_state'] = silver_data['transaction_state']
        gold_data['transaction_region'] = silver_data['transaction_region']
        gold_data['transaction_type'] = silver_data['transaction_type']
        gold_data['transaction_subtype'] = silver_data['transaction_subtype']
        
        # Account information
        gold_data['account_id'] = silver_data['account_id']
        gold_data['customer_user_id'] = silver_data['customer_user_id']
        gold_data['customer_name'] = silver_data['customer_name']
        gold_data['account_detail'] = silver_data['account_detail']
        gold_data['account_services'] = silver_data['account_services']
        
        # Transaction details
        gold_data['purchased_device_service_price'] = silver_data['purchased_device_service_price']
        gold_data['payment_method'] = silver_data['payment_method']
        gold_data['merchant_category_code'] = silver_data['merchant_category_code']
        
        # Device information
        gold_data['subscriber_device_id'] = silver_data['subscriber_device_id']
        
        # Geographic information
        gold_data['subscriber_location_lat'] = silver_data['subscriber_location_lat']
        gold_data['subscriber_location_long'] = silver_data['subscriber_location_long']
        gold_data['subscriber_timezone'] = silver_data['subscriber_timezone']
        
        # Network information
        gold_data['network_type'] = silver_data['network_type']
        gold_data['subscriber_ip_country'] = silver_data['subscriber_ip_country']
        
        # Session information
        gold_data['session_id'] = silver_data['session_id']
        
        # Risk indicator (business-level flag, NOT fraud label)
        gold_data['high_risk_flag'] = silver_data['high_risk_flag']
        
        # Risk Features (for risk engine)
        gold_data['subscriber_geo_velocity_kmph'] = silver_data['subscriber_geo_velocity_kmph']
        gold_data['subscriber_high_value_device_amount_flag'] = silver_data['subscriber_high_value_device_amount_flag']
        gold_data['subscriber_after_hours_txn_flag'] = silver_data['subscriber_after_hours_txn_flag']
        gold_data['subscriber_device_novelty_flag'] = silver_data['subscriber_device_novelty_flag']
        gold_data['has_mfa'] = silver_data['has_mfa']
        gold_data['subscriber_mfa_anomaly_score'] = silver_data['subscriber_mfa_anomaly_score']
        gold_data['subscriber_login_failure_burst_cnt'] = silver_data['subscriber_login_failure_burst_cnt']
        gold_data['subscriber_pwd_reset_cnt_24h'] = silver_data['subscriber_pwd_reset_cnt_24h']
        gold_data['subscriber_pwd_reset_txn_gap_min'] = silver_data['subscriber_pwd_reset_txn_gap_min']
        gold_data['previous_txn_count_1h'] = silver_data['previous_txn_count_1h']
        gold_data['previous_txn_count_24h'] = silver_data['previous_txn_count_24h']
        gold_data['previous_txn_amount_1h'] = silver_data['previous_txn_amount_1h']
        gold_data['previous_txn_amount_24h'] = silver_data['previous_txn_amount_24h']
        gold_data['subscriber_sim_swap_recent_flag'] = silver_data['subscriber_sim_swap_recent_flag']
        gold_data['subscriber_profile_change_cnt_24h'] = silver_data['subscriber_profile_change_cnt_24h']
        gold_data['subscriber_darkweb_breach_flag'] = silver_data['subscriber_darkweb_breach_flag']
        gold_data['device_trust_score'] = silver_data['device_trust_score']
        
        # Apply final formatting
        gold_data = self._apply_business_formatting(gold_data)
        
        print(f"Gold layer created with {len(gold_data)} records")
        return gold_data
    
    def _apply_business_formatting(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply final business formatting and data quality checks."""
        print("Applying business formatting...")
        
        # Ensure datetime format
        data['transaction_timestamp'] = pd.to_datetime(data['transaction_timestamp'])
        
        # Ensure numeric types
        data['purchased_device_service_price'] = data['purchased_device_service_price'].astype(float)
        data['subscriber_location_lat'] = data['subscriber_location_lat'].astype(float)
        data['subscriber_location_long'] = data['subscriber_location_long'].astype(float)
        
        # Ensure string types
        string_columns = [
            'transaction_id', 'transaction_state', 'transaction_region',
            'transaction_type', 'transaction_subtype', 'account_id',
            'customer_user_id', 'customer_name', 'account_detail',
            'account_services', 'payment_method', 'merchant_category_code',
            'subscriber_device_id', 'subscriber_timezone', 'network_type',
            'subscriber_ip_country', 'session_id'
        ]
        for col in string_columns:
            if col in data.columns:
                data[col] = data[col].astype(str)
        
        # Ensure boolean type
        data['high_risk_flag'] = data['high_risk_flag'].astype(bool)
        
        return data
    
    def save_gold_layer(self, gold_data: pd.DataFrame):
        """Save gold layer to catalog."""
        print(f"Saving gold layer to {self.catalog}.{self.schema}.gold_app_transactions...")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.gold_app_transactions")
        spark_df = spark.createDataFrame(gold_data)
        spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.{self.schema}.gold_app_transactions")
        print(f"Gold layer saved successfully with {len(gold_data)} records")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Generate gold layer for app transactions")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    
    args = parser.parse_args()
    
    print("Gold App Transactions Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    
    generator = GoldAppTransactionsGenerator(catalog=args.catalog, schema=args.schema)
    
    # Load silver layer
    silver_data = generator.load_silver_layer()
    
    # Create gold layer
    gold_data = generator.create_gold_layer(silver_data)
    
    # Save gold layer
    generator.save_gold_layer(gold_data)
    
    print("Gold layer generation complete!")

if __name__ == "__main__":
    main()


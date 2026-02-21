#!/usr/bin/env python3
"""
Generate silver layer data for app transactions.
Reads bronze layer and creates cleaned, transformed data.
"""

import argparse
import pandas as pd
import json
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

class SilverAppTransactionsGenerator:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema
    
    def load_bronze_layer(self) -> pd.DataFrame:
        """Load bronze layer data from catalog."""
        print("Loading bronze layer data...")
        bronze_data = spark.read.table(f"{self.catalog}.{self.schema}.bronze_app_transactions")
        return bronze_data.toPandas()
    
    def create_silver_layer(self, bronze_data: pd.DataFrame) -> pd.DataFrame:
        """Create silver layer by unpacking bronze layer and applying transformations."""
        print("Creating silver layer data...")
        
        # Unpack JSON report
        silver_data = pd.DataFrame()
        silver_data['transaction_id'] = bronze_data['transaction_id']
        silver_data['transaction_timestamp'] = bronze_data['transaction_timestamp']
        
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
        """Apply transformations to create derived/calculated fields."""
        print("Applying transformations...")
        
        # Convert datetimes
        data['transaction_timestamp'] = pd.to_datetime(data['transaction_timestamp'])

        # Ensure geo coordinates are numeric floats
        if 'subscriber_location_lat' in data.columns:
            data['subscriber_location_lat'] = pd.to_numeric(
                data['subscriber_location_lat'], errors='coerce'
            ).astype(float)
        if 'subscriber_location_long' in data.columns:
            data['subscriber_location_long'] = pd.to_numeric(
                data['subscriber_location_long'], errors='coerce'
            ).astype(float)
        if 'subscriber_timezone' in data.columns:
            data['subscriber_timezone'] = data['subscriber_timezone'].astype(str)
        
        # Add feature timestamp (same as transaction timestamp)
        data['feature_timestamp'] = data['transaction_timestamp']
        
        # Business transformations for gold layer
        data = self._add_business_transformations(data)
        
        # Generate risk features based on raw data
        data = self._generate_risk_features(data)
        
        # Add label (0 for genuine transactions)
        data['label'] = 0
        
        return data
    
    def _add_business_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add business transformations for gold layer support."""
        import json
        
        # Regional mapping with US states
        regions_map = {
            'WEST': ['CA', 'OR', 'WA', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM', 'AK', 'HI'],
            'EAST': ['NY', 'PA', 'NJ', 'MA', 'CT', 'RI', 'VT', 'NH', 'ME', 'MD', 'DE', 'DC'],
            'SOUTH': ['TX', 'FL', 'GA', 'NC', 'SC', 'VA', 'TN', 'AL', 'MS', 'LA', 'AR', 'KY', 'WV'],
            'NORTH': ['IL', 'MI', 'OH', 'IN', 'WI', 'MN', 'IA', 'ND', 'SD'],
            'CENTER': ['MO', 'KS', 'NE', 'OK']
        }
        
        # Create reverse lookup for state to region
        state_to_region = {}
        for region, states in regions_map.items():
            for state in states:
                state_to_region[state] = region
        
        # Map transaction_state to transaction_region
        data['transaction_region'] = data['transaction_state'].map(state_to_region).fillna('UNKNOWN').astype(str)
        
        # Create account_detail JSON
        # Format: {"account_type": "individual", "tenure_months": 24, "services": ["wireless", "wireline"]}
        account_details = []
        for idx, row in data.iterrows():
            detail = {
                'account_type': row.get('account_type', 'individual'),
                'tenure_months': int(row.get('account_tenure_months', 0)),
                'services': row.get('account_services', '').split(',') if row.get('account_services') else []
            }
            account_details.append(json.dumps(detail))
        data['account_detail'] = account_details
        
        # Compute high_risk_flag based on transaction attributes
        # High risk if: amount > 1000 OR sim_swap OR pwd_reset_in_24h OR login_attempts > 3
        data['high_risk_flag'] = (
            (data['purchased_device_service_price'] > 1000) |
            (data.get('sim_swap_flag', False)) |
            (data.get('pwd_reset_in_last_24h', False)) |
            (data.get('login_attempts', 0) > 3)
        ).astype(bool)
        
        # Ensure account_services is string format
        if 'account_services' in data.columns:
            data['account_services'] = data['account_services'].fillna('').astype(str)
        
        return data
    
    def _generate_risk_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate risk indicator features derived from raw bronze data."""
        import numpy as np
        
        # Direct mappings from bronze raw fields
        data['subscriber_login_success'] = data['is_login_success'].astype(bool)
        data['subscriber_device_novelty_flag'] = data['is_new_device'].astype(bool)
        data['subscriber_device_reputation_score'] = data['device_trust_score'].astype(float)
        data['subscriber_sim_swap_recent_flag'] = data['sim_swap_flag'].astype(bool)
        data['subscriber_darkweb_breach_flag'] = data['darkweb_breach_flag'].astype(bool)
        data['subscriber_profile_change_cnt_24h'] = data['profile_change_count_24h'].astype(int)
        
        # Transaction hour and after hours flag
        data['subscriber_txn_hour'] = data['transaction_timestamp'].apply(lambda x: x.hour).astype(int)
        data['subscriber_after_hours_txn_flag'] = data['transaction_timestamp'].apply(
            lambda x: x.hour < 6 or x.hour > 22
        ).astype(bool)
        
        # High value transaction flag - derived from amount
        data['subscriber_high_value_device_amount_flag'] = (data['purchased_device_service_price'] > 1000).astype(bool)
        
        # Geographic features - derived from location data
        data['subscriber_geo_distance_km'] = pd.Series([0.0] * len(data), dtype=float)  # Would need previous location for actual calculation
        data['subscriber_geo_velocity_kmph'] = pd.Series([0.0] * len(data), dtype=float)  # Would need time/position data for actual calculation
        data['subscriber_geo_hop_cnt_24h'] = np.where(data['previous_txn_count_24h'] > 5, 
                                                       data['previous_txn_count_24h'].apply(lambda x: min(10, x // 5)), 
                                                       0).astype(int)
        
        # Device trust level derived from trust score
        data['subscriber_device_trust_level'] = data['device_trust_score'].apply(
            lambda x: 'high' if x > 80 else 'medium' if x > 60 else 'low'
        ).astype(str)
        
        # Login patterns derived from login data
        data['subscriber_login_failure_burst_cnt'] = np.where(~data['is_login_success'], data['login_attempts'], 0).astype(int)
        data['subscriber_logins_last_1h'] = np.where(data['previous_txn_count_1h'] > 0, 
                                                      (data['previous_txn_count_1h'] / 2).astype(int), 
                                                      1).astype(int)
        data['subscriber_logins_last_24h'] = np.where(data['previous_txn_count_24h'] > 0,
                                                       (data['previous_txn_count_24h'] / 3).astype(int).clip(5, 30),
                                                       5).astype(int)
        data['subscriber_login_cnt_ratio_1h_24h'] = (data['subscriber_logins_last_1h'] / 
                                                      data['subscriber_logins_last_24h'].replace(0, 1)).astype(float)
        data['subscriber_after_hours_login_cnt_24h'] = np.where(data['subscriber_after_hours_txn_flag'],
                                                                 np.random.randint(0, 3, len(data)), 0).astype(int)
        
        # MFA anomaly score - derived from MFA settings
        data['subscriber_mfa_anomaly_score'] = np.where(data['has_mfa'] == False, 50.0, 0.0)
        
        # subscriber_pwd_reset_txn_gap_min - ensure float type from bronze data
        # Handle any numeric type conversion properly
        if 'pwd_reset_time_minutes_ago' in data.columns:
            data['pwd_reset_time_minutes_ago'] = pd.to_numeric(data['pwd_reset_time_minutes_ago'], errors='coerce').fillna(1440.0).astype(float)
            data['subscriber_pwd_reset_txn_gap_min'] = data['pwd_reset_time_minutes_ago']
        else:
            # If column doesn't exist, create with default
            data['subscriber_pwd_reset_txn_gap_min'] = 1440.0
        
        data['subscriber_pwd_reset_cnt_24h'] = np.where(data['pwd_reset_in_last_24h'], 1, 0).astype(int)
        
        # Pattern flags - derived from raw data
        data['subscriber_contact_divergence_flag'] = pd.Series([False] * len(data), dtype=bool)  # Would need contact data
        data['subscriber_new_payee_hi_value_flag'] = np.where((data['purchased_device_service_price'] > 500) & 
                                                               (data['is_new_device'] == True), True, False).astype(bool)
        data['subscriber_out_of_pattern_mcc_flag'] = data['merchant_category_code'].isin(['6011', '5542']).astype(bool)
        
        # Transaction patterns from previous transactions
        data['subscriber_hi_value_txn_cnt_1h'] = np.where(data['previous_txn_amount_1h'] > 1000, 
                                                           (data['previous_txn_count_1h'] / 3).astype(int).clip(0, 5), 0).astype(int)
        data['subscriber_txn_amt_sum_1h'] = data['previous_txn_amount_1h'].astype(float)
        data['subscriber_txn_amt_ratio_1h_24h'] = (data['previous_txn_amount_1h'] / 
                                                    data['previous_txn_amount_24h'].replace(0, 1)).astype(float)
        
        # IP risk score derived from IP risk level
        ip_risk_map = {'low': 10.0, 'medium': 50.0, 'high': 90.0}
        data['subscriber_ip_risk_score'] = data['subscriber_ip_risk_level'].map(ip_risk_map).astype(float)
        
        # Z-scores - simplified calculation (would need historical data for proper z-scores)
        mean_txn_cnt = data['previous_txn_count_24h'].mean()
        std_txn_cnt = data['previous_txn_count_24h'].std()
        data['subscriber_txn_cnt_zscore'] = ((data['previous_txn_count_24h'] - mean_txn_cnt) / std_txn_cnt).fillna(0.0).astype(float)
        
        mean_txn_amt = data['purchased_device_service_price'].mean()
        std_txn_amt = data['purchased_device_service_price'].std()
        data['subscriber_avg_txn_amt_zscore'] = ((data['purchased_device_service_price'] - mean_txn_amt) / std_txn_amt).fillna(0.0).astype(float)
        
        mean_login_cnt = data['subscriber_logins_last_24h'].mean()
        std_login_cnt = data['subscriber_logins_last_24h'].std()
        data['subscriber_login_cnt_zscore'] = ((data['subscriber_logins_last_24h'] - mean_login_cnt) / std_login_cnt).fillna(0.0).astype(float)
        
        # Device counts - simplified
        data['subscriber_distinct_devices_14d'] = np.minimum(3, (data['previous_txn_count_24h'] / 10).astype(int) + 1).astype(int)
        data['subscriber_shared_device_acct_cnt'] = np.minimum(2, (data['previous_txn_count_24h'] / 25).astype(int) + 1).astype(int)
        
        return data
    
    def save_silver_layer(self, silver_data: pd.DataFrame):
        """Save silver layer data to catalog."""
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.silver_app_transactions")
        spark_df = spark.createDataFrame(silver_data)
        spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.{self.schema}.silver_app_transactions")
        print(f"Silver layer saved to {self.catalog}.{self.schema}.silver_app_transactions")

def main():
    """Generate silver layer data for app transactions."""
    parser = argparse.ArgumentParser(description="Generate silver layer for app transactions")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    
    args = parser.parse_args()
    
    print("Silver App Transactions Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    
    generator = SilverAppTransactionsGenerator(catalog=args.catalog, schema=args.schema)
    
    # Load bronze layer
    bronze_data = generator.load_bronze_layer()
    
    # Create and save silver layer
    silver_data = generator.create_silver_layer(bronze_data)
    generator.save_silver_layer(silver_data)
    
    print(f"\nSilver layer generation completed!")
    print(f"Total transactions: {len(silver_data)}")

if __name__ == "__main__":
    main()


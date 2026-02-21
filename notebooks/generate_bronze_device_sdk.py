#!/usr/bin/env python3
from __future__ import annotations
"""
Generate bronze layer data for device SDK.
Creates raw device profiling data wrapped in JSON format.
"""

import argparse
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.getActiveSession()

class BronzeDeviceSDKGenerator:
    def __init__(
        self,
        catalog: str = "telecommunications",
        schema: str = "fraud_data",
        num_devices: int = 10000,
        fraud_ratio: float = 0.05,
        start_date: str | datetime = "2024-01-01",
        end_date: str | datetime | None = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.num_devices = num_devices
        self.fraud_ratio = fraud_ratio
        self.num_fraudulent = int(num_devices * fraud_ratio)
        self.num_genuine = num_devices - self.num_fraudulent
        self.start_date = self._resolve_datetime(start_date)
        resolved_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else end_date
        self.end_date = self._resolve_datetime(resolved_end)
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        
        # Initialize random seed for reproducibility
        np.random.seed(42)
        random.seed(42)
        
        # Define realistic value ranges for telecom devices
        self.device_configs = self._initialize_device_configs()
        
    def _initialize_device_configs(self):
        """Initialize realistic device configurations"""
        return {
            # RAM configurations (in GB)
            'ram_options': [2, 3, 4, 6, 8, 12, 16],
            'ram_weights': [0.1, 0.15, 0.25, 0.25, 0.15, 0.08, 0.02],
            
            # Battery capacity (mAh)
            'battery_capacity_range': (2000, 6000),
            
            # Battery voltage (V)
            'battery_voltage_range': (3.7, 4.2),
            
            # Font scale
            'font_scale_range': (0.8, 1.4),
            
            # Sensors count
            'sensors_range': (5, 15),
            
            # Screen densities
            'density_options': ['ldpi', 'mdpi', 'hdpi', 'xhdpi', 'xxhdpi', 'xxxhdpi'],
            'density_weights': [0.05, 0.15, 0.25, 0.3, 0.2, 0.05],
            
            # Timezones (major cities)
            'timezone_options': [
                'America/New_York', 'America/Los_Angeles', 'America/Chicago',
                'America/Denver', 'Europe/London', 'Europe/Paris', 'Europe/Berlin',
                'Asia/Tokyo', 'Asia/Shanghai', 'Asia/Seoul', 'Australia/Sydney'
            ],
            
            # UI modes
            'ui_mode_options': ['normal', 'car', 'desk', 'television', 'appliance', 'watch'],
            'ui_mode_weights': [0.95, 0.01, 0.02, 0.01, 0.005, 0.005],
            
            # Screen layout sizes
            'screen_layout_options': ['small', 'normal', 'large', 'xlarge'],
            'screen_layout_weights': [0.1, 0.6, 0.25, 0.05],
            
            # Supported ABIs
            'abi_options': [
                'armeabi-v7a', 'arm64-v8a', 'x86', 'x86_64',
                'armeabi-v7a,arm64-v8a', 'x86,x86_64',
                'armeabi-v7a,arm64-v8a,x86', 'arm64-v8a,x86_64'
            ],
            'abi_weights': [0.2, 0.3, 0.1, 0.05, 0.2, 0.05, 0.08, 0.02],
            
            # Core counts
            'cores_options': [4, 6, 8, 10, 12],
            'cores_weights': [0.2, 0.3, 0.3, 0.15, 0.05],
            
            # Connection types
            'connection_options': ['wifi', 'mobile', 'ethernet', 'bluetooth'],
            'connection_weights': [0.6, 0.35, 0.03, 0.02],
            
            # OS versions
            'os_major_options': [10, 11, 12, 13, 14],
            'os_major_weights': [0.1, 0.2, 0.3, 0.25, 0.15],
            
            # OS architectures
            'os_arch_options': ['arm64-v8a', 'armeabi-v7a', 'x86', 'x86_64'],
            'os_arch_weights': [0.6, 0.25, 0.1, 0.05],
            
            # Animation scales
            'animation_scale_range': (0.5, 1.5),
            
            # Barometer count
            'barometer_range': (0, 2),
            
            # Bluetooth paired devices
            'bluetooth_devices_range': (0, 8),
            
            # Screen orientations
            'orientation_options': ['portrait', 'landscape', 'square'],
            'orientation_weights': [0.7, 0.25, 0.05],
            
            # VPN protocols
            'vpn_protocol_options': ['OpenVPN', 'IKEv2', 'WireGuard', 'L2TP', 'PPTP'],
            'vpn_protocol_weights': [0.3, 0.25, 0.2, 0.15, 0.1],
            
            # Play Services versions
            'play_services_major_range': (20, 24),
            'play_services_minor_range': (0, 50),
            'play_services_patch_range': (0, 20),
            'play_services_build_range': (1000000, 9999999),
            
            # Security patch dates
            'security_patch_years': [2023, 2024, 2025],
            'security_patch_years_weights': [0.1, 0.6, 0.3],
            
            # ROM configurations
            'rom_board_options': ['qcom', 'exynos', 'mediatek', 'kirin', 'google'],
            'rom_board_weights': [0.4, 0.2, 0.2, 0.1, 0.1],
            
            'rom_model_options': ['SM-G998B', 'SM-A525F', 'SM-G991B', 'Pixel 6', 'Pixel 7', 'OnePlus 9', 'Xiaomi 12'],
            'rom_model_weights': [0.15, 0.2, 0.15, 0.1, 0.1, 0.1, 0.2],
            
            'rom_product_options': ['o1s', 'a52s', 'o1s', 'redfin', 'panther', 'lemonade', 'cupid'],
            'rom_product_weights': [0.15, 0.2, 0.15, 0.1, 0.1, 0.1, 0.2],
            
            # Screen resolutions
            'screen_resolution_options': [
                '720x1280', '1080x1920', '1080x2400', '1440x2560', '1440x3200',
                '720x1560', '1080x2340', '1080x2400', '1440x3040'
            ],
            'screen_resolution_weights': [0.1, 0.25, 0.2, 0.15, 0.1, 0.05, 0.1, 0.03, 0.02],
            
            # Languages
            'language_options': ['en', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'zh', 'ja', 'ko'],
            'language_weights': [0.4, 0.1, 0.08, 0.08, 0.06, 0.06, 0.05, 0.08, 0.05, 0.04],
            
            # Geographic coordinates (major cities)
            'geo_coordinates': [
                (40.7128, -74.0060),  # New York
                (34.0522, -118.2437), # Los Angeles
                (41.8781, -87.6298),  # Chicago
                (39.7392, -104.9903), # Denver
                (51.5074, -0.1278),   # London
                (48.8566, 2.3522),    # Paris
                (52.5200, 13.4050),   # Berlin
                (35.6762, 139.6503),  # Tokyo
                (31.2304, 121.4737),  # Shanghai
                (37.5665, 126.9780),  # Seoul
                (-33.8688, 151.2093)  # Sydney
            ],
            'geo_weights': [0.15, 0.12, 0.1, 0.08, 0.1, 0.08, 0.08, 0.1, 0.08, 0.08, 0.03]
        }

    def _resolve_datetime(self, value: str | datetime) -> datetime:
        """Normalize input to a midnight datetime."""
        if isinstance(value, datetime):
            return value.replace(hour=0, minute=0, second=0, microsecond=0)
        ts = pd.to_datetime(value)
        return ts.normalize().to_pydatetime()
    
    def _generate_genuine_device(self, device_id, query_timestamp=None):
        """Generate a genuine device profile"""
        config = self.device_configs
        
        # Generate query_timestamp if not provided
        if query_timestamp is None:
            query_timestamp = self.start_date + timedelta(
                days=random.randint(0, (self.end_date - self.start_date).days),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        
        # Generate geographic location
        geo_idx = np.random.choice(len(config['geo_coordinates']), p=config['geo_weights'])
        lat, lon = config['geo_coordinates'][geo_idx]
        lat += np.random.normal(0, 0.01)
        lon += np.random.normal(0, 0.01)
        
        # Generate language
        lang = np.random.choice(config['language_options'], p=config['language_weights'])
        
        # Generate OS version
        os_major = np.random.choice(config['os_major_options'], p=config['os_major_weights'])
        os_minor = np.random.randint(0, 10)
        os_patch = np.random.randint(0, 10)
        
        # Generate Play Services version
        play_major = np.random.randint(*config['play_services_major_range'])
        play_minor = np.random.randint(*config['play_services_minor_range'])
        play_patch = np.random.randint(*config['play_services_patch_range'])
        play_build = np.random.randint(*config['play_services_build_range'])
        
        # Generate security patch
        patch_year = np.random.choice(config['security_patch_years'], p=config['security_patch_years_weights'])
        patch_month = np.random.randint(1, 13)
        patch_day = np.random.randint(1, 29)
        
        # Generate ROM info
        rom_board = np.random.choice(config['rom_board_options'], p=config['rom_board_weights'])
        rom_model = np.random.choice(config['rom_model_options'], p=config['rom_model_weights'])
        rom_product = np.random.choice(config['rom_product_options'], p=config['rom_product_weights'])
        
        # Generate screen resolution
        screen_res = np.random.choice(config['screen_resolution_options'], p=config['screen_resolution_weights'])
        
        # Generate CPU hash
        cpu_hash = f"{rom_board}_{np.random.randint(1000, 9999)}"
        
        device = {
            'subscriber_device_id': device_id,
            'query_timestamp': query_timestamp,
            'is_fraudulent': False,
            'fraud_type': 'genuine',
            
            # Numerical features
            'subscriber_device_ram': np.random.choice(config['ram_options'], p=config['ram_weights']),
            'installed_applications_count': np.random.randint(50, 200),
            'subscriber_device_battery_capacity': np.random.randint(*config['battery_capacity_range']),
            'subscriber_device_battery_voltage': round(np.random.uniform(*config['battery_voltage_range']), 2),
            'subscriber_device_font_scale': round(np.random.uniform(*config['font_scale_range']), 2),
            'subscriber_device_sensors_total': np.random.randint(*config['sensors_range']),
            'subscriber_location_lat': round(lat, 6),
            'subscriber_location_long': round(lon, 6),
            
            # Categorical features
            'subscriber_device_density': np.random.choice(config['density_options'], p=config['density_weights']),
            'subscriber_timezone': np.random.choice(config['timezone_options']),
            'subscriber_profile_count': np.random.randint(1, 4),
            'subscriber_managed_profile': np.random.choice([True, False], p=[0.1, 0.9]),
            'subscriber_device_storage': np.random.choice([32, 64, 128, 256, 512], p=[0.1, 0.2, 0.4, 0.2, 0.1]),
            'subscriber_device_ui_mode': np.random.choice(config['ui_mode_options'], p=config['ui_mode_weights']),
            'subscriber_device_dark_mode': np.random.choice([True, False], p=[0.3, 0.7]),
            'subscriber_device_adaptive_display': np.random.choice([True, False], p=[0.2, 0.8]),
            'subscriber_device_screen_layout': np.random.choice(config['screen_layout_options'], p=config['screen_layout_weights']),
            'subscriber_device_supported_abis': np.random.choice(config['abi_options'], p=config['abi_weights']),
            'subscriber_device_cores_count': np.random.choice(config['cores_options'], p=config['cores_weights']),
            'subscriber_vpn_active': np.random.choice([True, False], p=[0.05, 0.95]),
            'subscriber_connection_type': np.random.choice(config['connection_options'], p=config['connection_weights']),
            'subscriber_pin_enabled': np.random.choice([True, False], p=[0.8, 0.2]),
            'subscriber_device_locked': np.random.choice([True, False], p=[0.7, 0.3]),
            'subscriber_device_encryption': np.random.choice(['encrypted', 'unencrypted'], p=[0.9, 0.1]),
            'subscriber_os_major': os_major,
            'subscriber_device_arch': np.random.choice(config['os_arch_options'], p=config['os_arch_weights']),
            'subscriber_device_animation_scale': round(np.random.uniform(*config['animation_scale_range']), 2),
            'subscriber_device_animator_scale': round(np.random.uniform(*config['animation_scale_range']), 2),
            'subscriber_device_barometer_count': np.random.randint(*config['barometer_range']),
            'subscriber_bluetooth_devices': np.random.randint(*config['bluetooth_devices_range']),
            'subscriber_device_orientation': np.random.choice(config['orientation_options'], p=config['orientation_weights']),
            'subscriber_vpn_protocol': np.random.choice(config['vpn_protocol_options'], p=config['vpn_protocol_weights']) if np.random.random() < 0.05 else 'None',
            'subscriber_play_services_major': play_major,
            'subscriber_play_services_minor': play_minor,
            'subscriber_play_services_patch': play_patch,
            'subscriber_play_services_build': play_build,
            'subscriber_play_services_internal_id': f"build_{play_build}",
            'subscriber_security_patch_month': patch_month,
            'subscriber_security_patch_year': patch_year,
            'subscriber_security_patch_day': patch_day,
            'subscriber_device_board': rom_board,
            'subscriber_device_model': rom_model,
            'subscriber_device_product': rom_product,
            'subscriber_device_bootloader': f"{rom_board}_bootloader_v{np.random.randint(1, 10)}",
            'subscriber_device_hardware': f"{rom_board}_hw_v{np.random.randint(1, 5)}",
            'subscriber_device_resolution': screen_res,
            'subscriber_device_cpu_hash': cpu_hash,
            'subscriber_os_device': f"Android {os_major}.{os_minor}.{os_patch}",
            'subscriber_os_version': f"{os_major}.{os_minor}.{os_patch}",
            'subscriber_language': lang,
            
            # Binary features
            'subscriber_system_user': np.random.choice([True, False], p=[0.95, 0.05]),
            'subscriber_dual_space': np.random.choice([True, False], p=[0.1, 0.9]),
            'subscriber_vpn_connected': np.random.choice([True, False], p=[0.05, 0.95]),
            'subscriber_selinux_status': np.random.choice(['enforcing', 'permissive', 'disabled'], p=[0.8, 0.15, 0.05])
        }
        
        return device
    
    def _generate_fraudulent_device(self, device_id, query_timestamp=None):
        """Generate a fraudulent device profile"""
        device = self._generate_genuine_device(device_id, query_timestamp)
        device['is_fraudulent'] = True
        
        # Define fraud types
        fraud_types = {
            'vpn_manipulation': 0.20,
            'os_spoofing': 0.15,
            'device_emulation': 0.15,
            'jailbreak_root': 0.15,
            'id_cloning': 0.10,
            'ring_fraud': 0.10,
            'device_farm': 0.05,
            'sim_hijacking': 0.10
        }
        
        fraud_type = np.random.choice(list(fraud_types.keys()), p=list(fraud_types.values()))
        device['fraud_type'] = fraud_type
        
        # Apply fraud-specific modifications (simplified)
        if fraud_type == 'vpn_manipulation':
            device['subscriber_vpn_active'] = True
            device['subscriber_vpn_connected'] = True
            device['subscriber_location_lat'] = round(np.random.uniform(-90, 90), 6)
            device['subscriber_location_long'] = round(np.random.uniform(-180, 180), 6)
        elif fraud_type == 'device_emulation':
            device['subscriber_device_model'] = 'Android SDK built for x86'
            device['subscriber_device_ram'] = 2048
            device['subscriber_device_cores_count'] = 2
        elif fraud_type == 'jailbreak_root':
            device['subscriber_system_user'] = False
            device['subscriber_selinux_status'] = 'disabled'
            device['subscriber_device_encryption'] = 'unencrypted'
            
        return device
    
    def _load_device_ids(self):
        """
        Load device IDs from reference table.
        
        This ensures both Device SDK and Transaction pipelines use the same device IDs.
        
        Returns:
            List of device ID strings
        """
        try:
            # Load from reference table (preferred - enables independent pipelines)
            device_ref = spark.read.table(f"{self.catalog}.{self.schema}.device_id_reference")
            device_ids = device_ref.select("device_id").collect()
            return [row.device_id for row in device_ids]
        except Exception:
            # Fallback: generate device IDs if reference table doesn't exist
            print("Warning: Reference table not found, generating synthetic device IDs")
            return [f'device_{i:06d}' for i in range(self.num_devices)]
    
    def generate_raw_device_data(self):
        """Generate raw device data with all fields"""
        print(f"Generating {self.num_devices:,} raw devices...")
        
        device_ids = self._load_device_ids()
        if not device_ids:
            raise ValueError("No device IDs available for generation")

        total_available = len(device_ids)
        if total_available < self.num_devices:
            print(f"Warning: Requested {self.num_devices:,} device IDs but only {total_available:,} available. Using available IDs.")
        num_to_use = min(self.num_devices, total_available)

        random.shuffle(device_ids)
        selected_ids = device_ids[:num_to_use]

        num_fraudulent = int(num_to_use * self.fraud_ratio)
        if self.fraud_ratio > 0 and num_to_use > 0 and num_fraudulent == 0:
            num_fraudulent = 1
        num_fraudulent = min(num_fraudulent, num_to_use)
        num_genuine = max(num_to_use - num_fraudulent, 0)

        # Update counts for reporting/logging
        self.num_devices = num_to_use
        self.num_fraudulent = num_fraudulent
        self.num_genuine = num_genuine

        devices = []
        delta_days = max((self.end_date - self.start_date).days, 0)

        def random_timestamp() -> datetime:
            day_offset = random.randint(0, delta_days) if delta_days else 0
            return self.start_date + timedelta(
                days=day_offset,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

        # Generate genuine devices
        print(f"Generating {self.num_genuine} genuine devices...")
        for device_id in selected_ids[:self.num_genuine]:
            query_timestamp = random_timestamp()
            device = self._generate_genuine_device(device_id, query_timestamp)
            devices.append(device)
        
        # Generate fraudulent devices
        print(f"Generating {self.num_fraudulent} fraudulent devices...")
        for device_id in selected_ids[self.num_genuine:self.num_genuine + self.num_fraudulent]:
            query_timestamp = random_timestamp()
            device = self._generate_fraudulent_device(device_id, query_timestamp)
            devices.append(device)
        
        # Shuffle to randomize fraud/genuine order
        random.shuffle(devices)
        
        df = pd.DataFrame(devices)
        print(f"Generated {len(df)} raw devices")
        
        return df
    
    def create_bronze_layer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create bronze layer with JSON report format"""
        print("Creating bronze layer data...")
        
        # Key columns: device_id, query_timestamp
        key_columns = ['subscriber_device_id', 'query_timestamp']
        report_columns = [col for col in df.columns if col not in key_columns]
        
        # Create bronze layer DataFrame
        bronze_data = pd.DataFrame({
            'device_id': df['subscriber_device_id'],
            'query_timestamp': df['query_timestamp']
        })
        
        # Convert report columns to JSON
        def create_report(row):
            report_dict = {col: row[col] for col in report_columns}
            return json.dumps(report_dict, default=str)
        
        bronze_data['report'] = df.apply(create_report, axis=1)
        
        print(f"Bronze layer created with {len(bronze_data)} records")
        return bronze_data
    
    def create_bronze_layer_from_spark(self, spark_df) -> pd.DataFrame:
        """Create bronze layer from Spark DataFrame (e.g., read from Volume)."""
        print("Creating bronze layer from raw data...")
        partition_cols = ['yyyy', 'mm', 'dd']
        key_columns = ['subscriber_device_id', 'query_timestamp']
        all_cols = spark_df.columns
        report_columns = [
            c for c in all_cols
            if c not in key_columns and c not in partition_cols
        ]
        report_struct = F.struct(*[F.col(c).alias(c) for c in report_columns])
        bronze_spark = spark_df.select(
            F.col('subscriber_device_id').alias('device_id'),
            F.col('query_timestamp'),
            F.to_json(report_struct, options={'timestampFormat': 'yyyy-MM-dd HH:mm:ss'}).alias('report')
        )
        bronze_data = bronze_spark.toPandas()
        print(f"Bronze layer created with {len(bronze_data)} records")
        return bronze_data
    
    def save_bronze_layer(self, bronze_data: pd.DataFrame):
        """Save bronze layer data to catalog"""
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.bronze_device_sdk")
        spark_df = spark.createDataFrame(bronze_data)
        spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.{self.schema}.bronze_device_sdk")
        print(f"Bronze layer saved to {self.catalog}.{self.schema}.bronze_device_sdk")

def main():
    """Generate bronze layer data for device SDK"""
    parser = argparse.ArgumentParser(description="Generate bronze layer for device SDK")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    parser.add_argument("--source", type=str, default="generate",
                       choices=["generate", "volume"],
                       help="Source: 'generate' (in-memory) or 'volume' (read from Volume) (default: generate)")
    parser.add_argument("--volume-name", type=str, default="raw_device_sdk",
                       help="Volume name when source=volume (default: raw_device_sdk)")
    
    args = parser.parse_args()
    
    print("Bronze Device SDK Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    print(f"Source: {args.source}")

    generator = BronzeDeviceSDKGenerator(
        catalog=args.catalog,
        schema=args.schema,
        num_devices=10000,
        fraud_ratio=0.05,
        start_date=datetime(2024, 1, 1),
        end_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    )

    if args.source == "volume":
        volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume_name}"
        print(f"Reading raw data from {volume_path}")
        raw_spark = spark.read.json(volume_path)
        bronze_data = generator.create_bronze_layer_from_spark(raw_spark)
    else:
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        raw_data = generator.generate_raw_device_data()
        bronze_data = generator.create_bronze_layer(raw_data)
        # Print fraud statistics
        fraud_stats = raw_data[raw_data['is_fraudulent'] == True]['fraud_type'].value_counts()
        print("\nFraud Type Distribution:")
        for fraud_type, count in fraud_stats.items():
            print(f"  {fraud_type}: {count} ({count/generator.num_fraudulent*100:.1f}%)")

    generator.save_bronze_layer(bronze_data)

    print("\nBronze layer generation completed!")
    print(f"Total devices: {len(bronze_data)}")


if __name__ == "__main__":
    main()


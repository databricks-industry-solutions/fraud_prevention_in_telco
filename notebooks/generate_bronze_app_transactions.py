#!/usr/bin/env python3
from __future__ import annotations
"""
Generate bronze layer data for app transactions.
Creates raw transaction data wrapped in JSON format.
"""

import argparse
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from faker import Faker
spark = SparkSession.getActiveSession()

class BronzeAppTransactionsGenerator:
    def __init__(
        self,
        catalog: str = "telecommunications",
        schema: str = "fraud_data",
        num_transactions: int = 100000,
        fraud_rate: float = 0.05,
        start_date: str | datetime = "2024-01-01",
        end_date: str | datetime | None = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.num_transactions = num_transactions
        self.fraud_rate = fraud_rate
        self.num_fraud = int(num_transactions * fraud_rate)
        self.num_genuine = num_transactions - self.num_fraud
        self.start_date = self._resolve_datetime(start_date)
        resolved_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else end_date
        self.end_date = self._resolve_datetime(resolved_end)
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        
        # Initialize device IDs from our device profiling data
        self.device_ids = self._load_device_ids()
        
        # Initialize faker for names
        self.fake = Faker()
        
        # Transaction type configurations - mapped to BUY/CHANGE
        self.txn_types = {
            'BUY': {
                'weight': 0.40,
                'subtypes': {
                    'device_purchase': 0.50,
                    'service_purchase': 0.30,
                    'modem_purchase': 0.20
                }
            },
            'CHANGE': {
                'weight': 0.60,
                'subtypes': {
                    'address_change': 0.25,
                    'sim_swap': 0.10,
                    'name_change': 0.10,
                    'payment_method_change': 0.10,
                    'plan_change': 0.05,
                    'contact_update': 0.20,
                    'security_settings': 0.20
                }
            }
        }
        
        # Account services
        self.account_services = [
            'wireless', 'wireline', 'health_account', 'home_security', 'streaming'
        ]
        
        # Regional mapping with US states
        self.regions = {
            'WEST': ['CA', 'OR', 'WA', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM', 'AK', 'HI'],
            'EAST': ['NY', 'PA', 'NJ', 'MA', 'CT', 'RI', 'VT', 'NH', 'ME', 'MD', 'DE', 'DC'],
            'SOUTH': ['TX', 'FL', 'GA', 'NC', 'SC', 'VA', 'TN', 'AL', 'MS', 'LA', 'AR', 'KY', 'WV'],
            'NORTH': ['IL', 'MI', 'OH', 'IN', 'WI', 'MN', 'IA', 'ND', 'SD'],
            'CENTER': ['MO', 'KS', 'NE', 'OK']
        }
        
        # State population weights for realistic distribution
        self.state_weights = {
            'CA': 0.12, 'TX': 0.09, 'FL': 0.07, 'NY': 0.06, 'PA': 0.04,
            'IL': 0.04, 'OH': 0.04, 'GA': 0.03, 'NC': 0.03, 'MI': 0.03,
            'NJ': 0.03, 'VA': 0.03, 'WA': 0.02, 'AZ': 0.02, 'MA': 0.02,
            'TN': 0.02, 'IN': 0.02, 'MO': 0.02, 'MD': 0.02, 'WI': 0.02,
            'CO': 0.02, 'MN': 0.02, 'SC': 0.02, 'AL': 0.02, 'LA': 0.02,
            'KY': 0.01, 'OR': 0.01, 'OK': 0.01, 'CT': 0.01, 'UT': 0.01,
            'IA': 0.01, 'NV': 0.01, 'AR': 0.01, 'MS': 0.01, 'KS': 0.01,
            'NM': 0.01, 'NE': 0.01, 'WV': 0.01, 'ID': 0.01, 'HI': 0.01,
            'NH': 0.01, 'ME': 0.01, 'RI': 0.01, 'MT': 0.01, 'DE': 0.01,
            'SD': 0.01, 'ND': 0.01, 'AK': 0.01, 'VT': 0.01, 'WY': 0.01,
            'DC': 0.01
        }

        self.state_city_distribution = {
            'AL': [
                {'lat': 33.5186, 'lon': -86.8104, 'timezone': 'America/Chicago', 'weight': 0.35},
                {'lat': 32.3792, 'lon': -86.3077, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 30.6954, 'lon': -88.0399, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 34.7304, 'lon': -86.5861, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'AK': [
                {'lat': 61.2181, 'lon': -149.9003, 'timezone': 'America/Anchorage', 'weight': 0.45},
                {'lat': 64.8378, 'lon': -147.7164, 'timezone': 'America/Anchorage', 'weight': 0.20},
                {'lat': 58.3019, 'lon': -134.4197, 'timezone': 'America/Juneau', 'weight': 0.20},
                {'lat': 60.5544, 'lon': -151.2583, 'timezone': 'America/Anchorage', 'weight': 0.15},
            ],
            'AZ': [
                {'lat': 33.4484, 'lon': -112.0740, 'timezone': 'America/Phoenix', 'weight': 0.55},
                {'lat': 32.2217, 'lon': -110.9265, 'timezone': 'America/Phoenix', 'weight': 0.20},
                {'lat': 33.4152, 'lon': -111.8315, 'timezone': 'America/Phoenix', 'weight': 0.15},
                {'lat': 34.0489, 'lon': -111.0937, 'timezone': 'America/Phoenix', 'weight': 0.10},
            ],
            'AR': [
                {'lat': 34.7465, 'lon': -92.2896, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 36.1867, 'lon': -94.1288, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 35.2140, 'lon': -91.7375, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 33.4470, 'lon': -94.0377, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'CA': [
                {'lat': 34.0522, 'lon': -118.2437, 'timezone': 'America/Los_Angeles', 'weight': 0.32},
                {'lat': 37.7749, 'lon': -122.4194, 'timezone': 'America/Los_Angeles', 'weight': 0.18},
                {'lat': 32.7157, 'lon': -117.1611, 'timezone': 'America/Los_Angeles', 'weight': 0.14},
                {'lat': 36.7783, 'lon': -119.4179, 'timezone': 'America/Los_Angeles', 'weight': 0.10},
                {'lat': 38.5816, 'lon': -121.4944, 'timezone': 'America/Los_Angeles', 'weight': 0.10},
                {'lat': 33.9533, 'lon': -117.3962, 'timezone': 'America/Los_Angeles', 'weight': 0.08},
                {'lat': 37.3382, 'lon': -121.8863, 'timezone': 'America/Los_Angeles', 'weight': 0.08},
            ],
            'CO': [
                {'lat': 39.7392, 'lon': -104.9903, 'timezone': 'America/Denver', 'weight': 0.50},
                {'lat': 40.5853, 'lon': -105.0844, 'timezone': 'America/Denver', 'weight': 0.20},
                {'lat': 38.8339, 'lon': -104.8214, 'timezone': 'America/Denver', 'weight': 0.20},
                {'lat': 39.1911, 'lon': -106.8175, 'timezone': 'America/Denver', 'weight': 0.10},
            ],
            'CT': [
                {'lat': 41.3083, 'lon': -72.9279, 'timezone': 'America/New_York', 'weight': 0.35},
                {'lat': 41.7658, 'lon': -72.6734, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 41.1865, 'lon': -73.1952, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 41.5236, 'lon': -72.0759, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'DE': [
                {'lat': 39.7391, 'lon': -75.5398, 'timezone': 'America/New_York', 'weight': 0.55},
                {'lat': 39.1582, 'lon': -75.5244, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 38.9229, 'lon': -75.4275, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'DC': [
                {'lat': 38.9072, 'lon': -77.0369, 'timezone': 'America/New_York', 'weight': 1.0},
            ],
            'FL': [
                {'lat': 25.7617, 'lon': -80.1918, 'timezone': 'America/New_York', 'weight': 0.28},
                {'lat': 28.5383, 'lon': -81.3792, 'timezone': 'America/New_York', 'weight': 0.24},
                {'lat': 27.9506, 'lon': -82.4572, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 30.3322, 'lon': -81.6557, 'timezone': 'America/New_York', 'weight': 0.18},
                {'lat': 26.6406, 'lon': -81.8723, 'timezone': 'America/New_York', 'weight': 0.10},
            ],
            'GA': [
                {'lat': 33.7490, 'lon': -84.3880, 'timezone': 'America/New_York', 'weight': 0.45},
                {'lat': 31.1499, 'lon': -81.4915, 'timezone': 'America/New_York', 'weight': 0.18},
                {'lat': 32.0835, 'lon': -81.0998, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 34.2570, 'lon': -85.1647, 'timezone': 'America/New_York', 'weight': 0.17},
            ],
            'HI': [
                {'lat': 21.3069, 'lon': -157.8583, 'timezone': 'Pacific/Honolulu', 'weight': 0.60},
                {'lat': 21.9811, 'lon': -159.3711, 'timezone': 'Pacific/Honolulu', 'weight': 0.15},
                {'lat': 19.7297, 'lon': -155.0902, 'timezone': 'Pacific/Honolulu', 'weight': 0.10},
                {'lat': 20.8890, 'lon': -156.4729, 'timezone': 'Pacific/Honolulu', 'weight': 0.15},
            ],
            'ID': [
                {'lat': 43.6150, 'lon': -116.2023, 'timezone': 'America/Boise', 'weight': 0.50},
                {'lat': 42.8794, 'lon': -112.4506, 'timezone': 'America/Boise', 'weight': 0.20},
                {'lat': 46.7324, 'lon': -117.0002, 'timezone': 'America/Los_Angeles', 'weight': 0.15},
                {'lat': 43.4904, 'lon': -112.0333, 'timezone': 'America/Boise', 'weight': 0.15},
            ],
            'IL': [
                {'lat': 41.8781, 'lon': -87.6298, 'timezone': 'America/Chicago', 'weight': 0.60},
                {'lat': 42.0451, 'lon': -87.6877, 'timezone': 'America/Chicago', 'weight': 0.14},
                {'lat': 38.6270, 'lon': -90.1994, 'timezone': 'America/Chicago', 'weight': 0.16},
                {'lat': 40.6331, 'lon': -89.3985, 'timezone': 'America/Chicago', 'weight': 0.10},
            ],
            'IN': [
                {'lat': 39.7684, 'lon': -86.1581, 'timezone': 'America/Indiana/Indianapolis', 'weight': 0.50},
                {'lat': 41.7005, 'lon': -86.2353, 'timezone': 'America/Indiana/Indianapolis', 'weight': 0.20},
                {'lat': 38.6773, 'lon': -87.5286, 'timezone': 'America/Indiana/Indianapolis', 'weight': 0.15},
                {'lat': 40.1934, 'lon': -85.3864, 'timezone': 'America/Indiana/Indianapolis', 'weight': 0.15},
            ],
            'IA': [
                {'lat': 41.5868, 'lon': -93.6250, 'timezone': 'America/Chicago', 'weight': 0.45},
                {'lat': 41.9779, 'lon': -91.6656, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 41.5236, 'lon': -90.5776, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 42.4990, 'lon': -96.4003, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'KS': [
                {'lat': 39.0997, 'lon': -94.5786, 'timezone': 'America/Chicago', 'weight': 0.45},
                {'lat': 37.6862, 'lon': -97.3356, 'timezone': 'America/Chicago', 'weight': 0.30},
                {'lat': 38.8814, 'lon': -94.8191, 'timezone': 'America/Chicago', 'weight': 0.15},
                {'lat': 39.0443, 'lon': -95.6900, 'timezone': 'America/Chicago', 'weight': 0.10},
            ],
            'KY': [
                {'lat': 38.2527, 'lon': -85.7585, 'timezone': 'America/New_York', 'weight': 0.35},
                {'lat': 38.0406, 'lon': -84.5037, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 39.0837, 'lon': -84.5086, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 37.9901, 'lon': -84.1797, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'LA': [
                {'lat': 29.9511, 'lon': -90.0715, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 30.4515, 'lon': -91.1871, 'timezone': 'America/Chicago', 'weight': 0.30},
                {'lat': 32.5252, 'lon': -92.0810, 'timezone': 'America/Chicago', 'weight': 0.15},
                {'lat': 30.2266, 'lon': -93.2174, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'ME': [
                {'lat': 43.6591, 'lon': -70.2568, 'timezone': 'America/New_York', 'weight': 0.45},
                {'lat': 44.3106, 'lon': -69.7795, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 44.8016, 'lon': -68.7712, 'timezone': 'America/New_York', 'weight': 0.25},
            ],
            'MD': [
                {'lat': 39.2904, 'lon': -76.6122, 'timezone': 'America/New_York', 'weight': 0.45},
                {'lat': 38.9784, 'lon': -76.4922, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 39.0458, 'lon': -76.6413, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 39.0839, 'lon': -77.1528, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'MA': [
                {'lat': 42.3601, 'lon': -71.0589, 'timezone': 'America/New_York', 'weight': 0.55},
                {'lat': 42.2626, 'lon': -71.8023, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 42.4668, 'lon': -70.9495, 'timezone': 'America/New_York', 'weight': 0.15},
                {'lat': 41.7001, 'lon': -71.1548, 'timezone': 'America/New_York', 'weight': 0.10},
            ],
            'MI': [
                {'lat': 42.3314, 'lon': -83.0458, 'timezone': 'America/Detroit', 'weight': 0.40},
                {'lat': 42.7325, 'lon': -84.5555, 'timezone': 'America/Detroit', 'weight': 0.20},
                {'lat': 42.9634, 'lon': -85.6681, 'timezone': 'America/Detroit', 'weight': 0.20},
                {'lat': 42.1292, 'lon': -83.1467, 'timezone': 'America/Detroit', 'weight': 0.20},
            ],
            'MN': [
                {'lat': 44.9778, 'lon': -93.2650, 'timezone': 'America/Chicago', 'weight': 0.35},
                {'lat': 44.9537, 'lon': -93.0900, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 44.0121, 'lon': -92.4802, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 46.7867, 'lon': -92.1005, 'timezone': 'America/Chicago', 'weight': 0.20},
            ],
            'MS': [
                {'lat': 32.2988, 'lon': -90.1848, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 34.2576, 'lon': -88.7034, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 31.5604, 'lon': -91.4032, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 30.3674, 'lon': -89.0928, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'MO': [
                {'lat': 38.6270, 'lon': -90.1994, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 39.0997, 'lon': -94.5786, 'timezone': 'America/Chicago', 'weight': 0.35},
                {'lat': 37.2089, 'lon': -93.2923, 'timezone': 'America/Chicago', 'weight': 0.15},
                {'lat': 39.7686, 'lon': -94.8466, 'timezone': 'America/Chicago', 'weight': 0.10},
            ],
            'MT': [
                {'lat': 45.7833, 'lon': -108.5007, 'timezone': 'America/Denver', 'weight': 0.30},
                {'lat': 46.5958, 'lon': -112.0270, 'timezone': 'America/Denver', 'weight': 0.25},
                {'lat': 47.5053, 'lon': -111.3008, 'timezone': 'America/Denver', 'weight': 0.20},
                {'lat': 48.1958, 'lon': -114.3129, 'timezone': 'America/Denver', 'weight': 0.25},
            ],
            'NE': [
                {'lat': 41.2565, 'lon': -95.9345, 'timezone': 'America/Chicago', 'weight': 0.45},
                {'lat': 41.4925, 'lon': -99.9018, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 40.8136, 'lon': -96.7026, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 42.0327, 'lon': -97.4139, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'NV': [
                {'lat': 36.1699, 'lon': -115.1398, 'timezone': 'America/Los_Angeles', 'weight': 0.57},
                {'lat': 39.1638, 'lon': -119.7674, 'timezone': 'America/Los_Angeles', 'weight': 0.18},
                {'lat': 39.5296, 'lon': -119.8138, 'timezone': 'America/Los_Angeles', 'weight': 0.15},
                {'lat': 36.5463, 'lon': -115.3226, 'timezone': 'America/Los_Angeles', 'weight': 0.10},
            ],
            'NH': [
                {'lat': 42.9956, 'lon': -71.4548, 'timezone': 'America/New_York', 'weight': 0.40},
                {'lat': 43.2081, 'lon': -71.5376, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 43.7012, 'lon': -72.2896, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 44.2706, 'lon': -71.3033, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'NJ': [
                {'lat': 40.7357, 'lon': -74.1724, 'timezone': 'America/New_York', 'weight': 0.40},
                {'lat': 40.7282, 'lon': -74.0776, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 39.9523, 'lon': -75.0849, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 39.3643, 'lon': -74.4229, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'NM': [
                {'lat': 35.0844, 'lon': -106.6504, 'timezone': 'America/Denver', 'weight': 0.50},
                {'lat': 32.3199, 'lon': -106.7637, 'timezone': 'America/Denver', 'weight': 0.25},
                {'lat': 35.6866, 'lon': -105.9378, 'timezone': 'America/Denver', 'weight': 0.15},
                {'lat': 36.4072, 'lon': -105.5731, 'timezone': 'America/Denver', 'weight': 0.10},
            ],
            'NY': [
                {'lat': 40.7128, 'lon': -74.0060, 'timezone': 'America/New_York', 'weight': 0.55},
                {'lat': 43.0481, 'lon': -76.1474, 'timezone': 'America/New_York', 'weight': 0.15},
                {'lat': 42.8864, 'lon': -78.8784, 'timezone': 'America/New_York', 'weight': 0.15},
                {'lat': 42.6526, 'lon': -73.7562, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'NC': [
                {'lat': 35.2271, 'lon': -80.8431, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 35.7796, 'lon': -78.6382, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 36.0726, 'lon': -79.7920, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 34.2257, 'lon': -77.9447, 'timezone': 'America/New_York', 'weight': 0.15},
                {'lat': 35.6127, 'lon': -77.3664, 'timezone': 'America/New_York', 'weight': 0.10},
            ],
            'ND': [
                {'lat': 46.8772, 'lon': -96.7898, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 46.8083, 'lon': -100.7837, 'timezone': 'America/Chicago', 'weight': 0.30},
                {'lat': 48.2325, 'lon': -101.2963, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 46.9068, 'lon': -98.7084, 'timezone': 'America/Chicago', 'weight': 0.10},
            ],
            'OH': [
                {'lat': 39.9612, 'lon': -82.9988, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 41.4993, 'lon': -81.6944, 'timezone': 'America/New_York', 'weight': 0.28},
                {'lat': 39.1031, 'lon': -84.5120, 'timezone': 'America/New_York', 'weight': 0.22},
                {'lat': 41.0814, 'lon': -81.5190, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'OK': [
                {'lat': 35.4676, 'lon': -97.5164, 'timezone': 'America/Chicago', 'weight': 0.45},
                {'lat': 36.1539, 'lon': -95.9928, 'timezone': 'America/Chicago', 'weight': 0.35},
                {'lat': 35.2226, 'lon': -97.4395, 'timezone': 'America/Chicago', 'weight': 0.20},
            ],
            'OR': [
                {'lat': 45.5051, 'lon': -122.6750, 'timezone': 'America/Los_Angeles', 'weight': 0.45},
                {'lat': 44.0521, 'lon': -123.0868, 'timezone': 'America/Los_Angeles', 'weight': 0.25},
                {'lat': 45.4335, 'lon': -122.7703, 'timezone': 'America/Los_Angeles', 'weight': 0.20},
                {'lat': 43.8041, 'lon': -120.5542, 'timezone': 'America/Los_Angeles', 'weight': 0.10},
            ],
            'PA': [
                {'lat': 39.9526, 'lon': -75.1652, 'timezone': 'America/New_York', 'weight': 0.35},
                {'lat': 40.4406, 'lon': -79.9959, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 40.2732, 'lon': -76.8867, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 40.6084, 'lon': -75.4902, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'RI': [
                {'lat': 41.8240, 'lon': -71.4128, 'timezone': 'America/New_York', 'weight': 0.60},
                {'lat': 41.6771, 'lon': -71.2662, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 41.4501, 'lon': -71.4495, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'SC': [
                {'lat': 32.7765, 'lon': -79.9311, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 34.0007, 'lon': -81.0348, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 34.8526, 'lon': -82.3940, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 33.5207, 'lon': -80.8545, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'SD': [
                {'lat': 43.5473, 'lon': -96.7310, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 44.0805, 'lon': -103.2310, 'timezone': 'America/Denver', 'weight': 0.30},
                {'lat': 44.3683, 'lon': -100.3509, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 45.4647, 'lon': -98.4865, 'timezone': 'America/Chicago', 'weight': 0.10},
            ],
            'TN': [
                {'lat': 36.1627, 'lon': -86.7816, 'timezone': 'America/Chicago', 'weight': 0.30},
                {'lat': 35.1495, 'lon': -90.0490, 'timezone': 'America/Chicago', 'weight': 0.28},
                {'lat': 35.0456, 'lon': -85.3097, 'timezone': 'America/New_York', 'weight': 0.22},
                {'lat': 36.1740, 'lon': -83.2949, 'timezone': 'America/New_York', 'weight': 0.20},
            ],
            'TX': [
                {'lat': 29.7604, 'lon': -95.3698, 'timezone': 'America/Chicago', 'weight': 0.26},
                {'lat': 32.7767, 'lon': -96.7970, 'timezone': 'America/Chicago', 'weight': 0.24},
                {'lat': 30.2672, 'lon': -97.7431, 'timezone': 'America/Chicago', 'weight': 0.18},
                {'lat': 29.4241, 'lon': -98.4936, 'timezone': 'America/Chicago', 'weight': 0.16},
                {'lat': 33.4484, 'lon': -94.0377, 'timezone': 'America/Chicago', 'weight': 0.16},
            ],
            'UT': [
                {'lat': 40.7608, 'lon': -111.8910, 'timezone': 'America/Denver', 'weight': 0.48},
                {'lat': 40.2338, 'lon': -111.6585, 'timezone': 'America/Denver', 'weight': 0.22},
                {'lat': 41.2230, 'lon': -111.9738, 'timezone': 'America/Denver', 'weight': 0.18},
                {'lat': 37.0965, 'lon': -113.5684, 'timezone': 'America/Denver', 'weight': 0.12},
            ],
            'VT': [
                {'lat': 44.4759, 'lon': -73.2121, 'timezone': 'America/New_York', 'weight': 0.40},
                {'lat': 44.2601, 'lon': -72.5754, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 43.6106, 'lon': -72.9726, 'timezone': 'America/New_York', 'weight': 0.30},
            ],
            'VA': [
                {'lat': 38.9072, 'lon': -77.0369, 'timezone': 'America/New_York', 'weight': 0.30},
                {'lat': 37.5407, 'lon': -77.4360, 'timezone': 'America/New_York', 'weight': 0.22},
                {'lat': 36.8468, 'lon': -76.2859, 'timezone': 'America/New_York', 'weight': 0.22},
                {'lat': 38.8048, 'lon': -77.0469, 'timezone': 'America/New_York', 'weight': 0.16},
                {'lat': 38.4496, 'lon': -78.8689, 'timezone': 'America/New_York', 'weight': 0.10},
            ],
            'WA': [
                {'lat': 47.6062, 'lon': -122.3321, 'timezone': 'America/Los_Angeles', 'weight': 0.45},
                {'lat': 47.2529, 'lon': -122.4443, 'timezone': 'America/Los_Angeles', 'weight': 0.20},
                {'lat': 47.6588, 'lon': -117.4260, 'timezone': 'America/Los_Angeles', 'weight': 0.20},
                {'lat': 48.7496, 'lon': -122.4787, 'timezone': 'America/Los_Angeles', 'weight': 0.15},
            ],
            'WV': [
                {'lat': 38.3498, 'lon': -81.6326, 'timezone': 'America/New_York', 'weight': 0.40},
                {'lat': 39.6295, 'lon': -79.9559, 'timezone': 'America/New_York', 'weight': 0.25},
                {'lat': 38.4192, 'lon': -82.4452, 'timezone': 'America/New_York', 'weight': 0.20},
                {'lat': 37.7840, 'lon': -81.1882, 'timezone': 'America/New_York', 'weight': 0.15},
            ],
            'WI': [
                {'lat': 43.0389, 'lon': -87.9065, 'timezone': 'America/Chicago', 'weight': 0.40},
                {'lat': 43.0731, 'lon': -89.4012, 'timezone': 'America/Chicago', 'weight': 0.25},
                {'lat': 44.2619, 'lon': -88.4154, 'timezone': 'America/Chicago', 'weight': 0.20},
                {'lat': 44.5192, 'lon': -88.0198, 'timezone': 'America/Chicago', 'weight': 0.15},
            ],
            'WY': [
                {'lat': 41.139981, 'lon': -104.820246, 'timezone': 'America/Denver', 'weight': 0.35},
                {'lat': 42.866632, 'lon': -106.313081, 'timezone': 'America/Denver', 'weight': 0.30},
                {'lat': 44.5263, 'lon': -109.0565, 'timezone': 'America/Denver', 'weight': 0.20},
                {'lat': 44.4750, 'lon': -110.7347, 'timezone': 'America/Denver', 'weight': 0.15},
            ],
        }
        
        # Risk thresholds
        self.risk_thresholds = {
            'high_value_amount': 1000,
            'geo_velocity_threshold': 500,
            'login_failure_threshold': 3,
            'after_hours_start': 22,
            'after_hours_end': 6,
        }
        
        # Initialize accounts structure
        self.accounts = {}
        
    def _load_device_ids(self) -> List[str]:
        """
        Load device IDs from reference table.
        
        This allows the transaction pipeline to run independently of the device SDK pipeline.
        The reference table is created separately and contains all valid device IDs.
        
        Returns:
            List of device ID strings
        """
        try:
            # Try to load from reference table first (preferred - no dependency)
            device_ref = spark.read.table(f"{self.catalog}.{self.schema}.device_id_reference")
            device_ids = device_ref.select("device_id").collect()
            return [row.device_id for row in device_ids]
        except Exception:
            try:
                # Fallback to bronze_device_sdk if reference table doesn't exist
                print("Warning: Reference table not found, falling back to bronze_device_sdk")
                device_data = spark.read.table(f"{self.catalog}.{self.schema}.bronze_device_sdk")
                device_data_pandas = device_data.toPandas()
                return device_data_pandas['device_id'].tolist()
            except Exception:
                # Generate synthetic device IDs if neither table exists
                print("Warning: No device tables found, generating synthetic device IDs")
                return [f'device_{i:06d}' for i in range(10000)]
    
    def _resolve_datetime(self, value: str | datetime) -> datetime:
        """Normalize input to a midnight datetime."""
        if isinstance(value, datetime):
            return value.replace(hour=0, minute=0, second=0, microsecond=0)
        ts = pd.to_datetime(value)
        return ts.normalize().to_pydatetime()
    
    def _generate_timestamps(self) -> List[datetime]:
        """Generate realistic transaction timestamps."""
        start_date = self.start_date
        end_date = self.end_date
        
        timestamps = []
        delta_days = max((end_date - start_date).days, 0)
        for _ in range(self.num_transactions):
            if random.random() < 0.7:
                hour = random.randint(8, 18)
            else:
                hour = random.choice([0, 1, 2, 3, 4, 5, 6, 7, 19, 20, 21, 22, 23])
            
            day_offset = random.randint(0, delta_days) if delta_days else 0

            date = start_date + timedelta(
                days=day_offset,
                hours=hour,
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            timestamps.append(date)
        
        return sorted(timestamps)
    
    def _generate_accounts(self) -> Dict:
        """Generate account structures with realistic distribution."""
        num_accounts = int(self.num_transactions * 0.3)  # 30% unique accounts
        accounts = {}
        
        for i in range(num_accounts):
            account_id = f"ACC-{i:06d}"
            
            # Account type distribution
            account_type = random.choices(
                ['individual', 'family', 'business'],
                weights=[0.70, 0.20, 0.10]
            )[0]
            
            # Number of users per account
            if account_type == 'individual':
                num_users = 1
            elif account_type == 'family':
                num_users = random.randint(2, 5)
            else:  # business
                num_users = random.randint(6, 10)
            
            # Generate users for this account
            users = []
            for j in range(num_users):
                user_id = f"USR-{i:06d}-{j:02d}"
                users.append({
                    'user_id': user_id,
                    'name': self.fake.name(),
                    'is_primary': j == 0
                })
            
            # Account services (1-3 services per account)
            num_services = random.randint(1, 3)
            services = random.sample(self.account_services, num_services)
            
            # Account tenure
            tenure_months = random.randint(1, 120)
            
            accounts[account_id] = {
                'account_id': account_id,
                'account_type': account_type,
                'users': users,
                'services': services,
                'tenure_months': tenure_months,
                'created_date': self.fake.date_between(start_date='-10y', end_date='today').isoformat()
            }
        
        return accounts
    
    def _generate_transaction_types_and_subtypes(self) -> Tuple[List[str], List[str]]:
        """Generate transaction types and subtypes based on weights."""
        types = []
        subtypes = []
        
        for _ in range(self.num_transactions):
            # Select transaction type
            txn_type = random.choices(
                list(self.txn_types.keys()),
                weights=[self.txn_types[t]['weight'] for t in self.txn_types.keys()]
            )[0]
            
            # Select subtype
            subtype_dict = self.txn_types[txn_type]['subtypes']
            subtype = random.choices(
                list(subtype_dict.keys()),
                weights=list(subtype_dict.values())
            )[0]
            
            types.append(txn_type)
            subtypes.append(subtype)
        
        return types, subtypes
    
    def _generate_amounts(self, txn_types: List[str], subtypes: List[str]) -> List[float]:
        """Generate transaction amounts based on type and subtype."""
        amounts = []
        for txn_type, subtype in zip(txn_types, subtypes):
            if txn_type == 'BUY':
                # BUY transactions: $50-$2000
                amount = random.uniform(50, 2000)
            else:  # CHANGE
                # CHANGE transactions: $0-$50
                amount = random.uniform(0, 50)
            
            amounts.append(round(amount, 2))
        
        return amounts
    
    def generate_raw_transaction_data(self) -> pd.DataFrame:
        """Generate raw transaction data with all fields."""
        print(f"Generating {self.num_transactions:,} raw transactions...")
        
        # Generate accounts first
        self.accounts = self._generate_accounts()
        account_ids = list(self.accounts.keys())
        
        # Generate timestamps
        timestamps = self._generate_timestamps()
        
        # Generate transaction types and subtypes
        txn_types, txn_subtypes = self._generate_transaction_types_and_subtypes()
        
        # Generate amounts based on types and subtypes
        amounts = self._generate_amounts(txn_types, txn_subtypes)
        
        # Generate states with realistic distribution
        states = random.choices(
            list(self.state_weights.keys()),
            weights=list(self.state_weights.values()),
            k=self.num_transactions
        )
        
        # Generate additional raw fields for calculating risk features
        geographic_locations = self._generate_geographic_data(states)
        network_data = self._generate_network_data()
        
        # Select account and user for each transaction
        selected_accounts = random.choices(account_ids, k=self.num_transactions)
        selected_users = []
        for account_id in selected_accounts:
            user = random.choice(self.accounts[account_id]['users'])
            selected_users.append(user)
        
        # Create raw transaction data with all observable fields
        data = pd.DataFrame({
            'transaction_id': [f'txn_{i:08d}' for i in range(self.num_transactions)],
            'transaction_timestamp': timestamps,
            'purchased_device_service_price': amounts,
            'subscriber_device_id': random.choices(self.device_ids, k=self.num_transactions),
            
            # Business context fields (NEW for gold layer)
            'transaction_state': states,
            'transaction_type': txn_types,
            'transaction_subtype': txn_subtypes,
            'account_id': selected_accounts,
            'customer_user_id': [user['user_id'] for user in selected_users],
            'customer_name': [user['name'] for user in selected_users],
            'account_type': [self.accounts[acc_id]['account_type'] for acc_id in selected_accounts],
            'account_services': [','.join(self.accounts[acc_id]['services']) for acc_id in selected_accounts],
            'account_tenure_months': [self.accounts[acc_id]['tenure_months'] for acc_id in selected_accounts],
            'account_users_json': [json.dumps(self.accounts[acc_id]['users']) for acc_id in selected_accounts],
            
            # Transaction details  
            'merchant_category_code': [random.choice(['5411', '5541', '5542', '5734', '5812', '5999', '6011', '7230']) for _ in range(self.num_transactions)],
            'payment_method': [random.choice(['card', 'wallet', 'bank_transfer', 'account_credit']) for _ in range(self.num_transactions)],
            
            # Geographic data
            'subscriber_location_lat': geographic_locations['lat'],
            'subscriber_location_long': geographic_locations['lon'],
            'subscriber_timezone': geographic_locations['timezone'],
            
            # Network data
            'subscriber_ip_address': network_data['ip_address'],
            'subscriber_ip_country': network_data['ip_country'],
            'subscriber_ip_risk_level': network_data['ip_risk_level'],
            'network_type': network_data['network_type'],
            
            # Account and session data
            'session_id': [f'session_{random.randint(100000, 999999)}' for _ in range(self.num_transactions)],
            'login_attempts': [random.randint(0, 5) for _ in range(self.num_transactions)],
            'is_login_success': [random.random() > 0.02 for _ in range(self.num_transactions)],
            
            # Transaction patterns (for calculating aggregations)
            'previous_txn_count_1h': [random.randint(0, 10) for _ in range(self.num_transactions)],
            'previous_txn_count_24h': [random.randint(0, 50) for _ in range(self.num_transactions)],
            'previous_txn_amount_1h': [random.uniform(0, 2000) for _ in range(self.num_transactions)],
            'previous_txn_amount_24h': [random.uniform(0, 5000) for _ in range(self.num_transactions)],
            
            # Device and security flags
            'is_new_device': [random.random() < 0.02 for _ in range(self.num_transactions)],
            'device_trust_score': [random.uniform(60, 100) for _ in range(self.num_transactions)],
            'has_mfa': [random.random() > 0.3 for _ in range(self.num_transactions)],
            'mfa_method': [random.choice(['sms', 'email', 'app', 'none']) if random.random() > 0.3 else 'none' for _ in range(self.num_transactions)],
            
            # Security events
            'pwd_reset_in_last_24h': [random.random() < 0.1 for _ in range(self.num_transactions)],
            'pwd_reset_time_minutes_ago': [random.randint(0, 1440) if random.random() < 0.1 else 1440 for _ in range(self.num_transactions)],
            'sim_swap_flag': [random.random() < 0.005 for _ in range(self.num_transactions)],
            'profile_change_count_24h': [random.randint(0, 5) for _ in range(self.num_transactions)],
            'darkweb_breach_flag': [random.random() < 0.002 for _ in range(self.num_transactions)],
        })
        
        print(f"Generated {len(data)} raw transactions")
        return data
    
    def _generate_geographic_data(self, states: List[str]) -> Dict:
        """Generate geographic location data aligned to selected states."""
        lats = []
        lons = []
        timezones = []
        for state in states:
            cities = self.state_city_distribution.get(state)
            if not cities:
                cities = [{'lat': 39.8283, 'lon': -98.5795, 'timezone': 'America/Chicago', 'weight': 1.0}]
            weights = [city.get('weight', 1.0) for city in cities]
            city_choice = random.choices(cities, weights=weights)[0]
            lats.append(city_choice['lat'] + random.uniform(-0.05, 0.05))
            lons.append(city_choice['lon'] + random.uniform(-0.05, 0.05))
            timezones.append(city_choice['timezone'])
        return {
            'lat': lats,
            'lon': lons,
            'timezone': timezones
        }
    
    def _generate_network_data(self) -> Dict:
        """Generate network and IP data"""
        return {
            'ip_address': [f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}" for _ in range(self.num_transactions)],
            'ip_country': random.choices(['US', 'CA', 'UK', 'FR', 'DE', 'JP'], k=self.num_transactions),
            'ip_risk_level': random.choices(['low', 'medium', 'high'], weights=[0.85, 0.12, 0.03], k=self.num_transactions),
            'network_type': random.choices(['wifi', 'mobile_4g', 'mobile_5g', 'ethernet'], weights=[0.6, 0.25, 0.1, 0.05], k=self.num_transactions)
        }
    
    def create_bronze_layer(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create bronze layer with JSON report format."""
        print("Creating bronze layer data...")
        
        # Key columns: transaction_id, transaction_timestamp
        # Everything else goes into the report
        key_columns = ['transaction_id', 'transaction_timestamp']
        report_columns = [col for col in data.columns if col not in key_columns]
        
        # Create bronze layer DataFrame
        bronze_data = pd.DataFrame({
            'transaction_id': data['transaction_id'],
            'transaction_timestamp': data['transaction_timestamp']
        })
        
        # Convert report columns to JSON
        def create_report(row):
            report_dict = {col: row[col] for col in report_columns}
            return json.dumps(report_dict, default=str)
        
        bronze_data['report'] = data.apply(create_report, axis=1)
        
        print(f"Bronze layer created with {len(bronze_data)} records")
        return bronze_data
    
    def create_bronze_layer_from_spark(self, spark_df) -> pd.DataFrame:
        """Create bronze layer from Spark DataFrame (e.g., read from Volume)."""
        print("Creating bronze layer from raw data...")
        partition_cols = ['yyyy', 'mm', 'dd']
        key_columns = ['transaction_id', 'transaction_timestamp']
        all_cols = spark_df.columns
        report_columns = [
            c for c in all_cols
            if c not in key_columns and c not in partition_cols
        ]
        report_struct = F.struct(*[F.col(c).alias(c) for c in report_columns])
        bronze_spark = spark_df.select(
            F.col('transaction_id'),
            F.col('transaction_timestamp'),
            F.to_json(report_struct, options={'timestampFormat': 'yyyy-MM-dd HH:mm:ss'}).alias('report')
        )
        bronze_data = bronze_spark.toPandas()
        print(f"Bronze layer created with {len(bronze_data)} records")
        return bronze_data
    
    def save_bronze_layer(self, bronze_data: pd.DataFrame):
        """Save bronze layer data to catalog."""
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.bronze_app_transactions")
        spark_df = spark.createDataFrame(bronze_data)
        spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.{self.schema}.bronze_app_transactions")
        print(f"Bronze layer saved to {self.catalog}.{self.schema}.bronze_app_transactions")

def main():
    """Generate bronze layer data for app transactions"""
    parser = argparse.ArgumentParser(description="Generate bronze layer for app transactions")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    parser.add_argument("--source", type=str, default="generate",
                       choices=["generate", "volume"],
                       help="Source: 'generate' (in-memory) or 'volume' (read from Volume) (default: generate)")
    parser.add_argument("--volume-name", type=str, default="raw_app_transactions",
                       help="Volume name when source=volume (default: raw_app_transactions)")
    
    args = parser.parse_args()
    
    print("Bronze App Transactions Generator")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    print(f"Source: {args.source}")

    generator = BronzeAppTransactionsGenerator(
        catalog=args.catalog,
        schema=args.schema,
        num_transactions=100000,
        fraud_rate=0.05,
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
        raw_data = generator.generate_raw_transaction_data()
        bronze_data = generator.create_bronze_layer(raw_data)

    generator.save_bronze_layer(bronze_data)

    print("\nBronze layer generation completed!")
    print(f"Total transactions: {len(bronze_data)}")


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Generate raw CDR-like network activity data and save to a Unity Catalog Volume.
Uses device_id_reference and cell_registry for consistent joins with Device SDK and Transactions.
Output: NDJSON partitioned by yyyy/mm/dd in raw_network_data Volume.
"""

from __future__ import annotations

import argparse
import uuid
import random
from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

EVENT_TYPES = [
    ("voice_out", "call_setup", "outbound"),
    ("voice_in", "call_end", "inbound"),
    ("sms_mo", "sms_submit", "outbound"),
    ("sms_mt", "sms_deliver", "inbound"),
    ("data_session", "http_request", "outbound"),
    ("data_session", "tcp_connect", "outbound"),
    ("app_login", "auth_request", "outbound"),
]
STATUSES = ["success", "success", "success", "failed", "dropped"]
RAT = ["2G", "3G", "4G", "4G", "4G", "5G", "Wi-Fi"]
ACCOUNT_SEGMENTS = ["prepaid", "postpaid", "postpaid", "high-value"]
AUTH_METHODS = ["SIM", "app-token", "2FA", "SIM", "app-token"]


def ensure_volume_exists(catalog: str, schema: str, volume_name: str) -> None:
    full_volume = f"{catalog}.{schema}.{volume_name}"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_volume}")
    print(f"Volume ready: {full_volume}")


def load_device_ids(catalog: str, schema: str) -> list[str]:
    df = spark.read.table(f"{catalog}.{schema}.device_id_reference")
    return [row.device_id for row in df.select("device_id").collect()]


def load_cell_ids(catalog: str, schema: str) -> list[tuple[str, int]]:
    df = spark.read.table(f"{catalog}.{schema}.cell_registry")
    return [(row.cell_id, row.lac) for row in df.select("cell_id", "lac").collect()]


def generate_raw_network_events(
    device_ids: list[str],
    cell_list: list[tuple[str, int]],
    num_events: int,
    start_date: datetime,
    end_date: datetime,
    seed: int = 42,
) -> pd.DataFrame:
    random.seed(seed)
    events = []
    for i in range(num_events):
        ts_start = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        duration_sec = random.choice([0, 30, 60, 120, 300, 600]) if random.random() > 0.3 else 0
        ts_end = ts_start + timedelta(seconds=duration_sec) if duration_sec else None
        event_type, subtype, direction = random.choice(EVENT_TYPES)
        cell_id, lac = random.choice(cell_list) if cell_list else (f"cell_{i % 10000:06d}", 10000 + (i % 1000))
        subscriber_device_id = random.choice(device_ids)
        event_id = str(uuid.uuid4())
        events.append({
            "event_id": event_id,
            "subscriber_id": subscriber_device_id,
            "subscriber_device_id": subscriber_device_id,
            "msisdn": f"+1555{random.randint(1000000, 9999999)}",
            "imsi": f"31026{random.randint(1000000000, 9999999999)}",
            "imei": subscriber_device_id,
            "auth_method": random.choice(AUTH_METHODS),
            "account_segment": random.choice(ACCOUNT_SEGMENTS),
            "event_type": event_type,
            "event_subtype": subtype,
            "direction": direction,
            "timestamp_start": ts_start,
            "timestamp_end": ts_end,
            "duration_sec": duration_sec,
            "status": random.choice(STATUSES),
            "a_party_id": subscriber_device_id,
            "b_party_id": f"ext_{random.randint(1000000, 9999999)}" if direction == "outbound" else subscriber_device_id,
            "a_party_country": "US",
            "b_party_country": "US",
            "is_internal_call": random.random() < 0.2,
            "originating_switch_id": f"mss_{random.randint(1, 50):03d}",
            "terminating_switch_id": f"mss_{random.randint(1, 50):03d}",
            "home_mno_id": "310260",
            "visited_mno_id": "310260" if random.random() > 0.1 else "310410",
            "radio_access_technology": random.choice(RAT),
            "lac": lac,
            "cell_id": cell_id,
            "base_station_id": f"bs_{random.randint(1, 9999):04d}",
            "session_id": f"sess_{random.randint(100000, 999999)}",
            "device_os": random.choice(["Android", "iOS"]),
            "device_model": random.choice(["SM-G991", "iPhone14,2", "Pixel 7"]),
            "app_channel": random.choice(["native_app", "web", "ussd"]),
            "client_ip": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "ip_geo_country": "US",
            "rating_group": random.randint(1, 10),
            "charge_amount": round(random.uniform(0, 5.0), 2),
            "charge_currency": "USD",
            "rated_volume": random.randint(0, 500),
            "is_roaming": random.random() < 0.05,
            "roaming_zone": "Domestic" if random.random() > 0.05 else random.choice(["EU", "International"]),
        })
    return pd.DataFrame(events)


def main():
    parser = argparse.ArgumentParser(description="Generate raw network CDR-like data to Volume")
    parser.add_argument("--catalog", type=str, default="telecommunications")
    parser.add_argument("--schema", type=str, default="fraud_data")
    parser.add_argument("--volume-name", type=str, default="raw_network_data")
    parser.add_argument("--num-events", type=int, default=150_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume_name}"
    print("Raw Network Data Generator")
    print("=" * 50)
    print(f"Target: {volume_path}")

    ensure_volume_exists(args.catalog, args.schema, args.volume_name)
    device_ids = load_device_ids(args.catalog, args.schema)
    cell_list = load_cell_ids(args.catalog, args.schema)
    print(f"Loaded {len(device_ids):,} device IDs, {len(cell_list):,} cells")

    start_date = datetime(2024, 1, 1)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    raw_data = generate_raw_network_events(
        device_ids, cell_list, args.num_events, start_date, end_date, args.seed
    )
    raw_data["yyyy"] = pd.to_datetime(raw_data["timestamp_start"]).dt.strftime("%Y")
    raw_data["mm"] = pd.to_datetime(raw_data["timestamp_start"]).dt.strftime("%m")
    raw_data["dd"] = pd.to_datetime(raw_data["timestamp_start"]).dt.strftime("%d")

    spark_df = spark.createDataFrame(raw_data)
    spark_df.write.mode("overwrite").partitionBy("yyyy", "mm", "dd").json(volume_path)
    print(f"Raw network data written: {len(raw_data):,} events, partitioned by yyyy/mm/dd")


if __name__ == "__main__":
    main()

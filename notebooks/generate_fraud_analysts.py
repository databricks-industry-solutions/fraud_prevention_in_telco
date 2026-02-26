#!/usr/bin/env python3
"""Generate fraud analyst roster: same number of analysts per state for workload imbalance visibility."""

from __future__ import annotations

import argparse
from typing import List, Tuple

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Same state set as generate_bronze_app_transactions (all states used in transactions)
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
    "VT", "VA", "WA", "WV", "WI", "WY",
]

REGION_BY_STATE = {
    "WEST": ["CA", "OR", "WA", "NV", "AZ", "UT", "ID", "MT", "WY", "CO", "NM", "AK", "HI"],
    "EAST": ["NY", "PA", "NJ", "MA", "CT", "RI", "VT", "NH", "ME", "MD", "DE", "DC"],
    "SOUTH": ["TX", "FL", "GA", "NC", "SC", "VA", "TN", "AL", "MS", "LA", "AR", "KY", "WV"],
    "NORTH": ["IL", "MI", "OH", "IN", "WI", "MN", "IA", "ND", "SD"],
    "CENTER": ["MO", "KS", "NE", "OK"],
}


def _state_to_region(state: str) -> str:
    for region, states in REGION_BY_STATE.items():
        if state in states:
            return region
    return "UNKNOWN"


def _generate_analyst_names(count: int, state: str, seed_offset: int) -> List[Tuple[str, str]]:
    """Generate (analyst_id, analyst_name) with deterministic variety per state."""
    try:
        from faker import Faker
        seed = (hash(state) % (2**32)) + seed_offset
        fake = Faker()
        fake.seed_instance(seed)
    except Exception:
        # Fallback if Faker unavailable or no seed_instance: generic names with state suffix
        return [(f"a_{state}_{i}", f"Analyst {i} ({state})") for i in range(count)]
    names = set()
    out = []
    for i in range(count):
        analyst_id = f"a_{state}_{i}"
        name = fake.name()
        while name in names:
            name = fake.name()
        names.add(name)
        out.append((analyst_id, name))
    return out


def build_fraud_analysts(
    catalog: str = "telecommunications",
    schema: str = "fraud_data",
    analysts_per_state: int = 4,
) -> pd.DataFrame:
    """Build roster with same number of analysts per state."""
    rows = []
    for state in US_STATES:
        region = _state_to_region(state)
        for analyst_id, analyst_name in _generate_analyst_names(analysts_per_state, state, 0):
            rows.append({
                "analyst_id": analyst_id,
                "analyst_name": analyst_name,
                "state": state,
                "region": region,
            })
    return pd.DataFrame(rows)


def main(catalog: str, schema: str, analysts_per_state: int = 4) -> None:
    df = build_fraud_analysts(catalog=catalog, schema=schema, analysts_per_state=analysts_per_state)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.fraud_analysts")
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.fraud_analysts"
    )
    print(f"Saved {len(df)} fraud analysts ({analysts_per_state} per state, {len(US_STATES)} states).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fraud analyst roster by state")
    parser.add_argument("--catalog", default="telecommunications")
    parser.add_argument("--schema", default="fraud_data")
    parser.add_argument("--analysts-per-state", type=int, default=4)
    args = parser.parse_args()
    main(catalog=args.catalog, schema=args.schema, analysts_per_state=args.analysts_per_state)

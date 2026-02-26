#!/usr/bin/env python3
"""
Generate a synthetic base station / cell registry for network pipeline.
Used by Silver_Network_Data to resolve cell_id -> geo (bs_lat, bs_lon, coverage, country/region/city).

Output Table: {catalog}.{schema}.cell_registry
"""

import argparse
import random
from typing import List, Tuple

import pandas as pd
from pyspark.sql import SparkSession

# US state bounding boxes (min_lat, max_lat, min_lon, max_lon) for synthetic cell placement
STATE_BBOX = {
    "CA": (32.5, 42.0, -124.4, -114.1),
    "TX": (25.8, 36.5, -106.7, -93.5),
    "FL": (24.5, 31.0, -87.6, -80.0),
    "NY": (40.5, 45.0, -79.8, -71.9),
    "PA": (39.7, 42.3, -80.5, -74.7),
    "IL": (37.0, 42.5, -91.5, -87.5),
    "OH": (38.4, 42.0, -84.8, -80.5),
    "GA": (30.4, 35.0, -85.6, -80.8),
    "NC": (33.8, 36.6, -84.3, -75.5),
    "MI": (41.7, 48.2, -90.4, -82.4),
    "NJ": (38.9, 41.4, -75.6, -73.9),
    "VA": (36.5, 39.5, -83.7, -75.2),
    "WA": (45.5, 49.0, -124.8, -116.9),
    "AZ": (31.3, 37.0, -114.8, -109.0),
    "MA": (41.2, 42.9, -73.5, -69.9),
    "TN": (35.0, 36.7, -90.3, -81.6),
    "IN": (37.8, 41.8, -88.1, -84.8),
    "MO": (36.0, 40.6, -95.8, -89.1),
    "MD": (37.9, 39.7, -79.5, -75.0),
    "WI": (42.5, 47.1, -92.9, -86.8),
    "CO": (37.0, 41.0, -109.1, -102.0),
    "MN": (43.5, 49.4, -97.2, -89.5),
    "SC": (32.0, 35.2, -83.4, -78.5),
    "AL": (30.2, 35.0, -88.5, -84.9),
    "LA": (29.0, 33.0, -94.0, -89.0),
    "KY": (36.5, 39.1, -89.6, -82.0),
    "OR": (42.0, 46.3, -124.6, -116.5),
    "OK": (33.6, 37.0, -103.0, -94.4),
    "CT": (40.98, 42.05, -73.73, -71.79),
    "UT": (37.0, 42.0, -114.1, -109.0),
    "IA": (40.4, 43.5, -96.6, -90.1),
    "NV": (35.0, 42.0, -120.0, -114.0),
    "AR": (33.0, 36.5, -94.6, -89.6),
    "MS": (30.2, 35.0, -91.7, -88.1),
    "KS": (37.0, 40.0, -102.1, -94.6),
    "NM": (31.3, 37.0, -109.1, -103.0),
    "NE": (40.0, 43.0, -104.1, -95.3),
    "WV": (37.2, 40.6, -82.6, -77.7),
    "ID": (42.0, 49.0, -117.2, -111.0),
    "HI": (18.9, 22.24, -160.3, -154.8),
    "NH": (42.7, 45.3, -72.6, -70.7),
    "ME": (43.1, 47.5, -71.1, -66.9),
    "RI": (41.1, 42.0, -71.9, -71.1),
    "MT": (45.0, 49.0, -116.1, -104.0),
    "DE": (38.45, 39.84, -75.79, -75.05),
    "SD": (42.5, 46.0, -104.1, -96.4),
    "ND": (45.9, 49.0, -104.1, -96.6),
    "AK": (51.2, 71.4, -179.1, -129.9),
    "VT": (42.7, 45.0, -73.4, -71.5),
    "WY": (41.0, 45.0, -111.1, -104.1),
    "DC": (38.79, 39.0, -77.12, -76.91),
}

# State -> (region, major city for labeling)
STATE_REGION_CITY = {
    "CA": ("WEST", "Los Angeles"), "TX": ("SOUTH", "Houston"), "FL": ("SOUTH", "Miami"),
    "NY": ("EAST", "New York"), "PA": ("EAST", "Philadelphia"), "IL": ("NORTH", "Chicago"),
    "OH": ("NORTH", "Columbus"), "GA": ("SOUTH", "Atlanta"), "NC": ("SOUTH", "Charlotte"),
    "MI": ("NORTH", "Detroit"), "NJ": ("EAST", "Newark"), "VA": ("SOUTH", "Richmond"),
    "WA": ("WEST", "Seattle"), "AZ": ("WEST", "Phoenix"), "MA": ("EAST", "Boston"),
    "TN": ("SOUTH", "Nashville"), "IN": ("NORTH", "Indianapolis"), "MO": ("CENTER", "Kansas City"),
    "MD": ("EAST", "Baltimore"), "WI": ("NORTH", "Milwaukee"), "CO": ("WEST", "Denver"),
    "MN": ("NORTH", "Minneapolis"), "SC": ("SOUTH", "Columbia"), "AL": ("SOUTH", "Birmingham"),
    "LA": ("SOUTH", "New Orleans"), "KY": ("SOUTH", "Louisville"), "OR": ("WEST", "Portland"),
    "OK": ("SOUTH", "Oklahoma City"), "CT": ("EAST", "Hartford"), "UT": ("WEST", "Salt Lake City"),
    "IA": ("NORTH", "Des Moines"), "NV": ("WEST", "Las Vegas"), "AR": ("SOUTH", "Little Rock"),
    "MS": ("SOUTH", "Jackson"), "KS": ("CENTER", "Wichita"), "NM": ("WEST", "Albuquerque"),
    "NE": ("CENTER", "Omaha"), "WV": ("EAST", "Charleston"), "ID": ("WEST", "Boise"),
    "HI": ("WEST", "Honolulu"), "NH": ("EAST", "Manchester"), "ME": ("EAST", "Portland"),
    "RI": ("EAST", "Providence"), "MT": ("WEST", "Billings"), "DE": ("EAST", "Wilmington"),
    "SD": ("NORTH", "Sioux Falls"), "ND": ("NORTH", "Fargo"), "AK": ("WEST", "Anchorage"),
    "VT": ("EAST", "Burlington"), "WY": ("WEST", "Cheyenne"), "DC": ("EAST", "Washington"),
}


def generate_cell_registry(
    catalog: str = "telecommunications",
    schema: str = "fraud_data",
    num_cells: int = 5000,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic cell registry with base_station_id, lac, cell_id, geo, coverage."""
    random.seed(seed)
    states = list(STATE_BBOX.keys())
    # Weight by number of cells we want per state (roughly by size)
    weights = [1.0] * len(states)

    rows = []
    cell_idx = 0
    for _ in range(num_cells):
        state = random.choices(states, weights=weights, k=1)[0]
        min_lat, max_lat, min_lon, max_lon = STATE_BBOX[state]
        bs_lat = round(random.uniform(min_lat, max_lat), 6)
        bs_lon = round(random.uniform(min_lon, max_lon), 6)
        region, city = STATE_REGION_CITY.get(state, ("US", "Unknown"))
        env_type = random.choices(
            ["urban", "suburban", "rural"],
            weights=[0.5, 0.35, 0.15],
            k=1,
        )[0]
        if env_type == "urban":
            nominal_radius_m = random.randint(500, 2000)
        elif env_type == "suburban":
            nominal_radius_m = random.randint(2000, 5000)
        else:
            nominal_radius_m = random.randint(5000, 15000)
        lac = random.randint(1000, 99999)
        sector_id = random.choice(["alpha", "beta", "gamma"])
        base_station_id = f"bs_{state}_{cell_idx % 1000:04d}"
        cell_id = f"cell_{cell_idx:06d}"
        sector_azimuth_deg = random.randint(0, 359)
        sector_beamwidth_deg = random.choice([65, 90, 120])
        rows.append({
            "base_station_id": base_station_id,
            "lac": lac,
            "cell_id": cell_id,
            "sector_id": sector_id,
            "bs_lat": bs_lat,
            "bs_lon": bs_lon,
            "sector_azimuth_deg": sector_azimuth_deg,
            "sector_beamwidth_deg": sector_beamwidth_deg,
            "env_type": env_type,
            "nominal_radius_m": nominal_radius_m,
            "country": "US",
            "region": region,
            "city": city,
        })
        cell_idx += 1

    df = pd.DataFrame(rows)
    spark = SparkSession.getActiveSession()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.cell_registry")
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.cell_registry")
    print(f"✓ Cell registry created: {catalog}.{schema}.cell_registry with {len(df):,} cells")
    return df


def main():
    parser = argparse.ArgumentParser(description="Generate cell/base station registry")
    parser.add_argument("--catalog", type=str, default="telecommunications")
    parser.add_argument("--schema", type=str, default="fraud_data")
    parser.add_argument("--num-cells", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_cell_registry(args.catalog, args.schema, args.num_cells, args.seed)


if __name__ == "__main__":
    main()

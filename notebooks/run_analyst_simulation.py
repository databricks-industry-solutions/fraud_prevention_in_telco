#!/usr/bin/env python3
"""Entrypoint for running the analyst review simulation."""

import argparse
from analysts_notes import run_analyst_simulation


def main() -> None:
    parser = argparse.ArgumentParser(description="Run analyst simulation")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    
    args = parser.parse_args()
    
    print(f"Running analyst simulation on {args.catalog}.{args.schema}")
    run_analyst_simulation(catalog=args.catalog, schema=args.schema)


if __name__ == "__main__":
    main()

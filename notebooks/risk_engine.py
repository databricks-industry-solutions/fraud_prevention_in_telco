#!/usr/bin/env python3
"""
Risk Engine baseline scoring.
Calculates fraud scores and engine-level labels, saving to transaction_risk_engine.
"""

from __future__ import annotations

import argparse
import pandas as pd
import numpy as np
import random
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()


class RiskEngine:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema
        # Target distribution for fraud_score
        self.score_distribution = {
            "very_high": {"min": 95, "max": 100, "weight": 0.10},
            "high": {"min": 70, "max": 95, "weight": 0.10},
            "low": {"min": 0, "max": 70, "weight": 0.80},
        }

        self.fraud_rate = 0.2663  # original target for legacy parity
        self.block_threshold = 95
        self.pending_threshold = 70

        self.engine_reasons = {
            "blocked": [
                "Device Compromise",
                "Geographic Anomaly",
                "SIM Swap",
                "Account Takeover",
            ],
            "pending_review": [
                "Suspicious Device",
                "Velocity Anomaly",
                "MFA Bypass Attempt",
                "Dark Web Exposure",
            ],
            "passed": [
                "Normal Transaction Pattern",
                "Known Device and Location",
                "Stable Account Behaviour",
            ],
        }

    def load_gold_layer(self) -> pd.DataFrame:
        """Load gold layer data from catalog."""
        print("Loading gold layer data...")
        gold_data = spark.read.table(f"{self.catalog}.{self.schema}.gold_app_transactions")
        return gold_data.toPandas()

    def calculate_fraud_scores(self, gold_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate fraud scores based on risk features from gold layer."""
        print("Calculating fraud scores...")

        risk_features = gold_data[
            [
                "transaction_id",
                "subscriber_geo_velocity_kmph",
                "subscriber_high_value_device_amount_flag",
                "subscriber_after_hours_txn_flag",
                "subscriber_device_novelty_flag",
                "has_mfa",
                "subscriber_mfa_anomaly_score",
                "subscriber_login_failure_burst_cnt",
                "subscriber_pwd_reset_cnt_24h",
                "subscriber_pwd_reset_txn_gap_min",
                "previous_txn_count_1h",
                "previous_txn_count_24h",
                "previous_txn_amount_1h",
                "previous_txn_amount_24h",
                "subscriber_sim_swap_recent_flag",
                "subscriber_profile_change_cnt_24h",
                "subscriber_darkweb_breach_flag",
                "device_trust_score",
            ]
        ].copy()

        risk_features["base_score"] = 0.0
        risk_features["base_score"] += (
            (risk_features["subscriber_geo_velocity_kmph"] > 0).astype(int) * 15
        )
        risk_features["base_score"] += (
            (risk_features["subscriber_geo_velocity_kmph"] > 50).astype(int) * 10
        )
        risk_features["base_score"] += (
            risk_features["subscriber_high_value_device_amount_flag"].astype(int) * 12
        )
        risk_features["base_score"] += (
            risk_features["subscriber_after_hours_txn_flag"].astype(int) * 5
        )
        risk_features["base_score"] += (
            risk_features["subscriber_device_novelty_flag"].astype(int) * 8
        )
        risk_features["base_score"] += (1 - risk_features["has_mfa"].astype(int)) * 10
        risk_features["base_score"] += (
            risk_features["subscriber_mfa_anomaly_score"] * 0.15
        )
        risk_features["base_score"] += (
            (risk_features["subscriber_login_failure_burst_cnt"] > 0).astype(int) * 12
        )
        risk_features["base_score"] += (
            risk_features["subscriber_pwd_reset_cnt_24h"].astype(int) * 7
        )
        risk_features["base_score"] += (
            risk_features["subscriber_sim_swap_recent_flag"].astype(int) * 20
        )
        risk_features["base_score"] += (
            risk_features["subscriber_profile_change_cnt_24h"] * 3
        ).clip(upper=15)
        risk_features["base_score"] += (
            risk_features["subscriber_darkweb_breach_flag"].astype(int) * 18
        )
        risk_features["base_score"] += (
            (100 - risk_features["device_trust_score"]) * 0.2
        ).clip(upper=15)

        risk_features["base_score"] = risk_features["base_score"].clip(0, 100)

        num_transactions = len(risk_features)
        score_buckets = random.choices(
            list(self.score_distribution.keys()),
            weights=[
                self.score_distribution[k]["weight"] for k in self.score_distribution
            ],
            k=num_transactions,
        )

        fraud_scores = []
        for bucket in score_buckets:
            config = self.score_distribution[bucket]
            score = random.uniform(config["min"], config["max"])
            fraud_scores.append(round(score, 2))

        result = gold_data.copy()
        result["fraud_score"] = fraud_scores
        return result

    def assign_engine_labels(self, data: pd.DataFrame) -> pd.DataFrame:
        """Assign engine-level fraud flags based on score distribution."""
        print("Assigning engine fraud labels...")

        data_sorted = data.sort_values("fraud_score", ascending=False).copy()
        num_fraud = int(len(data_sorted) * self.fraud_rate)

        data_sorted["fraud_label_engine"] = 0
        data_sorted.iloc[
            :num_fraud, data_sorted.columns.get_loc("fraud_label_engine")
        ] = 1

        data_sorted = data_sorted.sort_values("transaction_id").reset_index(drop=True)

        # Ensure all high-risk scores are flagged by the engine
        data_sorted.loc[
            data_sorted["fraud_score"] >= self.pending_threshold, "fraud_label_engine"
        ] = 1
        data_sorted["fraud_label_engine"] = data_sorted["fraud_label_engine"].astype(int)
        return data_sorted

    def generate_engine_assessment(self, data: pd.DataFrame) -> pd.DataFrame:
        """Derive engine risk status and reasons."""
        print("Generating engine assessment...")

        statuses: list[str] = []
        reasons: list[str] = []
        exposures: list[float] = []

        for _, row in data.iterrows():
            score = row['fraud_score']
            amount = float(row.get('purchased_device_service_price', 0.0))

            if score >= self.block_threshold:
                status = 'blocked'
            elif score >= self.pending_threshold:
                status = 'pending_review'
            else:
                status = 'passed'

            statuses.append(status)
            exposures.append(amount)

            # Determine primary risk driver using feature flags (ordered by severity)
            if row.get('subscriber_sim_swap_recent_flag'):
                reason = 'SIM swap detected'
            elif row.get('subscriber_high_value_device_amount_flag'):
                reason = 'High value transaction'
            elif not row.get('has_mfa', True):
                reason = 'MFA disabled'
            elif row.get('subscriber_darkweb_breach_flag'):
                reason = 'Dark web breach exposure'
            elif row.get('subscriber_login_failure_burst_cnt', 0) > 0:
                reason = 'Login failure burst'
            elif row.get('subscriber_after_hours_txn_flag'):
                reason = 'After hours activity'
            elif row.get('subscriber_geo_velocity_kmph', 0) > 200:
                reason = 'Geo velocity anomaly'
            elif row.get('subscriber_profile_change_cnt_24h', 0) > 3:
                reason = 'Profile change spike'
            else:
                reason = 'Composite risk signals'

            reasons.append(reason)

        data['risk_status_engine'] = statuses
        data['fraud_root_cause_engine'] = reasons
        data['case_exposure_usd'] = exposures
        data['risk_assessment_timestamp'] = datetime.now()
        data['risk_reason_engine'] = reasons
        return data

    def build_engine_table(self) -> pd.DataFrame:
        """End-to-end creation of the transaction_risk_engine table."""
        gold = self.load_gold_layer()
        scored = self.calculate_fraud_scores(gold)
        labeled = self.assign_engine_labels(scored)
        engine_data = self.generate_engine_assessment(labeled)

        engine_data = engine_data.rename(
            columns={
                "transaction_timestamp": "transaction_date",
                "purchased_device_service_price": "transaction_cost",
            }
        )

        engine_table = engine_data[
            [
                "transaction_id",
                "transaction_date",
                "transaction_state",
                "transaction_region",
                "transaction_type",
                "transaction_subtype",
                "transaction_cost",
                "account_id",
                "customer_user_id",
                "customer_name",
                "account_services",
                "account_detail",
                "high_risk_flag",
                "subscriber_location_lat",
                "subscriber_location_long",
                "fraud_score",
                "fraud_label_engine",
                "risk_status_engine",
                "risk_reason_engine",
                "fraud_root_cause_engine",
                "case_exposure_usd",
                "risk_assessment_timestamp",
            ]
        ].copy()

        engine_table = self._apply_engine_types(engine_table)

        print(
            "Engine assessment summary:",
            f"{(engine_table['fraud_label_engine']).mean():.2%} flagged",
        )
        print(
            "Risk status counts:",
            engine_table["risk_status_engine"].value_counts(normalize=True),
        )
        return engine_table

    def _apply_engine_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply consistent types for engine output."""
        data["transaction_date"] = pd.to_datetime(data["transaction_date"])
        data["transaction_cost"] = data["transaction_cost"].astype(float)
        data["fraud_score"] = data["fraud_score"].astype(float)
        data["fraud_label_engine"] = data["fraud_label_engine"].astype(int)
        data["high_risk_flag"] = data["high_risk_flag"].astype(bool)
        data["case_exposure_usd"] = data["case_exposure_usd"].astype(float)
        data["risk_assessment_timestamp"] = pd.to_datetime(
            data["risk_assessment_timestamp"]
        )
        data["subscriber_location_lat"] = data["subscriber_location_lat"].astype(float)
        data["subscriber_location_long"] = data["subscriber_location_long"].astype(float)

        string_cols = [
            "transaction_id",
            "transaction_state",
            "transaction_region",
            "transaction_type",
            "transaction_subtype",
            "account_id",
            "customer_user_id",
            "customer_name",
            "account_services",
            "account_detail",
            "risk_status_engine",
            "fraud_root_cause_engine",
        ]
        for col in string_cols:
            data[col] = data[col].astype(str)

        return data

    def save_engine_table(self, engine_table: pd.DataFrame) -> None:
        """Persist engine baseline to Delta."""
        print(
            f"Saving engine baseline to {self.catalog}.{self.schema}.transaction_risk_engine..."
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.transaction_risk_engine")
        spark_df = spark.createDataFrame(engine_table)
        spark_df.write.mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.schema}.transaction_risk_engine"
        )
        print(f"Saved {len(engine_table)} engine records.")


def main():
    parser = argparse.ArgumentParser(description="Run risk engine on transaction data")
    parser.add_argument("--catalog", type=str, default="telecommunications",
                       help="Unity Catalog name (default: telecommunications)")
    parser.add_argument("--schema", type=str, default="fraud_data",
                       help="Schema name (default: fraud_data)")
    
    args = parser.parse_args()
    
    print("Transaction Risk Engine")
    print("=" * 50)
    print(f"Target: {args.catalog}.{args.schema}")
    
    engine = RiskEngine(catalog=args.catalog, schema=args.schema)
    engine_table = engine.build_engine_table()
    engine.save_engine_table(engine_table)
    print("Risk engine baseline complete.")


if __name__ == "__main__":
    main()


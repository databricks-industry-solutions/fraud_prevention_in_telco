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

# Earth radius in km for haversine
_EARTH_RADIUS_KM = 6371.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between (lat1,lon1) and (lat2,lon2)."""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.minimum(a, 1.0)))


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

    def load_gold_transactions(self) -> pd.DataFrame:
        """Load gold transaction layer from catalog."""
        print("Loading gold transaction layer...")
        gold_data = spark.read.table(
            f"{self.catalog}.{self.schema}.gold_app_transactions"
        )
        return gold_data.toPandas()

    def load_gold_device_sdk(self) -> pd.DataFrame:
        """Load gold device SDK layer from catalog."""
        print("Loading gold device SDK layer...")
        gold_device = spark.read.table(
            f"{self.catalog}.{self.schema}.gold_device_sdk"
        )
        return gold_device.toPandas()

    def load_gold_network_data(self) -> pd.DataFrame:
        """Load gold network (CDR-like) layer from catalog."""
        print("Loading gold network data...")
        gold_network = spark.read.table(
            f"{self.catalog}.{self.schema}.gold_network_data"
        )
        return gold_network.toPandas()

    def compute_network_rule_features(self, network_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute per-device network rule flags from gold_network_data (R1, R2, R5, R7, R13).
        Returns one row per subscriber_device_id with boolean rule columns.
        """
        if network_df is None or len(network_df) == 0:
            return pd.DataFrame(columns=["subscriber_device_id"])

        required = ["subscriber_device_id", "timestamp_start", "geo_lat_est", "geo_lon_est"]
        if not all(c in network_df.columns for c in required):
            return pd.DataFrame(columns=["subscriber_device_id"])

        network_df = network_df.copy()
        network_df["timestamp_start"] = pd.to_datetime(network_df["timestamp_start"])
        network_df["geo_lat_est"] = pd.to_numeric(network_df["geo_lat_est"], errors="coerce")
        network_df["geo_lon_est"] = pd.to_numeric(network_df["geo_lon_est"], errors="coerce")
        network_df = network_df.dropna(subset=["geo_lat_est", "geo_lon_est"])
        if len(network_df) == 0:
            return pd.DataFrame(columns=["subscriber_device_id"])

        rows = []
        for device_id, grp in network_df.groupby("subscriber_device_id"):
            grp = grp.sort_values("timestamp_start").reset_index(drop=True)
            impossible_high = False
            impossible_medium = False
            cell_ip_mismatch = False
            rapid_cell_hop = False
            roaming_anomaly = False

            # R1/R2: Impossible travel (consecutive events)
            for i in range(1, len(grp)):
                lat1 = grp.iloc[i - 1]["geo_lat_est"]
                lon1 = grp.iloc[i - 1]["geo_lon_est"]
                lat2 = grp.iloc[i]["geo_lat_est"]
                lon2 = grp.iloc[i]["geo_lon_est"]
                t1 = grp.iloc[i - 1]["timestamp_start"]
                t2 = grp.iloc[i]["timestamp_start"]
                delta_hr = (t2 - t1).total_seconds() / 3600.0
                if delta_hr <= 0:
                    continue
                dist_km = _haversine_km(lat1, lon1, lat2, lon2)
                speed_kmph = dist_km / delta_hr
                if dist_km > 100 and speed_kmph > 1000:
                    impossible_high = True
                if speed_kmph > 250:
                    impossible_medium = True

            # R5: Cell country != IP country (high-risk event types)
            if "geo_country" in grp.columns and "ip_geo_country" in grp.columns:
                mismatch = (grp["geo_country"].astype(str).str.strip() != grp["ip_geo_country"].astype(str).str.strip())
                if mismatch.any():
                    cell_ip_mismatch = True

            # R7: Consecutive events > 50 km apart with time_delta < 5 min
            for i in range(1, len(grp)):
                lat1 = grp.iloc[i - 1]["geo_lat_est"]
                lon1 = grp.iloc[i - 1]["geo_lon_est"]
                lat2 = grp.iloc[i]["geo_lat_est"]
                lon2 = grp.iloc[i]["geo_lon_est"]
                t1 = grp.iloc[i - 1]["timestamp_start"]
                t2 = grp.iloc[i]["timestamp_start"]
                delta_min = (t2 - t1).total_seconds() / 60.0
                dist_km = _haversine_km(lat1, lon1, lat2, lon2)
                if dist_km > 50 and delta_min < 5:
                    rapid_cell_hop = True
                    break

            # R13: Roaming - multiple countries in short window (simplified)
            if "geo_country" in grp.columns and "is_roaming" in grp.columns:
                roaming = grp[grp["is_roaming"].fillna(False).astype(bool)]
                if len(roaming) >= 2:
                    countries = roaming["geo_country"].astype(str).unique()
                    if len(countries) >= 2:
                        time_span_hr = (roaming["timestamp_start"].max() - roaming["timestamp_start"].min()).total_seconds() / 3600.0
                        if time_span_hr < 2:
                            roaming_anomaly = True

            rows.append({
                "subscriber_device_id": device_id,
                "network_impossible_travel_high": impossible_high,
                "network_impossible_travel_medium": impossible_medium,
                "network_cell_ip_mismatch": cell_ip_mismatch,
                "network_rapid_cell_hop": rapid_cell_hop,
                "network_roaming_anomaly": roaming_anomaly,
            })

        return pd.DataFrame(rows)

    def join_gold_transactions_with_device(
        self, gold_transactions: pd.DataFrame, gold_device: pd.DataFrame
    ) -> pd.DataFrame:
        """Left-join transaction gold to device gold on subscriber_device_id."""
        print("Joining gold transactions with gold device SDK...")
        # Suffix device columns where names collide (e.g. location, timezone)
        merged = gold_transactions.merge(
            gold_device,
            on="subscriber_device_id",
            how="left",
            suffixes=("", "_device"),
        )
        print(
            f"Joined {len(merged)} transactions; "
            f"{merged['subscriber_device_id'].isin(gold_device['subscriber_device_id']).sum()} with device match"
        )
        return merged

    def join_gold_with_network_features(
        self, gold_merged: pd.DataFrame, network_features: pd.DataFrame
    ) -> pd.DataFrame:
        """Left-join network rule features to gold (transactions + device) on subscriber_device_id."""
        if network_features is None or len(network_features) == 0:
            for col in [
                "network_impossible_travel_high",
                "network_impossible_travel_medium",
                "network_cell_ip_mismatch",
                "network_rapid_cell_hop",
                "network_roaming_anomaly",
            ]:
                gold_merged[col] = False
            return gold_merged
        merged = gold_merged.merge(
            network_features,
            on="subscriber_device_id",
            how="left",
        )
        for col in [
            "network_impossible_travel_high",
            "network_impossible_travel_medium",
            "network_cell_ip_mismatch",
            "network_rapid_cell_hop",
            "network_roaming_anomaly",
        ]:
            if col in merged.columns:
                merged[col] = merged[col].fillna(False).astype(bool)
        print(
            f"Joined network features; "
            f"{merged['subscriber_device_id'].isin(network_features['subscriber_device_id']).sum()} with network features"
        )
        return merged

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

        # Device SDK-based risk (from gold_device_sdk when joined)
        if "subscriber_vpn_active" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["subscriber_vpn_active"].fillna(False).astype(int) * 6
            )
        if "subscriber_vpn_connected" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["subscriber_vpn_connected"].fillna(False).astype(int) * 6
            )
        if "subscriber_device_encryption" in gold_data.columns:
            risk_features["base_score"] += (
                (
                    gold_data["subscriber_device_encryption"].astype(str).str.lower()
                    == "unencrypted"
                ).astype(int)
                * 10
            )
        if "subscriber_selinux_status" in gold_data.columns:
            risk_features["base_score"] += (
                (
                    gold_data["subscriber_selinux_status"].astype(str).str.lower()
                    == "disabled"
                ).astype(int)
                * 10
            )
        if "subscriber_device_model" in gold_data.columns:
            emulator = (
                gold_data["subscriber_device_model"]
                .astype(str)
                .str.contains("SDK built for x86", case=False, na=False)
            )
            risk_features["base_score"] += emulator.astype(int) * 12

        # Network (CDR) rule-based risk (R1, R2, R5, R7, R13)
        if "network_impossible_travel_high" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["network_impossible_travel_high"].fillna(False).astype(int) * 15
            )
        if "network_impossible_travel_medium" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["network_impossible_travel_medium"].fillna(False).astype(int) * 8
            )
        if "network_cell_ip_mismatch" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["network_cell_ip_mismatch"].fillna(False).astype(int) * 10
            )
        if "network_rapid_cell_hop" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["network_rapid_cell_hop"].fillna(False).astype(int) * 12
            )
        if "network_roaming_anomaly" in gold_data.columns:
            risk_features["base_score"] += (
                gold_data["network_roaming_anomaly"].fillna(False).astype(int) * 10
            )

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
            elif row.get('network_impossible_travel_high'):
                reason = 'Impossible travel (network)'
            elif row.get('network_impossible_travel_medium'):
                reason = 'High implied speed (network)'
            elif row.get('network_cell_ip_mismatch'):
                reason = 'Cell vs IP location mismatch'
            elif row.get('network_rapid_cell_hop'):
                reason = 'Rapid cell tower hop'
            elif row.get('network_roaming_anomaly'):
                reason = 'Roaming / country jump anomaly'
            elif row.get('subscriber_vpn_active') or row.get('subscriber_vpn_connected'):
                reason = 'Suspicious device (VPN)'
            elif (
                str(row.get('subscriber_device_encryption', '')).lower()
                == 'unencrypted'
            ):
                reason = 'Suspicious device (unencrypted)'
            elif (
                str(row.get('subscriber_selinux_status', '')).lower()
                == 'disabled'
            ):
                reason = 'Suspicious device (SELinux disabled)'
            elif pd.notna(row.get('subscriber_device_model')) and 'SDK built for x86' in str(row.get('subscriber_device_model', '')):
                reason = 'Suspicious device (emulator)'
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
        gold_transactions = self.load_gold_transactions()
        gold_device = self.load_gold_device_sdk()
        gold = self.join_gold_transactions_with_device(
            gold_transactions, gold_device
        )
        try:
            gold_network = self.load_gold_network_data()
            network_features = self.compute_network_rule_features(gold_network)
            gold = self.join_gold_with_network_features(gold, network_features)
        except Exception as e:
            print(f"Gold network data not available or error: {e}; continuing without network rules.")
            gold = self.join_gold_with_network_features(gold, pd.DataFrame())
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


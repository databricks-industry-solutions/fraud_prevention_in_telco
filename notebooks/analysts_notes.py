#!/usr/bin/env python3
"""Simulate analyst review workflow on top of risk engine baseline."""

from __future__ import annotations

import random
from datetime import timedelta
from typing import Tuple

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

ANALYSTS = [
    "Sarah Johnson",
    "Michael Chen",
    "Emily Rodriguez",
    "David Kim",
    "Jessica Martinez",
    "James Wilson",
    "Maria Garcia",
    "Robert Taylor",
    "Lisa Anderson",
]

ESCALATION_NOTES = [
    "Escalated to SME for second-level investigation.",
    "Escalated to third-party fraud partner for validation.",
    "Transferred to specialist queue for deeper device forensics.",
]

MITIGATION_FRAUD = [
    "Account locked and customer notified.",
    "Charge reversed and SIM reissued.",
    "Credentials reset and MFA enforced.",
]

MITIGATION_FALSE_POSITIVE = [
    "Block lifted and customer apologised.",
    "Case closed as legitimate; customer reassured.",
    "Risk flags cleared after verification call.",
]

MITIGATION_ESCALATED = [
    "Awaiting SME response before final action.",
    "Pending external review; monitoring account activity.",
]


class AnalystReview:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.catalog = catalog
        self.schema = schema
        self.engine_table = self._load_engine_table()

    def _load_engine_table(self) -> pd.DataFrame:
        print("Loading transaction_risk_engine baseline...")
        engine = spark.read.table(
            f"{self.catalog}.{self.schema}.transaction_risk_engine"
        )
        return engine.toPandas()

    def _initialise_review_frame(self) -> pd.DataFrame:
        df = self.engine_table.copy()
        df["review_status"] = "pending_review"
        df["assigned_analyst"] = None
        df["fraud_label_analyst"] = np.nan
        df["analyst_notes"] = ""
        df["last_review_date"] = pd.NaT
        df["mitigation_steps"] = ""
        df["is_fp"] = np.nan
        df["is_fn"] = np.nan
        return df

    def _select_indices(
        self, mask: pd.Series, review_ratio: float, escalate_ratio: float
    ) -> Tuple[set[int], set[int]]:
        candidates = list(self.review_frame.index[mask])
        random.shuffle(candidates)
        review_count = int(len(candidates) * review_ratio)
        escalate_count = int(len(candidates) * escalate_ratio)

        review_indices = set(candidates[:review_count])
        remaining = [idx for idx in candidates if idx not in review_indices]
        escalate_indices = set(remaining[:escalate_count])
        return review_indices, escalate_indices

    def _apply_review_updates(self, indices: set[int]) -> None:
        for idx in indices:
            row = self.review_frame.loc[idx]
            self.review_frame.at[idx, "review_status"] = "reviewed"
            self.review_frame.at[idx, "assigned_analyst"] = random.choice(ANALYSTS)

            if row["risk_status_engine"] == "passed":
                fraud_choice = random.random() < 0.35
            else:
                fraud_choice = random.random() < 0.85

            self.review_frame.at[idx, "fraud_label_analyst"] = 1 if fraud_choice else 0

            notes_pool = (
                [
                    "Analyst confirmed fraud indicators and took action.",
                    "Fraud confirmed after customer outreach.",
                    "Fraudulent behaviour validated by analyst review.",
                ]
                if fraud_choice
                else [
                    "Analyst cleared transaction as legitimate.",
                    "False positive; customer verified successfully.",
                    "Investigation complete – no fraud detected.",
                ]
            )
            self.review_frame.at[idx, "analyst_notes"] = random.choice(notes_pool)

            txn_date = row["transaction_date"]
            offset_hours = random.randint(6, 72)
            self.review_frame.at[idx, "last_review_date"] = txn_date + timedelta(
                hours=offset_hours
            )

            mitigation_pool = (
                MITIGATION_FRAUD if fraud_choice else MITIGATION_FALSE_POSITIVE
            )
            self.review_frame.at[idx, "mitigation_steps"] = random.choice(
                mitigation_pool
            )

            engine_label = row["fraud_label_engine"]
            is_fp = bool(engine_label == 1 and not fraud_choice)
            is_fn = bool(engine_label == 0 and fraud_choice)
            self.review_frame.at[idx, "is_fp"] = is_fp
            self.review_frame.at[idx, "is_fn"] = is_fn

    def _apply_escalation_updates(self, indices: set[int]) -> None:
        for idx in indices:
            row = self.review_frame.loc[idx]
            self.review_frame.at[idx, "review_status"] = "escalated"
            self.review_frame.at[idx, "analyst_notes"] = random.choice(ESCALATION_NOTES)
            txn_date = row["transaction_date"]
            offset_hours = random.randint(12, 96)
            self.review_frame.at[idx, "last_review_date"] = txn_date + timedelta(
                hours=offset_hours
            )
            self.review_frame.at[idx, "mitigation_steps"] = random.choice(
                MITIGATION_ESCALATED
            )

    def _review_high_risk(self) -> None:
        mask = self.review_frame["risk_status_engine"] == "pending_review"
        review_indices, escalate_indices = self._select_indices(mask, 0.45, 0.25)
        self._apply_review_updates(review_indices)
        self._apply_escalation_updates(escalate_indices)

    def _review_blocked(self) -> None:
        mask = self.review_frame["risk_status_engine"] == "blocked"
        review_indices, escalate_indices = self._select_indices(mask, 0.60, 0.20)
        self._apply_review_updates(review_indices)
        self._apply_escalation_updates(escalate_indices)

    def _spot_check_passed(self) -> None:
        mask = self.review_frame["risk_status_engine"] == "passed"
        review_indices, _ = self._select_indices(mask, 0.08, 0.0)
        self._apply_review_updates(review_indices)

    def _finalise_pending(self) -> None:
        pending_mask = self.review_frame["review_status"] == "pending_review"
        self.review_frame.loc[pending_mask, "analyst_notes"] = ""
        self.review_frame.loc[pending_mask, "mitigation_steps"] = ""

    def build_analyst_review(self) -> pd.DataFrame:
        print("Simulating analyst workflow...")
        self.review_frame = self._initialise_review_frame()
        self._review_high_risk()
        self._review_blocked()
        self._spot_check_passed()
        self._finalise_pending()

        for col in ["last_review_date"]:
            self.review_frame[col] = pd.to_datetime(
                self.review_frame[col], errors="coerce"
            )
        return self.review_frame[
            [
                "transaction_id",
                "review_status",
                "assigned_analyst",
                "fraud_label_analyst",
                "analyst_notes",
                "last_review_date",
                "mitigation_steps",
                "is_fp",
                "is_fn",
            ]
        ].copy()

    def merge_with_engine(self, analyst: pd.DataFrame) -> pd.DataFrame:
        print("Merging engine baseline with analyst outcomes...")
        engine = self.engine_table.copy()
        if "review_status" in engine.columns:
            engine = engine.drop(columns=["review_status"], errors="ignore")

        merged = engine.merge(analyst, on="transaction_id", how="left", suffixes=("_engine", ""))

        merged["fraud_label"] = merged["fraud_label_analyst"].where(
            merged["fraud_label_analyst"].notna(), merged["fraud_label_engine"]
        )

        merged["fraud_root_cause"] = merged["fraud_root_cause_engine"]
        reviewed_mask = merged["review_status"] == "reviewed"
        confirmed_fraud = reviewed_mask & (merged["fraud_label_analyst"] == 1)
        cleared_cases = reviewed_mask & (merged["fraud_label_analyst"] == 0)

        merged.loc[confirmed_fraud, "fraud_root_cause"] = "Confirmed Fraud"
        merged.loc[cleared_cases & (merged["fraud_label_engine"] == 1), "fraud_root_cause"] = "False Positive"
        merged.loc[cleared_cases & (merged["fraud_label_engine"] == 0), "fraud_root_cause"] = "Legitimate Transaction"
        merged.loc[merged["review_status"] == "escalated", "fraud_root_cause"] = "Escalated for SME review"
        merged.loc[merged["review_status"] == "pending_review", "fraud_root_cause"] = "Awaiting analyst triage"

        merged["is_fp"] = np.where(reviewed_mask, merged["is_fp"].fillna(False), None)
        merged["is_fn"] = np.where(reviewed_mask, merged["is_fn"].fillna(False), None)

        for col in ["review_status", "assigned_analyst", "analyst_notes", "mitigation_steps"]:
            merged[col] = merged[col].fillna("")

        merged["last_review_date"] = pd.to_datetime(merged["last_review_date"], errors="coerce")
        merged["subscriber_location_lat"] = merged["subscriber_location_lat"].astype(float)
        merged["subscriber_location_long"] = merged["subscriber_location_long"].astype(float)

        ordered = merged[
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
                "fraud_label",
                "fraud_label_analyst",
                "risk_status_engine",
                "risk_reason_engine",
                "review_status",
                "assigned_analyst",
                "analyst_notes",
                "last_review_date",
                "mitigation_steps",
                "fraud_root_cause",
                "case_exposure_usd",
                "is_fp",
                "is_fn",
                "risk_assessment_timestamp",
            ]
        ].copy()
        return ordered

    def save_tables(self, analyst: pd.DataFrame, final: pd.DataFrame) -> None:
        print("Saving analyst_review and transaction_risk tables...")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.analyst_review")
        spark.createDataFrame(analyst).write.mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.schema}.analyst_review"
        )
        spark.sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.schema}.transaction_risk")
        spark.createDataFrame(final).write.mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.schema}.transaction_risk"
        )
        print(f"Saved {len(analyst)} analyst review rows and {len(final)} final transaction risk rows.")


def run_analyst_simulation(catalog: str = "telecommunications", schema: str = "fraud_data") -> None:
    review = AnalystReview(catalog=catalog, schema=schema)
    analyst_table = review.build_analyst_review()
    final_table = review.merge_with_engine(analyst_table)
    review.save_tables(analyst_table, final_table)
    print("Analyst simulation complete.")

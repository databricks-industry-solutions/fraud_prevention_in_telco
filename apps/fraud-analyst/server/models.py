"""Pydantic models for the Fraud Analyst Workbench API."""

from pydantic import BaseModel, Field
from typing import Literal, Optional


class CaseListItem(BaseModel):
    transaction_id: str
    customer_name: Optional[str] = None
    transaction_type: Optional[str] = None
    transaction_cost: Optional[str] = None
    fraud_score: Optional[str] = None
    risk_status_engine: Optional[str] = None
    review_status: Optional[str] = None
    transaction_date: Optional[str] = None
    transaction_region: Optional[str] = None
    high_risk_flag: Optional[str] = None


class CaseDetail(BaseModel):
    transaction_id: str
    transaction_date: Optional[str] = None
    transaction_state: Optional[str] = None
    transaction_region: Optional[str] = None
    transaction_type: Optional[str] = None
    transaction_subtype: Optional[str] = None
    transaction_cost: Optional[str] = None
    account_id: Optional[str] = None
    customer_user_id: Optional[str] = None
    customer_name: Optional[str] = None
    account_services: Optional[str] = None
    fraud_score: Optional[str] = None
    fraud_label_engine: Optional[str] = None
    fraud_label: Optional[str] = None
    risk_status_engine: Optional[str] = None
    risk_reason_engine: Optional[str] = None
    review_status: Optional[str] = None
    assigned_analyst: Optional[str] = None
    analyst_notes: Optional[str] = None
    mitigation_steps: Optional[str] = None
    fraud_root_cause: Optional[str] = None
    case_exposure_usd: Optional[str] = None
    subscriber_location_lat: Optional[str] = None
    subscriber_location_long: Optional[str] = None
    high_risk_flag: Optional[str] = None
    risk_assessment_timestamp: Optional[str] = None


class DeviceProfile(BaseModel):
    device_id: Optional[str] = None
    subscriber_device_model: Optional[str] = None
    subscriber_device_board: Optional[str] = None
    subscriber_os_version: Optional[str] = None
    subscriber_device_ram: Optional[str] = None
    subscriber_device_storage: Optional[str] = None
    subscriber_vpn_active: Optional[str] = None
    subscriber_device_encryption: Optional[str] = None
    subscriber_selinux_status: Optional[str] = None
    is_fraudulent: Optional[str] = None
    fraud_type: Optional[str] = None


class CaseActionRequest(BaseModel):
    decision: Literal["confirmed_fraud", "false_positive", "false_negative", "escalated", "reviewed"]
    analyst_name: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9 .\-']+$")
    notes: Optional[str] = Field(default="", max_length=5000)
    mitigation_action: Optional[str] = Field(default="", max_length=2000)
    confidence_level: Optional[str] = Field(default="medium", max_length=20)


class DashboardStats(BaseModel):
    total_cases: int = 0
    pending_count: int = 0
    confirmed_fraud_count: int = 0
    false_positive_count: int = 0
    escalated_count: int = 0
    avg_fraud_score: float = 0.0
    total_exposure_usd: float = 0.0
    high_risk_count: int = 0


class GeoDataItem(BaseModel):
    transaction_region: str
    case_count: int = 0
    total_exposure: float = 0.0
    avg_score: float = 0.0

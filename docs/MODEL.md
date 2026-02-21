# Fraud Detection Model Documentation

Comprehensive documentation for the Telecom Fraud Detection risk scoring model and detection capabilities.

## Overview

The fraud detection system uses a **rule-based risk scoring engine** that evaluates transactions based on multiple risk indicators. The model generates synthetic fraud data and calculates risk scores (0-100) to classify transactions as safe, pending review, or blocked.

**Note**: This is a **synthetic data generation pipeline** with simulated fraud detection logic. It does not use machine learning models, and performance metrics (accuracy, precision, recall) are not calculated or tracked in the current implementation.

---

## Key Features

### SIM Hijacking Detection

The system detects SIM hijacking through multiple indicators:

- **SIM Swap Events**: Recent SIM card changes flagged via `subscriber_sim_swap_recent_flag` (20 risk points)
- **Device Security Indicators**: 
  - Unlocked devices (`subscriber_device_locked`)
  - No PIN protection (`subscriber_pin_enabled = False`)
  - Disabled encryption (`subscriber_device_encryption = 'unencrypted'`)
  - Root/jailbreak access (`subscriber_system_user = False`, `subscriber_selinux_status = 'disabled'`)
- **Location Anomalies**: 
  - Rapid geographic movements (`subscriber_geo_velocity_kmph`)
  - Timezone changes (`subscriber_timezone`)
- **Multiple Profiles**: Multiple managed profiles (`subscriber_profile_count`, `subscriber_managed_profile`)
- **VPN Usage**: Location masking attempts (`subscriber_vpn_active`, `subscriber_vpn_connected`)
- **Suspicious Applications**: Unusual app installations (`installed_applications_count`)

### Telecom-Specific Features

- **Fraud Patterns**: 8 fraud types including SIM hijacking (10% of fraudulent devices), VPN manipulation, device emulation, jailbreak/root, ID cloning, ring fraud, device farms
- **Risk Factors**: Industry-relevant risk scoring based on transaction patterns, device trust, and behavioral anomalies
- **Account Context**: Account tenure, services, user profiles, and account type (individual/family/business)

### Performance Optimizations

- **Pre-computed Results**: Risk features calculated once in Silver layer, reused in Gold and Risk Engine
- **Databricks Integration**: Efficient Delta table storage and Spark processing
- **Memory Management**: Optimized pandas/Spark conversions for large datasets
- **Parallel Processing**: Leverages Spark capabilities for distributed processing

---

## Risk Engine Configuration

### Risk Scoring Weights

The risk engine calculates fraud scores (0-100) using weighted risk indicators:

#### Critical Risk Indicators (15-20 points)
- **SIM Swap** (20 pts): Recent SIM card change detected
- **Dark Web Breach** (18 pts): Credentials found in breach database
- **Geographic Velocity** (15 pts for >0 kmph, +10 pts for >50 kmph): Rapid movement between locations
- **High Value Transaction** (12 pts): Amount exceeds $1,000 threshold

#### Medium Risk Indicators (8-12 points)
- **Login Failure** (12 pts): Recent failed authentication attempts
- **No MFA** (10 pts): Multi-factor authentication not enabled
- **New Device** (8 pts): First transaction from this device

#### Lower Risk Indicators (5-7 points)
- **Password Reset Activity** (7 pts): Recent password changes
- **After Hours Transaction** (5 pts): Outside normal business hours (6 AM - 10 PM)

#### Dynamic Modifiers
- **MFA Anomaly Score** (×0.15 multiplier): Unusual MFA behavior patterns
- **Profile Changes** (×3 per change, max 15 pts): Recent account modifications
- **Low Device Trust** (×0.2 multiplier, max 15 pts): Compromised device indicators (100 - device_trust_score)

### Risk Thresholds

Configured in `risk_engine.py`:

```python
self.block_threshold = 95      # Auto-block transactions ≥95
self.pending_threshold = 70   # Review transactions ≥70
self.fraud_rate = 0.2663      # Target fraud rate (26.63%)
```

### Score Distribution

Target distribution for fraud scores:
- **Very High Risk** (95-100): 10% of transactions
- **High Risk** (70-95): 10% of transactions  
- **Low Risk** (0-70): 80% of transactions

### Case Status Classification

Based on fraud score and review state:

- **`blocked`** (≥95): Auto-hold with immediate mitigation
- **`pending_review`** (70-95): Queued for analyst review
- **`reviewed`** (70-95 subset): Confirmed by analysts with notes
- **`escalated`** (70-95 subset): Awaiting SME/third-party review
- **`safe`** (<70): Cleared without intervention

---

## System Configuration

### Catalog and Schema

**Default**: `telecommunications.fraud_data`

Both catalog and schema are configurable via job parameters in `databricks.yml`:

```yaml
variables:
  catalog: "telecommunications"  # Default Unity Catalog
  schema: "fraud_data"           # Default schema
```

All tables, volumes, and data paths use the configured catalog and schema.

### Data Volumes

Raw unstructured data stored in Unity Catalog Volumes:

- **Device Profiles**: `/Volumes/{catalog}/{schema}/raw_device_sdk/`
- **Transactions**: `/Volumes/{catalog}/{schema}/raw_app_transactions/`

Format: JSON Lines (NDJSON), partitioned by `yyyy/mm/dd`

---

## Data Generation

### Transaction Volume

- **Total Transactions**: 100,000 (configurable)
- **Fraud Rate**: 5% in raw data generation
- **Engine Fraud Rate**: 26.63% (top N transactions by score)

### Device Volume

- **Total Devices**: 10,000 (configurable)
- **Fraud Rate**: 5% of devices
- **Fraud Types**: 8 types including SIM hijacking (10% of fraudulent devices)

### Date Range

- **Start Date**: 2024-01-01
- **End Date**: Current date (run-time)

---

## Risk Indicators Reference

| Indicator | Weight | Description |
|-----------|--------|-------------|
| SIM Swap | 20 pts | Recent SIM card change detected |
| Dark Web Breach | 18 pts | Credentials in breach database |
| Geo Velocity (>0) | 15 pts | Any geographic movement detected |
| Geo Velocity (>50 kmph) | +10 pts | Rapid movement (>50 kmph) |
| High Value Transaction | 12 pts | Amount > $1,000 |
| Login Failure | 12 pts | Recent failed login attempts |
| No MFA | 10 pts | Multi-factor authentication disabled |
| New Device | 8 pts | First transaction from device |
| Password Reset | 7 pts | Recent password changes |
| After Hours | 5 pts | Transaction outside 6 AM - 10 PM |
| MFA Anomaly | ×0.15 | Unusual MFA behavior (multiplier) |
| Profile Changes | ×3 (max 15) | Recent account modifications |
| Low Device Trust | ×0.2 (max 15) | Compromised device indicators |

---

## Fraud Types Detected

### Device-Level Fraud (from Device SDK)

1. **VPN Manipulation** (20%): Location masking via VPN
2. **OS Spoofing** (15%): Operating system manipulation
3. **Device Emulation** (15%): Emulated devices (e.g., Android SDK)
4. **Jailbreak/Root** (15%): Unauthorized system access
5. **ID Cloning** (10%): Device ID cloning
6. **Ring Fraud** (10%): Fraud ring participation
7. **SIM Hijacking** (10%): SIM swap attacks
8. **Device Farm** (5%): Automated device farms

### Transaction-Level Fraud (from Transactions)

- Account takeover attempts
- Payment fraud
- Identity theft
- Synthetic identity creation
- First-party fraud

---

## Technical Requirements

### Dependencies

Python packages (defined in `databricks.yml`):
- `pandas>=1.5.0`
- `numpy>=1.21.0`
- `scikit-learn>=1.1.0`
- `matplotlib>=3.5.0`
- `seaborn>=0.11.0`
- `hdbscan>=0.8.0`
- `faker>=18.0.0`

### Compute

- **Serverless Compute**: Auto-scaling Databricks compute
- **Performance Target**: `PERFORMANCE_OPTIMIZED` (configured in job)
- **Queue**: Enabled for job execution

### Storage

- **Tables**: Delta tables in Unity Catalog
- **Volumes**: Unity Catalog Volumes for raw unstructured data
- **Format**: JSON Lines (NDJSON) for raw data, Delta for processed data

---

## Configuration Files

### Risk Engine Configuration

Risk scoring logic is configured in `notebooks/risk_engine.py`:

```python
class RiskEngine:
    def __init__(self, catalog: str = "telecommunications", schema: str = "fraud_data"):
        self.score_distribution = {
            "very_high": {"min": 95, "max": 100, "weight": 0.10},
            "high": {"min": 70, "max": 95, "weight": 0.10},
            "low": {"min": 0, "max": 70, "weight": 0.80},
        }
        self.fraud_rate = 0.2663
        self.block_threshold = 95
        self.pending_threshold = 70
```

**Note**: There is no separate `config/risk_config.json` file. All configuration is in code.

---

## Model Limitations

### Synthetic Data

- This pipeline generates **synthetic data** for demonstration and testing
- Fraud patterns are simulated, not learned from real data
- Risk scores are calculated deterministically based on rules

### No ML Model

- The system uses **rule-based scoring**, not machine learning
- No model training, validation, or performance metrics are calculated
- Fraud detection is based on weighted risk indicators, not predictive models

### Performance Metrics

- **Accuracy, Precision, Recall, F1-Score**: Not calculated in the current implementation
- The pipeline generates synthetic data with known fraud labels for testing
- Real-world performance would require evaluation against actual fraud cases

---

## Usage

### Running the Pipeline

```bash
# Deploy and run entire pipeline
databricks bundle deploy
databricks bundle run fraud_data_pipeline

# Run with custom catalog/schema
databricks bundle deploy -t prod  # Uses catalog/schema from prod target
```

### Querying Results

```sql
-- High-risk transactions
SELECT transaction_id, fraud_score, risk_status_engine, fraud_root_cause_engine
FROM telecommunications.fraud_data.transaction_risk_engine
WHERE fraud_score >= 70
ORDER BY fraud_score DESC;

-- SIM swap detections
SELECT transaction_id, fraud_score, risk_reason_engine
FROM telecommunications.fraud_data.transaction_risk_engine
WHERE risk_reason_engine LIKE '%SIM swap%';

-- Analyst review outcomes
SELECT review_status, COUNT(*) as count, 
       SUM(CASE WHEN fraud_label_analyst = 1 THEN 1 ELSE 0 END) as confirmed_fraud
FROM telecommunications.fraud_data.analyst_review
GROUP BY review_status;
```

---

## Maintenance

### Modifying Risk Weights

Edit `notebooks/risk_engine.py` in the `calculate_fraud_scores()` method:

```python
risk_features["base_score"] += (
    risk_features["subscriber_sim_swap_recent_flag"].astype(int) * 20  # Change weight here
)
```

### Adjusting Thresholds

Modify thresholds in `RiskEngine.__init__()`:

```python
self.block_threshold = 95      # Change auto-block threshold
self.pending_threshold = 70    # Change review threshold
self.fraud_rate = 0.2663       # Change target fraud rate
```

### Adding New Risk Indicators

1. Add feature calculation in `generate_silver_app_transactions.py`
2. Include feature in `risk_engine.py` `calculate_fraud_scores()` method
3. Add appropriate weight based on risk severity
4. Update documentation

---

**For implementation details**: See `docs/PIPELINE_DETAILS.md` for complete pipeline architecture and `notebooks/risk_engine.py` for scoring logic.

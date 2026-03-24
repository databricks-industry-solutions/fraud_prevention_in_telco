"""
INDUSTRY CONFIGURATION — Default: Telecom Fraud Ops

Swap out this file to retarget the app for a different industry.
See README.md for prerequisites and step-by-step setup instructions.
"""

# ── App Title (shown in browser tab via /api/config) ─────────────────────────

APP_TITLE = "Telecom Fraud Operations"

# ── Databricks AI Integrations ───────────────────────────────────────────────
# Create these resources in your workspace BEFORE deploying the app.

GENIE_SPACE_ID_DEFAULT = "<your-genie-space-id>"               # Executive view: NL data exploration
KA_ENDPOINT_DEFAULT = "<your-ka-endpoint-name>"                # Analyst view: Company Playbook (RAG)
SERVING_ENDPOINT_DEFAULT = "databricks-claude-sonnet-4-5"      # Analyst view: AI case investigation agent

# ── Data Location ────────────────────────────────────────────────────────────

DELTA_CATALOG = "telecommunications"
DELTA_SCHEMA = "fraud_data"

# Lakebase table names (as they appear in Postgres after sync)
PRIMARY_TABLE = "transactions_synced"       # Main case table (synced from Delta)
DEVICE_TABLE = "device_sdk_synced"          # Detail profile table (synced from Delta)
REVIEW_TABLE = "analyst_review"             # Analyst decisions (writable, Lakebase-native)
AUDIT_TABLE = "decision_audit_log"          # Decision history (writable, Lakebase-native)
TARGETS_TABLE = "monthly_targets"           # Regional performance targets (writable)

# ── Score Thresholds ─────────────────────────────────────────────────────────
# Cases with score >= FLAGGED are shown in all views.
# Cases with score >= AUTO_BLOCKED are handled by the engine (no analyst needed).
# The range FLAGGED..AUTO_BLOCKED is the analyst workload.

FLAGGED_THRESHOLD = 70
HIGH_RISK_THRESHOLD = 95

# ── Review Status Labels ─────────────────────────────────────────────────────
# These must match the values stored in the review_status column.

STATUS_PENDING = "pending_review"
STATUS_REVIEWED = "reviewed"
STATUS_ESCALATED = "escalated"
STATUS_BLOCKED = "blocked"          # Auto-blocked by the risk engine

# ── Decision Labels ──────────────────────────────────────────────────────────
# Valid analyst decision values (used in the action form).

ALLOWED_DECISIONS = {"confirmed_fraud", "false_positive", "false_negative", "escalated", "reviewed"}

# ── Risk Severity Buckets (executive risk distribution chart) ────────────────

RISK_BUCKETS = [
    {"label": "Critical (95+)", "min": 95, "max": 100},
    {"label": "High (90-95)",   "min": 90, "max": 95},
    {"label": "Medium (80-90)", "min": 80, "max": 90},
    {"label": "Low (70-80)",    "min": 70, "max": 80},
]

# ── Mitigation Actions ───────────────────────────────────────────────────────
# Actions analysts can select when resolving a case.
# The management dashboard's "Mitigation Effectiveness" chart groups by these.
# Each entry: (full text stored in DB, short label for charts)

MITIGATIONS = [
    ("Account locked and customer notified.",               "Account Lock & Notify"),
    ("Awaiting SME response before final action.",          "Pending SME Review"),
    ("Block lifted and customer apologised.",               "Block Lifted (Cleared)"),
    ("Case closed as legitimate; customer reassured.",      "Case Closed (Legit)"),
    ("Charge reversed and SIM reissued.",                   "Charge Reversed (SIM)"),
    ("Credentials reset and MFA enforced.",                 "Credential Reset (MFA)"),
    ("Pending external review; monitoring account activity.","External Review"),
    ("Risk flags cleared after verification call.",         "Risk Flags Cleared"),
]

MITIGATION_STEPS = [m[0] for m in MITIGATIONS]
MITIGATION_SHORT_LABELS = [m[1] for m in MITIGATIONS]

# ── AI Agent Prompt ──────────────────────────────────────────────────────────
# The agent's personality, domain knowledge, and investigation playbook.
# This is the most important section to customize for a new industry.

AGENT_SYSTEM_PROMPT = """\
You are a senior fraud analyst assistant for a telecommunications company. \
You help analysts investigate suspicious transactions by querying case data, \
analyzing risk patterns, and providing actionable recommendations.

## Your Capabilities
You have access to tools that query the fraud case database:
- **get_transaction_details**: Look up full details for any transaction
- **get_device_profile**: Check device security posture for a subscriber
- **get_customer_history**: Review all transactions for an account to find patterns
- **search_similar_cases**: Find cases with similar risk profiles
- **get_region_stats**: Get fraud statistics for a specific region

## Domain Knowledge
Common telecom fraud patterns:
- **SIM Swap**: Attacker transfers victim's phone number to a new SIM.
- **Account Takeover**: Unauthorized access to a customer account.
- **Geo Velocity**: Transactions from distant locations in short timeframes.
- **Subscription Fraud**: Opening accounts with stolen identities.

## Guidelines
- Always use tools to look up data — never make up transaction details
- Investigation order: (1) transaction details, (2) device, (3) history, (4) similar cases
- Be specific: cite which signals led to your conclusion
- Never auto-approve or reject — explain reasoning and let the analyst decide
- If data is missing or inconclusive, say so"""

AGENT_CHAT_PROMPT = """\
You are a fraud analyst assistant for a telecommunications company.
You help analysts investigate suspicious transactions by:
- Summarizing case details and risk factors
- Explaining fraud patterns and risk signals
- Suggesting investigation steps
- Recommending mitigation actions

Be concise and specific. Use bullet points. Do not make up data."""

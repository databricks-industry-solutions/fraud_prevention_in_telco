"""Agent tools — each queries Lakebase and returns structured data."""

import logging
from ..db import db, PGSCHEMA

logger = logging.getLogger(__name__)

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_transaction_details",
            "description": "Get full details for a specific transaction by ID. Returns all fields from the transaction_risk table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string", "description": "The transaction ID (e.g. txn_00042)"}
                },
                "required": ["transaction_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_device_profile",
            "description": "Get device information for a subscriber. Returns model, OS, security status, and fraud indicators.",
            "parameters": {
                "type": "object",
                "properties": {
                    "device_id": {"type": "string", "description": "The subscriber device ID"}
                },
                "required": ["device_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_customer_history",
            "description": "Get all transactions for a customer account. Shows patterns of activity and past fraud flags.",
            "parameters": {
                "type": "object",
                "properties": {
                    "account_id": {"type": "string", "description": "The account ID to look up"}
                },
                "required": ["account_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_similar_cases",
            "description": "Find cases with similar characteristics — same risk reason, region, or score range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "risk_reason": {"type": "string", "description": "Risk reason to match (e.g. 'SIM Swap')"},
                    "region": {"type": "string", "description": "Transaction region to match"},
                    "min_score": {"type": "number", "description": "Minimum fraud score"},
                    "max_score": {"type": "number", "description": "Maximum fraud score"},
                    "limit": {"type": "integer", "description": "Max results (default 10)", "default": 10}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_region_stats",
            "description": "Get fraud statistics for a specific region — case count, avg score, fraud rate, exposure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "description": "Region name (e.g. WEST, NORTHEAST)"}
                },
                "required": ["region"]
            }
        }
    },
]


async def get_transaction_details(transaction_id: str) -> str:
    rows = await db.execute(
        f"""SELECT transaction_id, transaction_date, transaction_state, transaction_region,
               transaction_type, transaction_subtype, transaction_cost, account_id,
               customer_user_id, customer_name, account_services, account_detail,
               high_risk_flag, subscriber_location_lat, subscriber_location_long,
               fraud_score, fraud_label_engine, fraud_label, risk_status_engine,
               risk_reason_engine, review_status, assigned_analyst, analyst_notes,
               last_review_date, mitigation_steps, fraud_root_cause, case_exposure_usd,
               is_fp, is_fn
        FROM {PGSCHEMA}.transactions_synced WHERE transaction_id = $1""",
        transaction_id,
    )
    if not rows:
        return f"No transaction found with ID: {transaction_id}"
    row = rows[0]
    return "\n".join(f"  {k}: {v}" for k, v in row.items() if v is not None)


async def get_device_profile(device_id: str) -> str:
    rows = await db.execute(
        f"""SELECT subscriber_device_id, subscriber_device_model, subscriber_device_board,
               subscriber_os_version, subscriber_device_ram, subscriber_device_storage,
               subscriber_vpn_active, subscriber_device_encryption, subscriber_selinux_status,
               is_fraudulent, fraud_type
        FROM {PGSCHEMA}.device_sdk_synced WHERE subscriber_device_id = $1 LIMIT 1""",
        device_id,
    )
    if not rows:
        return f"No device found with ID: {device_id}"
    row = rows[0]
    return "\n".join(f"  {k}: {v}" for k, v in row.items() if v is not None)


async def get_customer_history(account_id: str) -> str:
    rows = await db.execute(
        f"""SELECT transaction_id, transaction_date, transaction_type, transaction_cost,
               fraud_score, risk_status_engine, review_status, transaction_region
        FROM {PGSCHEMA}.transactions_synced
        WHERE account_id = $1
        ORDER BY transaction_date DESC
        LIMIT 20""",
        account_id,
    )
    if not rows:
        return f"No transactions found for account: {account_id}"
    lines = [f"Found {len(rows)} transaction(s) for account {account_id}:"]
    for r in rows:
        lines.append(f"  {r.get('transaction_id')} | {r.get('transaction_date')} | {r.get('transaction_type')} | ${r.get('transaction_cost', '0')} | score={r.get('fraud_score')} | {r.get('review_status')}")
    return "\n".join(lines)


async def search_similar_cases(risk_reason: str = "", region: str = "",
                                min_score: float = 0, max_score: float = 100,
                                limit: int = 10) -> str:
    where_parts = []
    params = []
    idx = 1

    if risk_reason:
        where_parts.append(f"risk_reason_engine ILIKE ${idx}")
        params.append(f"%{risk_reason}%")
        idx += 1
    if region:
        where_parts.append(f"transaction_region = ${idx}")
        params.append(region)
        idx += 1
    if min_score > 0:
        where_parts.append(f"fraud_score::DOUBLE PRECISION >= ${idx}")
        params.append(min_score)
        idx += 1
    if max_score < 100:
        where_parts.append(f"fraud_score::DOUBLE PRECISION <= ${idx}")
        params.append(max_score)
        idx += 1

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    params.append(min(limit, 20))
    rows = await db.execute(
        f"""SELECT transaction_id, customer_name, fraud_score, risk_reason_engine,
               transaction_region, review_status, case_exposure_usd
        FROM {PGSCHEMA}.transactions_synced
        {where_sql}
        ORDER BY fraud_score::DOUBLE PRECISION DESC
        LIMIT ${idx}""",
        *params,
    )
    if not rows:
        return "No similar cases found matching the criteria."
    lines = [f"Found {len(rows)} similar case(s):"]
    for r in rows:
        lines.append(f"  {r.get('transaction_id')} | {r.get('customer_name')} | score={r.get('fraud_score')} | {r.get('risk_reason_engine')} | {r.get('transaction_region')}")
    return "\n".join(lines)


async def get_region_stats(region: str) -> str:
    rows = await db.execute(
        f"""SELECT
            COUNT(*) as total_cases,
            ROUND(AVG(fraud_score::NUMERIC), 2) as avg_score,
            SUM(CASE WHEN fraud_label_engine = '1' THEN 1 ELSE 0 END) as fraud_count,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) as total_exposure,
            SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) as pending
        FROM {PGSCHEMA}.transactions_synced
        WHERE transaction_region = $1""",
        region,
    )
    if not rows or rows[0].get("total_cases") == "0":
        return f"No data found for region: {region}"
    r = rows[0]
    return f"""Region: {region}
  Total Cases: {r.get('total_cases')}
  Avg Fraud Score: {r.get('avg_score')}
  Confirmed Fraud: {r.get('fraud_count')}
  Total Exposure: ${r.get('total_exposure')}
  Pending Review: {r.get('pending')}"""


# Dispatch map
TOOL_DISPATCH = {
    "get_transaction_details": get_transaction_details,
    "get_device_profile": get_device_profile,
    "get_customer_history": get_customer_history,
    "search_similar_cases": search_similar_cases,
    "get_region_stats": get_region_stats,
}

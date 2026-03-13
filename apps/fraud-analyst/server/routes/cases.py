"""Case queue and case detail API routes."""

import logging
from fastapi import APIRouter, Path, Query
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime, timezone
import uuid

from ..db import db, delta, PGSCHEMA
from ..models import CaseActionRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cases", tags=["cases"])

DELTA_CATALOG = "telecommunications"
DELTA_SCHEMA = "fraud_data"


@router.get("")
async def list_cases(
    status: Optional[str] = Query(None, description="Filter by review_status"),
    min_score: Optional[float] = Query(None, description="Minimum fraud score"),
    region: Optional[str] = Query(None, description="Filter by transaction_region"),
    high_risk: Optional[bool] = Query(None, description="Only high-risk flagged cases"),
    sort_by: str = Query("fraud_score", description="Sort column"),
    sort_dir: str = Query("desc", description="Sort direction: asc or desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
):
    """Paginated case queue with filters."""
    where_clauses: list[str] = []
    params: list = []
    idx = 1

    if status:
        where_clauses.append(f"review_status = ${idx}")
        params.append(status)
        idx += 1
    if min_score is not None:
        where_clauses.append(f"fraud_score::DOUBLE PRECISION >= ${idx}")
        params.append(min_score)
        idx += 1
    if region:
        where_clauses.append(f"transaction_region = ${idx}")
        params.append(region)
        idx += 1
    if high_risk is True:
        where_clauses.append("high_risk_flag = 'true'")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # Validate sort column against allow-list (not parameterisable)
    allowed_sorts = {
        "fraud_score", "transaction_date", "transaction_cost",
        "customer_name", "review_status", "transaction_region",
    }
    if sort_by not in allowed_sorts:
        sort_by = "fraud_score"
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    if sort_by in ("fraud_score", "transaction_cost"):
        order_expr = f"{sort_by}::DOUBLE PRECISION {direction}"
    else:
        order_expr = f"{sort_by} {direction}"

    offset = (page - 1) * limit

    count_sql = f"SELECT COUNT(*) FROM {PGSCHEMA}.transaction_risk {where_sql}"
    total = int(await db.fetchval(count_sql, *params) or 0)

    query_sql = f"""
        SELECT
            transaction_id,
            customer_name,
            transaction_type,
            transaction_cost,
            fraud_score,
            risk_status_engine,
            review_status,
            transaction_date,
            transaction_region,
            high_risk_flag
        FROM {PGSCHEMA}.transaction_risk
        {where_sql}
        ORDER BY {order_expr}
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params.extend([limit, offset])
    rows = await db.execute(query_sql, *params)

    return {
        "cases": rows,
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit if total > 0 else 0,
    }


@router.get("/filters")
async def get_filter_options():
    """Get distinct values for filter dropdowns."""
    statuses = await db.execute(
        f"SELECT DISTINCT review_status FROM {PGSCHEMA}.transaction_risk WHERE review_status IS NOT NULL ORDER BY review_status"
    )
    regions = await db.execute(
        f"SELECT DISTINCT transaction_region FROM {PGSCHEMA}.transaction_risk WHERE transaction_region IS NOT NULL ORDER BY transaction_region"
    )
    types = await db.execute(
        f"SELECT DISTINCT transaction_type FROM {PGSCHEMA}.transaction_risk WHERE transaction_type IS NOT NULL ORDER BY transaction_type"
    )
    return {
        "statuses": [r["review_status"] for r in statuses],
        "regions": [r["transaction_region"] for r in regions],
        "types": [r["transaction_type"] for r in types],
    }


@router.get("/{transaction_id}")
async def get_case_detail(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")):
    """Full case detail with device profile."""
    rows = await db.execute(
        f"SELECT * FROM {PGSCHEMA}.transaction_risk WHERE transaction_id = $1",
        transaction_id,
    )

    if not rows:
        return JSONResponse(
            status_code=404,
            content={"error": "Case not found", "transaction_id": transaction_id},
        )

    case = rows[0]

    device = None
    customer_user_id = case.get("customer_user_id")
    if customer_user_id:
        device_rows = await db.execute(
            f"""
            SELECT
                subscriber_device_id AS device_id,
                subscriber_device_model,
                subscriber_device_board,
                subscriber_os_version,
                subscriber_device_ram,
                subscriber_device_storage,
                subscriber_vpn_active,
                subscriber_device_encryption,
                subscriber_selinux_status,
                is_fraudulent,
                fraud_type
            FROM {PGSCHEMA}.silver_device_sdk
            WHERE subscriber_device_id = $1
            LIMIT 1
            """,
            customer_user_id,
        )
        if device_rows:
            device = device_rows[0]

    return {
        "case": case,
        "device": device,
    }


@router.get("/{transaction_id}/history")
async def get_decision_history(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")):
    """Get decision history for a transaction from analyst_decisions table."""
    rows = await db.execute(
        f"""SELECT decision_id, analyst_name, decision, notes, mitigation_action,
               confidence_level, decided_at
        FROM {PGSCHEMA}.analyst_decisions
        WHERE transaction_id = $1
        ORDER BY decided_at DESC
        LIMIT 20""",
        transaction_id,
    )
    return {"history": rows}


@router.get("/{transaction_id}/customer-history")
async def get_customer_transaction_history(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")):
    """Get recent transaction history for the customer who owns this transaction."""
    # First get the account_id for this transaction
    rows = await db.execute(
        f"SELECT account_id FROM {PGSCHEMA}.transaction_risk WHERE transaction_id = $1",
        transaction_id,
    )
    if not rows or not rows[0].get("account_id"):
        return {"history": []}

    account_id = rows[0]["account_id"]
    history = await db.execute(
        f"""SELECT transaction_id, transaction_date, transaction_cost,
               fraud_score, review_status, transaction_type
        FROM {PGSCHEMA}.transaction_risk
        WHERE account_id = $1
        ORDER BY transaction_date DESC
        LIMIT 30""",
        account_id,
    )
    return {"history": history, "account_id": account_id}


@router.post("/{transaction_id}/action")
async def submit_action(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$"), action: CaseActionRequest = ...):
    """Submit an analyst decision on a case.

    Primary write goes directly to the Lakebase analyst_decisions table via
    asyncpg (parameterised, fast).  Delta write-back is secondary and
    non-blocking for eventual consistency.
    """
    decision_id = f"dec_{uuid.uuid4().hex[:12]}"
    decided_at = datetime.utcnow()

    # Primary write: direct to Lakebase analyst_decisions (fast, parameterized)
    insert_sql = f"""
        INSERT INTO {PGSCHEMA}.analyst_decisions
        (decision_id, transaction_id, analyst_name, decision, notes, mitigation_action, confidence_level, decided_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """
    try:
        await db.execute_write(
            insert_sql,
            decision_id, transaction_id, action.analyst_name,
            action.decision, action.notes or "", action.mitigation_action or "",
            action.confidence_level or "medium", decided_at,
        )
    except Exception as e:
        logger.error("Lakebase INSERT failed for %s: %s (type: %s)", transaction_id, e, type(e).__name__)
        return JSONResponse(
            status_code=502,
            content={"status": "error", "message": "Failed to record decision. Please try again."},
        )

    # Delta write-back for eventual consistency (non-blocking)
    # transaction_risk is a synced table (read-only in Lakebase), so status
    # updates go through Delta via SQL Statement API, then sync back via CDF.
    # Escape single-quotes for SQL Statement API (no parameterised queries)
    safe_decision = action.decision.replace("'", "''")
    safe_analyst = action.analyst_name.replace("'", "''")
    safe_notes = (action.notes or "").replace("'", "''")
    safe_mitigation = (action.mitigation_action or "").replace("'", "''")
    safe_txn = transaction_id.replace("'", "''")

    update_sql = f"""
        UPDATE {DELTA_CATALOG}.{DELTA_SCHEMA}.analyst_review
        SET
            review_status = '{safe_decision}',
            assigned_analyst = '{safe_analyst}',
            analyst_notes = '{safe_notes}',
            mitigation_steps = '{safe_mitigation}',
            last_review_date = TIMESTAMP '{decided_at.isoformat()}'
        WHERE transaction_id = '{safe_txn}'
    """
    try:
        await delta.execute(update_sql, catalog=DELTA_CATALOG, schema=DELTA_SCHEMA)
    except Exception:
        logger.warning("Delta write-back failed for %s (will retry on next sync)", transaction_id)

    try:
        await delta.execute(
            f"""
            UPDATE {DELTA_CATALOG}.{DELTA_SCHEMA}.transaction_risk
            SET review_status = '{safe_decision}',
                assigned_analyst = '{safe_analyst}'
            WHERE transaction_id = '{safe_txn}'
            """,
            catalog=DELTA_CATALOG,
            schema=DELTA_SCHEMA,
        )
    except Exception:
        logger.warning("Delta transaction_risk write-back failed for %s", transaction_id)

    logger.info(
        "AUDIT: decision=%s analyst=%s txn=%s decision_id=%s",
        action.decision, action.analyst_name, transaction_id, decision_id,
    )

    return {
        "status": "ok",
        "decision_id": decision_id,
        "transaction_id": transaction_id,
        "decision": action.decision,
        "analyst": action.analyst_name,
        "timestamp": decided_at.isoformat(),
    }

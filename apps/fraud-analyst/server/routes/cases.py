"""Case queue and case detail API routes."""

import asyncio
import logging
import re
from fastapi import APIRouter, Path, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date as date_cls, datetime, timezone
from slowapi import Limiter
from slowapi.util import get_remote_address

from ..db import db, delta, PGSCHEMA
from ..models import CaseActionRequest

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/api/cases", tags=["cases"])

from ..industry_config import DELTA_CATALOG, DELTA_SCHEMA, FLAGGED_THRESHOLD


@router.get("")
@limiter.limit("60/minute")
async def list_cases(
    request: Request,
    status: Optional[str] = Query(None, description="Filter by review_status"),
    min_score: Optional[float] = Query(None, description="Minimum fraud score"),
    max_score: Optional[float] = Query(None, description="Maximum fraud score"),
    date_from: Optional[str] = Query(None, description="Filter cases from this date (YYYY-MM-DD)"),
    region: Optional[str] = Query(None, description="Filter by transaction_region"),
    analyst: Optional[str] = Query(None, description="Filter by assigned_analyst"),
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
    if max_score is not None:
        where_clauses.append(f"fraud_score::DOUBLE PRECISION < ${idx}")
        params.append(max_score)
        idx += 1
    if date_from:
        try:
            parsed_date = date_cls.fromisoformat(date_from)
            where_clauses.append(f"transaction_date >= ${idx}")
            params.append(parsed_date)
            idx += 1
        except ValueError:
            pass  # ignore invalid date
    if region:
        where_clauses.append(f"transaction_region = ${idx}")
        params.append(region)
        idx += 1
    if analyst:
        where_clauses.append(f"assigned_analyst = ${idx}")
        params.append(analyst)
        idx += 1
    if high_risk is True:
        where_clauses.append(f"fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}")

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

    count_sql = f"SELECT COUNT(*) FROM {PGSCHEMA}.transactions_synced {where_sql}"
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
        FROM {PGSCHEMA}.transactions_synced
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
    statuses, regions, types = await asyncio.gather(
        db.execute(
            f"SELECT DISTINCT review_status FROM {PGSCHEMA}.transactions_synced WHERE review_status IS NOT NULL ORDER BY review_status"
        ),
        db.execute(
            f"SELECT DISTINCT transaction_region FROM {PGSCHEMA}.transactions_synced WHERE transaction_region IS NOT NULL ORDER BY transaction_region"
        ),
        db.execute(
            f"SELECT DISTINCT transaction_type FROM {PGSCHEMA}.transactions_synced WHERE transaction_type IS NOT NULL ORDER BY transaction_type"
        ),
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
        return JSONResponse(
            status_code=404,
            content={"error": "Case not found", "transaction_id": transaction_id},
        )

    case = rows[0]

    # Device lookup: transactions_synced uses customer_user_id (USR-XXXXXX-00)
    # while device_sdk_synced uses subscriber_device_id (device_XXXXXX).
    # Extract the numeric portion to cross-reference.
    device = None
    customer_user_id = case.get("customer_user_id")
    if customer_user_id:
        # Extract numeric ID: "USR-008227-00" -> "008227" -> "device_008227"
        match = re.search(r'(\d+)', customer_user_id)
        if match:
            device_id = f"device_{match.group(1)}"
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
                FROM {PGSCHEMA}.device_sdk_synced
                WHERE subscriber_device_id = $1
                LIMIT 1
                """,
                device_id,
            )
            if device_rows:
                device = device_rows[0]

    return {
        "case": case,
        "device": device,
    }


@router.get("/{transaction_id}/history")
async def get_decision_history(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")):
    """Get analyst decision history from audit log (full history, not just latest)."""
    rows = await db.execute(
        f"""SELECT transaction_id, review_status, assigned_analyst,
               analyst_notes, mitigation_steps, decision_timestamp AS last_review_date,
               is_fp, is_fn
        FROM {PGSCHEMA}.decision_audit_log
        WHERE transaction_id = $1
        ORDER BY decision_timestamp DESC
        LIMIT 20""",
        transaction_id,
    )
    return {"history": rows}


@router.get("/{transaction_id}/customer-history")
async def get_customer_transaction_history(transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")):
    """Get recent transaction history for the customer who owns this transaction."""
    # First get the account_id for this transaction
    rows = await db.execute(
        f"SELECT account_id FROM {PGSCHEMA}.transactions_synced WHERE transaction_id = $1",
        transaction_id,
    )
    if not rows or not rows[0].get("account_id"):
        return {"history": []}

    account_id = rows[0]["account_id"]
    history = await db.execute(
        f"""SELECT transaction_id, transaction_date, transaction_cost,
               fraud_score, review_status, transaction_type
        FROM {PGSCHEMA}.transactions_synced
        WHERE account_id = $1
        ORDER BY transaction_date DESC
        LIMIT 30""",
        account_id,
    )
    return {"history": history, "account_id": account_id}


@router.post("/{transaction_id}/action")
@limiter.limit("20/minute")
async def submit_action(request: Request, transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$"), action: CaseActionRequest = ...):
    """Submit an analyst decision on a case.

    Primary write goes directly to Lakebase analyst_review table via asyncpg
    (parameterised, sub-10ms).  Delta write-back is secondary and non-blocking
    for eventual consistency with the lakehouse.
    """
    decided_at = datetime.now(timezone.utc)

    # Primary write: UPSERT into Lakebase analyst_review (fast, parameterized)
    upsert_sql = f"""
        INSERT INTO {PGSCHEMA}.analyst_review
        (transaction_id, review_status, assigned_analyst, analyst_notes,
         mitigation_steps, last_review_date, is_fp, is_fn)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_id) DO UPDATE SET
            review_status = EXCLUDED.review_status,
            assigned_analyst = EXCLUDED.assigned_analyst,
            analyst_notes = EXCLUDED.analyst_notes,
            mitigation_steps = EXCLUDED.mitigation_steps,
            last_review_date = EXCLUDED.last_review_date,
            is_fp = EXCLUDED.is_fp,
            is_fn = EXCLUDED.is_fn
    """
    is_fp = action.decision == "false_positive"
    is_fn = action.decision == "false_negative"
    try:
        await db.execute_write(
            upsert_sql,
            transaction_id, action.decision, action.analyst_name,
            action.notes or "", action.mitigation_action or "",
            decided_at, is_fp, is_fn,
        )
    except Exception as e:
        logger.error("Lakebase UPSERT failed for %s: %s (type: %s)", transaction_id, e, type(e).__name__)
        return JSONResponse(
            status_code=502,
            content={"status": "error", "message": "Failed to record decision. Please try again."},
        )

    # Append to audit log (full decision history, not just latest)
    try:
        await db.execute_write(
            f"""INSERT INTO {PGSCHEMA}.decision_audit_log
            (transaction_id, review_status, assigned_analyst, analyst_notes,
             mitigation_steps, decision_timestamp, is_fp, is_fn)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
            transaction_id, action.decision, action.analyst_name,
            action.notes or "", action.mitigation_action or "",
            decided_at, is_fp, is_fn,
        )
    except Exception as e:
        logger.warning("Audit log insert failed for %s: %s", transaction_id, e)

    # Delta write-back (transactions_synced is read-only; updates go to Delta
    # and propagate back via CDF sync).
    #
    # SQL Statement API does not support parameterised queries, so we
    # validate all values strictly before interpolation:
    #   - decision: already validated by Pydantic Literal (5 allowlisted values)
    #   - analyst_name: validated by Pydantic regex (alphanumeric + spaces/dots/hyphens)
    #   - transaction_id: validated by path regex (alphanumeric + underscores/hyphens)
    #   - notes/mitigation: validated by Pydantic regex (safe characters only)
    #   - is_fp/is_fn: derived booleans (not user-controlled)
    #   - decided_at: server-generated datetime (not user-controlled)

    ALLOWED_DECISIONS = {"confirmed_fraud", "false_positive", "false_negative", "escalated", "reviewed"}
    if action.decision not in ALLOWED_DECISIONS:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid decision value."})

    def _sql_escape(val: str) -> str:
        """Escape single quotes and strip control characters for SQL string literals."""
        return val.replace("\\", "\\\\").replace("'", "''").replace("\x00", "")

    esc_decision = _sql_escape(action.decision)
    esc_analyst = _sql_escape(action.analyst_name)
    esc_notes = _sql_escape(action.notes or "")
    esc_mitigation = _sql_escape(action.mitigation_action or "")
    esc_txn = _sql_escape(transaction_id)
    esc_timestamp = decided_at.strftime("%Y-%m-%d %H:%M:%S")

    try:
        await delta.execute(
            f"""UPDATE {DELTA_CATALOG}.{DELTA_SCHEMA}.analyst_review
                SET review_status = '{esc_decision}',
                    assigned_analyst = '{esc_analyst}',
                    analyst_notes = '{esc_notes}',
                    mitigation_steps = '{esc_mitigation}',
                    last_review_date = TIMESTAMP '{esc_timestamp}',
                    is_fp = {str(is_fp).lower()},
                    is_fn = {str(is_fn).lower()}
                WHERE transaction_id = '{esc_txn}'""",
            catalog=DELTA_CATALOG, schema=DELTA_SCHEMA,
        )
    except Exception:
        logger.warning("Delta analyst_review write-back failed for %s", transaction_id)

    try:
        await delta.execute(
            f"""UPDATE {DELTA_CATALOG}.{DELTA_SCHEMA}.transaction_risk
                SET review_status = '{esc_decision}',
                    assigned_analyst = '{esc_analyst}'
                WHERE transaction_id = '{esc_txn}'""",
            catalog=DELTA_CATALOG, schema=DELTA_SCHEMA,
        )
    except Exception:
        logger.warning("Delta transaction_risk write-back failed for %s", transaction_id)

    logger.info(
        "AUDIT: decision=%s analyst=%s txn=%s",
        action.decision, action.analyst_name, transaction_id,
    )

    return {
        "status": "ok",
        "transaction_id": transaction_id,
        "decision": action.decision,
        "analyst": action.analyst_name,
        "timestamp": decided_at.isoformat(),
    }

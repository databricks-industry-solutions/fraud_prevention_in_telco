"""Executive Dashboard API routes.

Financial P&L, actual vs target, growth trends, pipeline changes,
and regional team performance for executive oversight.
"""

import os
import logging
import aiohttp
from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional

from ..db import db, PGSCHEMA
from ..config import get_oauth_token, get_workspace_host

from ..industry_config import GENIE_SPACE_ID_DEFAULT, FLAGGED_THRESHOLD
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", GENIE_SPACE_ID_DEFAULT)

router = APIRouter(prefix="/api/executive", tags=["executive"])
logger = logging.getLogger(__name__)


def _f(val, default=0.0):
    if val is None: return default
    try: return float(val)
    except (ValueError, TypeError): return default

def _i(val, default=0):
    if val is None: return default
    try: return int(val)
    except (ValueError, TypeError): return default


@router.get("/financial-summary")
async def financial_summary():
    """Current month financials with prior month comparison and targets."""
    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref
            FROM {PGSCHEMA}.transactions_synced WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        ),
        actuals AS (
            SELECT
                'current_month' AS period,
                COUNT(*) AS total_cases,
                ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
                SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END) AS autoblock_cases,
                ROUND(SUM(CASE WHEN risk_status_engine = 'blocked' AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1 THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS saved_by_autoblock,
                SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END) AS fp_cases,
                ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_by_fp,
                SUM(CASE WHEN is_fn = TRUE THEN 1 ELSE 0 END) AS fn_cases,
                ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_by_fn,
                SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending_cases,
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed_cases
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD} AND transaction_date >= latest.ref
            UNION ALL
            SELECT 'prior_month',
                COUNT(*), ROUND(SUM(case_exposure_usd::NUMERIC), 2),
                SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END),
                ROUND(SUM(CASE WHEN risk_status_engine = 'blocked' AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1 THEN case_exposure_usd::NUMERIC ELSE 0 END), 2),
                SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END),
                ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2),
                SUM(CASE WHEN is_fn = TRUE THEN 1 ELSE 0 END),
                ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2),
                SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END),
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END)
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref - INTERVAL '1 month' AND transaction_date < latest.ref
        )
        SELECT * FROM actuals
    """)

    # Get targets for current month
    target_rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date))::DATE AS ref
            FROM {PGSCHEMA}.transactions_synced WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT
            SUM(target_cases) AS target_cases,
            ROUND(AVG(target_autoblock_rate), 1) AS target_autoblock_rate,
            AVG(target_resolution_rate) AS target_resolution_rate,
            AVG(target_max_fp_rate) AS target_max_fp_rate,
            ROUND(SUM(target_max_exposure), 2) AS target_max_exposure
        FROM {PGSCHEMA}.monthly_targets, latest
        WHERE target_month = latest.ref
    """)

    result = {}
    for r in rows:
        period = r["period"]
        tc = _i(r["total_cases"])
        ab = _i(r["autoblock_cases"])
        fp = _i(r["fp_cases"])
        saved = _f(r["saved_by_autoblock"])
        lost_fp = _f(r["lost_by_fp"])
        lost_fn = _f(r["lost_by_fn"])
        result[period] = {
            "total_cases": tc,
            "total_exposure": _f(r["total_exposure"]),
            "autoblock_cases": ab,
            "saved_by_autoblock": saved,
            "fp_cases": fp,
            "lost_by_fp": lost_fp,
            "fn_cases": _i(r["fn_cases"]),
            "lost_by_fn": lost_fn,
            "net_savings": round(saved - lost_fp - lost_fn, 2),
            "autoblock_accuracy": round(100.0 * (ab - fp) / ab, 1) if ab > 0 else 0,
            "pending_cases": _i(r["pending_cases"]),
            "closed_cases": _i(r["closed_cases"]),
            "resolution_rate": round(100.0 * _i(r["closed_cases"]) / tc, 1) if tc > 0 else 0,
            "fp_rate": round(100.0 * fp / ab, 1) if ab > 0 else 0,
        }

    # Add targets
    if target_rows and target_rows[0].get("target_cases"):
        t = target_rows[0]
        result["targets"] = {
            "target_cases": _i(t["target_cases"]),
            "target_autoblock_rate": _f(t["target_autoblock_rate"]),
            "target_resolution_rate": _f(t["target_resolution_rate"]),
            "target_max_fp_rate": _f(t["target_max_fp_rate"]),
            "target_max_exposure": _f(t["target_max_exposure"]),
        }

    return result


@router.get("/quarterly-trend")
async def quarterly_trend():
    """Quarterly financial P&L with growth rates."""
    rows = await db.execute(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('quarter', transaction_date), 'YYYY-"Q"Q') AS quarter,
            COUNT(*) AS total_cases,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END) AS autoblock_cases,
            ROUND(SUM(CASE WHEN risk_status_engine = 'blocked' AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1 THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS saved,
            SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END) AS fp_cases,
            ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_fp,
            SUM(CASE WHEN is_fn = TRUE THEN 1 ELSE 0 END) AS fn_cases,
            ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_fn
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY DATE_TRUNC('quarter', transaction_date)
        ORDER BY quarter
    """)

    quarters = []
    for i, r in enumerate(rows):
        saved = _f(r["saved"])
        lost_fp = _f(r["lost_fp"])
        lost_fn = _f(r["lost_fn"])
        net = round(saved - lost_fp - lost_fn, 2)
        prev_net = None
        if i > 0:
            ps = _f(rows[i-1]["saved"])
            plf = _f(rows[i-1]["lost_fp"])
            pln = _f(rows[i-1]["lost_fn"])
            prev_net = round(ps - plf - pln, 2)

        quarters.append({
            "quarter": r["quarter"],
            "total_cases": _i(r["total_cases"]),
            "total_exposure": _f(r["total_exposure"]),
            "saved": saved,
            "lost_fp": lost_fp,
            "lost_fn": lost_fn,
            "net_savings": net,
            "qoq_growth": round((net - prev_net) / abs(prev_net) * 100, 1) if prev_net and prev_net != 0 else None,
        })

    return {"quarters": quarters}


@router.get("/monthly-financials")
async def monthly_financials():
    """12-month financial trend with targets overlay."""
    # Get actuals and targets separately to avoid join fan-out
    rows = await db.execute(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', transaction_date), 'YYYY-MM') AS month,
            COUNT(*) AS total_cases,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(SUM(CASE WHEN risk_status_engine = 'blocked' AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1 THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS saved,
            ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_fp,
            ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_fn
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY DATE_TRUNC('month', transaction_date)
        ORDER BY month
    """)
    tgt_rows = await db.execute(f"""
        SELECT TO_CHAR(target_month, 'YYYY-MM') AS month, ROUND(SUM(target_max_exposure), 2) AS target_exposure
        FROM {PGSCHEMA}.monthly_targets
        GROUP BY target_month ORDER BY month
    """)
    tgt_map = {t["month"]: _f(t["target_exposure"]) for t in tgt_rows}

    return {
        "months": [{
            "month": r["month"],
            "total_cases": _i(r["total_cases"]),
            "total_exposure": _f(r["total_exposure"]),
            "saved": _f(r["saved"]),
            "lost_fp": _f(r["lost_fp"]),
            "lost_fn": _f(r["lost_fn"]),
            "net_savings": round(_f(r["saved"]) - _f(r["lost_fp"]) - _f(r["lost_fn"]), 2),
            "target_exposure": tgt_map.get(r["month"], 0),
        } for r in rows]
    }


@router.get("/regional-teams")
async def regional_teams():
    """Regional team performance — region as proxy for manager group."""
    # Two-step: get actuals, then join targets in Python to avoid complex cross-join
    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref
            FROM {PGSCHEMA}.transactions_synced WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT
            transaction_region AS region,
            COUNT(DISTINCT assigned_analyst) AS analysts,
            COUNT(*) AS total_cases,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END) AS autoblock_cases,
            ROUND(SUM(CASE WHEN risk_status_engine = 'blocked' AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1 THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS saved,
            SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END) AS fp_cases,
            ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_fp,
            SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed,
            ROUND(100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS resolution_rate,
            ROUND(100.0 * SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS autoblock_rate,
            ROUND(100.0 * SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END), 0), 1) AS fp_rate
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref
        GROUP BY transaction_region
        ORDER BY total_exposure DESC
    """)

    # Get targets separately
    tgt_rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date))::DATE AS ref
            FROM {PGSCHEMA}.transactions_synced WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT transaction_region, target_autoblock_rate, target_resolution_rate, target_max_fp_rate
        FROM {PGSCHEMA}.monthly_targets, latest
        WHERE target_month = latest.ref
    """)
    tgt_map = {t["transaction_region"]: t for t in tgt_rows}

    return {
        "teams": [{
            "region": r["region"],
            "analysts": _i(r["analysts"]),
            "total_cases": _i(r["total_cases"]),
            "total_exposure": _f(r["total_exposure"]),
            "saved": _f(r["saved"]),
            "lost_fp": _f(r["lost_fp"]),
            "pending": _i(r["pending"]),
            "closed": _i(r["closed"]),
            "resolution_rate": _f(r["resolution_rate"]),
            "autoblock_rate": _f(r["autoblock_rate"]),
            "fp_rate": _f(r["fp_rate"]),
            "net_savings": round(_f(r["saved"]) - _f(r["lost_fp"]), 2),
            "target_autoblock_rate": _f(tgt_map.get(r["region"], {}).get("target_autoblock_rate")),
            "target_resolution_rate": _f(tgt_map.get(r["region"], {}).get("target_resolution_rate")),
            "target_max_fp_rate": _f(tgt_map.get(r["region"], {}).get("target_max_fp_rate")),
        } for r in rows]
    }


@router.get("/pipeline-changes")
async def pipeline_changes():
    """Month-over-month pipeline changes by status."""
    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref
            FROM {PGSCHEMA}.transactions_synced WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        ),
        current_m AS (
            SELECT review_status, COUNT(*) AS cnt, ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD} AND transaction_date >= latest.ref
            GROUP BY review_status
        ),
        prior_m AS (
            SELECT review_status, COUNT(*) AS cnt, ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref - INTERVAL '1 month' AND transaction_date < latest.ref
            GROUP BY review_status
        )
        SELECT
            COALESCE(c.review_status, p.review_status) AS status,
            COALESCE(c.cnt, 0) AS current_cases,
            COALESCE(c.exposure, 0) AS current_exposure,
            COALESCE(p.cnt, 0) AS prior_cases,
            COALESCE(p.exposure, 0) AS prior_exposure
        FROM current_m c
        FULL OUTER JOIN prior_m p ON c.review_status = p.review_status
        ORDER BY current_cases DESC
    """)

    return {
        "changes": [{
            "status": r["status"],
            "current_cases": _i(r["current_cases"]),
            "current_exposure": _f(r["current_exposure"]),
            "prior_cases": _i(r["prior_cases"]),
            "prior_exposure": _f(r["prior_exposure"]),
            "case_change": _i(r["current_cases"]) - _i(r["prior_cases"]),
            "exposure_change": round(_f(r["current_exposure"]) - _f(r["prior_exposure"]), 2),
        } for r in rows]
    }


@router.get("/risk-distribution")
async def risk_distribution():
    """Cases by risk severity bucket."""
    rows = await db.execute(f"""
        SELECT
            CASE
                WHEN fraud_score::DOUBLE PRECISION >= 95 THEN 'Critical'
                WHEN fraud_score::DOUBLE PRECISION >= 90 THEN 'Very High'
                WHEN fraud_score::DOUBLE PRECISION >= 80 THEN 'High'
                ELSE 'Moderate'
            END AS category,
            COUNT(*) AS case_count,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(AVG(case_exposure_usd::NUMERIC), 2) AS avg_exposure
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY 1
        ORDER BY MIN(fraud_score::DOUBLE PRECISION) DESC
    """)
    total = sum(_i(r["case_count"]) for r in rows)
    return {
        "buckets": [{
            "category": r["category"],
            "case_count": _i(r["case_count"]),
            "total_exposure": _f(r["total_exposure"]),
            "avg_exposure": _f(r["avg_exposure"]),
            "pct_of_total": round(100.0 * _i(r["case_count"]) / total, 1) if total > 0 else 0,
        } for r in rows]
    }


@router.get("/top-exposure-cases")
async def top_exposure_cases():
    """Top 10 highest exposure pending cases."""
    rows = await db.execute(f"""
        SELECT transaction_id, customer_name, fraud_score, case_exposure_usd,
               risk_reason_engine, transaction_region, review_status, assigned_analyst
        FROM {PGSCHEMA}.transactions_synced
        WHERE review_status = 'pending_review' AND fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        ORDER BY case_exposure_usd::DOUBLE PRECISION DESC
        LIMIT 10
    """)
    return {"cases": rows}


# ── Genie Chat ────────────────────────────────────────────────

class GenieQuestion(BaseModel):
    question: str = Field(..., max_length=2000)
    conversation_id: Optional[str] = None


@router.post("/genie")
async def genie_ask(req: GenieQuestion):
    """Ask the Genie Space a question about fraud operations data."""
    host = get_workspace_host()
    token = get_oauth_token()

    # Start or continue conversation
    payload = {"content": req.question}
    if req.conversation_id:
        url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{req.conversation_id}/messages"
    else:
        url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"

    try:
        async with aiohttp.ClientSession() as session:
            # Create conversation or send message
            async with session.post(url, headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                data = await resp.json()

            conv_id = data.get("conversation_id", req.conversation_id)
            msg_id = data.get("message_id", data.get("id"))

            if not conv_id or not msg_id:
                return {"reply": str(data), "conversation_id": conv_id}

            # Poll for result (Genie is async)
            import asyncio
            for _ in range(30):
                await asyncio.sleep(2)
                poll_url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conv_id}/messages/{msg_id}"
                async with session.get(poll_url, headers={
                    "Authorization": f"Bearer {token}",
                }) as poll_resp:
                    result = await poll_resp.json()

                status = result.get("status", "")
                if status in ("COMPLETED", "completed"):
                    attachments = result.get("attachments", [])
                    reply_parts = []

                    for att in attachments:
                        # Text responses
                        text_obj = att.get("text")
                        if text_obj and text_obj.get("content"):
                            reply_parts.append(text_obj["content"])

                        # Query results
                        query_obj = att.get("query")
                        if query_obj:
                            desc = query_obj.get("description", "")
                            if desc:
                                reply_parts.append(desc)
                            columns = [c.get("name", "") for c in query_obj.get("columns", [])]
                            data_rows = query_obj.get("result", {}).get("data_array", [])
                            if columns and data_rows:
                                reply_parts.append("")
                                reply_parts.append(" | ".join(columns))
                                reply_parts.append("-" * len(" | ".join(columns)))
                                for row in data_rows[:20]:
                                    reply_parts.append(" | ".join(str(v) for v in row))
                                if len(data_rows) > 20:
                                    reply_parts.append(f"... and {len(data_rows) - 20} more rows")

                    # Fallback: check top-level content
                    if not reply_parts:
                        top_content = result.get("content", "")
                        if top_content and top_content != req.question:
                            reply_parts.append(top_content)

                    return {
                        "reply": "\n".join(reply_parts) if reply_parts else "Query completed but no results returned.",
                        "conversation_id": conv_id,
                    }

                elif status in ("FAILED", "failed", "CANCELLED"):
                    error = result.get("error", {}).get("message", "Query failed")
                    return {"reply": f"Genie error: {error}", "conversation_id": conv_id}

            return {"reply": "Query timed out. Please try a simpler question.", "conversation_id": conv_id}

    except Exception as e:
        logger.error("Genie error: %s", e)
        return {"reply": f"Genie unavailable: {str(e)}", "conversation_id": None}

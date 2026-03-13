"""Dashboard aggregate API routes."""

from fastapi import APIRouter

from ..db import db, PGSCHEMA

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def dashboard_stats():
    """Aggregate statistics for the dashboard overview."""
    rows = await db.execute(f"""
        SELECT
            COUNT(*) AS total_cases,
            SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN review_status = 'confirmed_fraud' THEN 1 ELSE 0 END) AS confirmed_fraud_count,
            SUM(CASE WHEN review_status = 'false_positive' THEN 1 ELSE 0 END) AS false_positive_count,
            SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated_count,
            ROUND(AVG(fraud_score::NUMERIC), 2) AS avg_fraud_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure_usd,
            SUM(CASE WHEN high_risk_flag = 'true' THEN 1 ELSE 0 END) AS high_risk_count
        FROM {PGSCHEMA}.transaction_risk
    """)

    if rows:
        stats = rows[0]
        return {
            "total_cases": int(stats.get("total_cases") or 0),
            "pending_count": int(stats.get("pending_count") or 0),
            "confirmed_fraud_count": int(stats.get("confirmed_fraud_count") or 0),
            "false_positive_count": int(stats.get("false_positive_count") or 0),
            "escalated_count": int(stats.get("escalated_count") or 0),
            "avg_fraud_score": float(stats.get("avg_fraud_score") or 0),
            "total_exposure_usd": float(stats.get("total_exposure_usd") or 0),
            "high_risk_count": int(stats.get("high_risk_count") or 0),
        }
    return {}


@router.get("/trends")
async def dashboard_trends():
    """Daily case counts and fraud rates for the last 30 days."""
    rows = await db.execute(f"""
        SELECT
            DATE(transaction_date) AS day,
            COUNT(*) AS case_count,
            SUM(CASE WHEN fraud_label_engine = '1' THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(AVG(fraud_score::NUMERIC), 2) AS avg_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS daily_exposure
        FROM {PGSCHEMA}.transaction_risk
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(transaction_date)
        ORDER BY day DESC
    """)
    return {"trends": rows}


@router.get("/geo")
async def dashboard_geo():
    """State/region-level aggregation for map visualization."""
    rows = await db.execute(f"""
        SELECT
            transaction_region,
            COUNT(*) AS case_count,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(AVG(fraud_score::NUMERIC), 2) AS avg_score,
            SUM(CASE WHEN fraud_label_engine = '1' THEN 1 ELSE 0 END) AS fraud_count
        FROM {PGSCHEMA}.transaction_risk
        WHERE transaction_region IS NOT NULL
        GROUP BY transaction_region
        ORDER BY case_count DESC
    """)
    return {"regions": rows}


@router.get("/top-risk")
async def dashboard_top_risk():
    """Top 10 highest-risk pending cases for the priority queue."""
    rows = await db.execute(f"""
        SELECT
            transaction_id,
            customer_name,
            fraud_score,
            case_exposure_usd,
            risk_reason_engine,
            transaction_region,
            review_status
        FROM {PGSCHEMA}.transaction_risk
        WHERE review_status IN ('pending_review', 'needs_review')
        ORDER BY fraud_score::DOUBLE PRECISION DESC
        LIMIT 10
    """)
    return {"cases": rows}

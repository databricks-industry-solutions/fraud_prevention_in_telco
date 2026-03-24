"""Management View dashboard API routes.

Provides pipeline health with dollar exposure, period-over-period
comparisons, AI-powered forecasting, environmental risk factors,
mitigation effectiveness analysis, and team performance benchmarks.
"""

import logging
from fastapi import APIRouter, Path, Request

from ..db import db, delta, PGSCHEMA
from ..industry_config import (
    DELTA_CATALOG, DELTA_SCHEMA,
    FLAGGED_THRESHOLD, HIGH_RISK_THRESHOLD,
    MITIGATION_STEPS, MITIGATION_SHORT_LABELS,
)

# SQL queries use {FLAGGED_THRESHOLD} (70) and {HIGH_RISK_THRESHOLD} (95)
# to segment cases. Change these in industry_config.py to adjust thresholds.

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
logger = logging.getLogger(__name__)

# Short labels for mitigation steps (from industry_config.py)
MITIGATION_LABELS = dict(zip(MITIGATION_STEPS, MITIGATION_SHORT_LABELS))

# ── Pipeline Summary with Dollar Exposure ─────────────────────

@router.get("/pipeline-summary")
async def pipeline_summary(period: str = "month"):
    """Current pipeline with $ exposure, plus prior period comparison.

    period: 'day', 'week', 'month' (default), 'quarter'

    Note: Uses fraud_score >= FLAGGED_THRESHOLD (all flagged cases including auto-blocked).
    Team Performance and Forecast use FLAGGED_THRESHOLD to HIGH_RISK_THRESHOLD
    (analyst workload only, excluding auto-blocked).
    Thresholds are configured in industry_config.py.
    """
    allowed_periods = {"day", "week", "month", "quarter"}
    if period not in allowed_periods:
        period = "month"

    # Map period to SQL truncation and intervals
    trunc_map = {"day": "day", "week": "week", "month": "month", "quarter": "quarter"}
    interval_map = {"day": "1 day", "week": "7 days", "month": "1 month", "quarter": "3 months"}
    prior_q_map = {"day": "7 days", "week": "28 days", "month": "3 months", "quarter": "12 months"}
    prior_y_map = {"day": "1 year", "week": "1 year", "month": "1 year", "quarter": "1 year"}

    trunc = trunc_map[period]
    cur_iv = interval_map[period]
    prior_iv = interval_map[period]
    prior_q_iv = prior_q_map[period]
    prior_y_iv = prior_y_map[period]

    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('{trunc}', MAX(transaction_date)) AS ref_month
            FROM {PGSCHEMA}.transactions_synced
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        ),
        current_period AS (
            SELECT
                COUNT(*) AS total_cases,
                SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS open_cases,
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed_cases,
                SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
                ROUND(AVG(fraud_score::NUMERIC), 1) AS avg_score,
                ROUND(
                    100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1
                ) AS resolution_rate,
                ROUND(
                    100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1
                ) AS escalation_rate,
                ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
                ROUND(SUM(CASE WHEN review_status = 'pending_review' THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS open_exposure,
                ROUND(AVG(case_exposure_usd::NUMERIC), 2) AS avg_exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref_month
        ),
        prior_month AS (
            SELECT
                COUNT(*) AS total_cases,
                SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS open_cases,
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed_cases,
                SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
                ROUND(100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS resolution_rate,
                ROUND(100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1) AS escalation_rate,
                ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
                ROUND(SUM(CASE WHEN review_status = 'pending_review' THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS open_exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref_month - INTERVAL '{prior_iv}'
              AND transaction_date < latest.ref_month
        ),
        prior_quarter AS (
            SELECT
                COUNT(*) AS total_cases,
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed_cases,
                SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
                ROUND(100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS resolution_rate,
                ROUND(100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1) AS escalation_rate,
                ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref_month - INTERVAL '{prior_q_iv}'
              AND transaction_date < latest.ref_month
        ),
        prior_year AS (
            SELECT
                COUNT(*) AS total_cases,
                SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed_cases,
                SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
                ROUND(100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS resolution_rate,
                ROUND(100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1) AS escalation_rate,
                ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure
            FROM {PGSCHEMA}.transactions_synced, latest
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
              AND transaction_date >= latest.ref_month - INTERVAL '{prior_y_iv}'
              AND transaction_date < latest.ref_month - INTERVAL '{prior_y_iv}' + INTERVAL '{cur_iv}'
        )
        SELECT 'current' AS period, c.* FROM current_period c
        UNION ALL SELECT 'prior_month', pm.total_cases, pm.open_cases, pm.closed_cases, pm.escalated, NULL, pm.resolution_rate, pm.escalation_rate, pm.total_exposure, pm.open_exposure, NULL FROM prior_month pm
        UNION ALL SELECT 'prior_quarter', pq.total_cases, NULL, pq.closed_cases, pq.escalated, NULL, pq.resolution_rate, pq.escalation_rate, pq.total_exposure, NULL, NULL FROM prior_quarter pq
        UNION ALL SELECT 'prior_year', py.total_cases, NULL, py.closed_cases, py.escalated, NULL, py.resolution_rate, py.escalation_rate, py.total_exposure, NULL, NULL FROM prior_year py
    """)

    def to_dict(r):
        return {k: (float(v) if v is not None and k != 'period' else v) for k, v in r.items()}

    result = {}
    for r in rows:
        result[r["period"]] = to_dict(r)

    # Include the reference date so the frontend can build matching filters
    ref_rows = await db.execute(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('{trunc}', MAX(transaction_date)), 'YYYY-MM-DD') AS ref_date,
            TO_CHAR(MAX(transaction_date), 'YYYY-MM-DD') AS latest_date
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
    """)
    result["ref_month"] = ref_rows[0]["ref_date"] if ref_rows else None
    result["latest_date"] = ref_rows[0]["latest_date"] if ref_rows else None
    result["period"] = period

    return result


# ── Engine Performance ────────────────────────────────────────

@router.get("/engine-performance")
async def engine_performance(period: str = "month"):
    """Fraud detection engine effectiveness: auto-blocks, FP/FN costs, $ saved.

    Mirrors the Lakeview dashboard's engine metrics using the same logic.
    """
    allowed = {"day", "week", "month", "quarter"}
    if period not in allowed:
        period = "month"
    trunc_map = {"day": "day", "week": "week", "month": "month", "quarter": "quarter"}
    interval_map = {"day": "1 day", "week": "7 days", "month": "1 month", "quarter": "3 months"}
    trunc = trunc_map[period]
    prior_iv = interval_map[period]

    # is_fp/is_fn are boolean, fraud_label/fraud_label_engine are numeric (1=fraud)
    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('{trunc}', MAX(transaction_date)) AS ref_start
            FROM {PGSCHEMA}.transactions_synced
        )
        SELECT
            'current' AS period,
            SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END) AS autoblock_cases,
            ROUND(SUM(CASE
                WHEN risk_status_engine = 'blocked'
                 AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1
                THEN case_exposure_usd::NUMERIC ELSE 0
            END), 2) AS saved_by_autoblock,
            SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END) AS false_positives,
            ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_by_fp,
            SUM(CASE WHEN is_fn = TRUE THEN 1 ELSE 0 END) AS false_negatives,
            ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS lost_by_fn,
            COUNT(*) AS total_flagged,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref_start

        UNION ALL

        SELECT
            'prior',
            SUM(CASE WHEN risk_status_engine = 'blocked' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE
                WHEN risk_status_engine = 'blocked'
                 AND COALESCE(fraud_label::INTEGER, fraud_label_engine::INTEGER, 0) = 1
                THEN case_exposure_usd::NUMERIC ELSE 0
            END), 2),
            SUM(CASE WHEN is_fp = TRUE THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN is_fp = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2),
            SUM(CASE WHEN is_fn = TRUE THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN is_fn = TRUE THEN case_exposure_usd::NUMERIC ELSE 0 END), 2),
            COUNT(*),
            ROUND(SUM(case_exposure_usd::NUMERIC), 2)
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref_start - INTERVAL '{prior_iv}'
          AND transaction_date < latest.ref_start
    """)

    def to_dict(r):
        return {k: (float(v) if v is not None and k != 'period' else v) for k, v in r.items()}

    result = {}
    for r in rows:
        result[r["period"]] = to_dict(r)
    return result


# ── AI Forecast (via SQL Statement API + ai_forecast) ─────────

@router.get("/forecast")
async def forecast(request: Request):
    """30-day case volume forecast.

    Attempts Databricks ai_forecast() via SQL Statement API first,
    using the authenticated user's token for catalog access.
    Falls back to Python linear regression if unavailable.
    """
    # Use the SP's token for SQL Statement API (the SP has warehouse access
    # via the sql_warehouse resource in app.yaml). User passthrough tokens
    # from the gateway don't have SQL Statement API permissions.
    method = "ai_forecast"
    try:
        return await _forecast_ai(user_token=None)
    except Exception as e:
        logger.warning("ai_forecast unavailable (%s), using linear regression fallback", e)
        method = "linear_regression"
        result = await _forecast_linreg()
        result["method"] = method
        return result


async def _forecast_ai(user_token=None):
    """Run ai_forecast via SQL Statement API using user passthrough token."""
    AI_FORECAST_SQL = f"""
    WITH daily_cases AS (
        SELECT
            DATE(transaction_date) AS transaction_day,
            COUNT(*) AS daily_case_count
        FROM {DELTA_CATALOG}.{DELTA_SCHEMA}.transaction_risk
        WHERE fraud_score BETWEEN {FLAGGED_THRESHOLD} AND {HIGH_RISK_THRESHOLD}
        GROUP BY DATE(transaction_date)
    ),
    recent_daily_cases AS (
        SELECT *
        FROM daily_cases
        WHERE transaction_day >= DATEADD(DAY, -60, (SELECT MAX(transaction_day) FROM daily_cases))
    ),
    forecast AS (
        SELECT *
        FROM ai_forecast(
            observed   => TABLE(recent_daily_cases),
            horizon    => (SELECT MAX(transaction_day) + INTERVAL 30 DAYS FROM daily_cases),
            time_col   => 'transaction_day',
            value_col  => 'daily_case_count'
        )
    )
    SELECT
        d.transaction_day AS day,
        d.daily_case_count AS actual_cases,
        f.daily_case_count_forecast   AS forecast_cases,
        f.daily_case_count_lower      AS forecast_cases_lower,
        f.daily_case_count_upper      AS forecast_cases_upper
    FROM daily_cases d
    LEFT JOIN forecast f ON d.transaction_day = f.transaction_day
    UNION ALL
    SELECT
        f.transaction_day AS day,
        NULL AS actual_cases,
        f.daily_case_count_forecast   AS forecast_cases,
        f.daily_case_count_lower      AS forecast_cases_lower,
        f.daily_case_count_upper      AS forecast_cases_upper
    FROM forecast f
    WHERE f.transaction_day > (SELECT MAX(transaction_day) FROM daily_cases)
    ORDER BY day
    """

    rows = await delta.execute(AI_FORECAST_SQL, catalog=DELTA_CATALOG, schema=DELTA_SCHEMA, user_token=user_token)

    series = []
    for r in rows:
        series.append({
            "day": r.get("day", ""),
            "actual_cases": _to_num(r.get("actual_cases")),
            "forecast_cases": _to_num(r.get("forecast_cases")),
            "forecast_cases_lower": _to_num(r.get("forecast_cases_lower")),
            "forecast_cases_upper": _to_num(r.get("forecast_cases_upper")),
        })

    # Trim to last 30 actuals + all forecast
    actuals = [s for s in series if s["actual_cases"] is not None]
    forecasts = [s for s in series if s["actual_cases"] is None]
    trimmed = actuals[-30:] + forecasts

    future_only = [s for s in trimmed if s["actual_cases"] is None and s["forecast_cases"] is not None]
    summary = {
        "predicted_cases_30d": round(sum(s["forecast_cases"] or 0 for s in future_only)),
        "predicted_exposure_30d": 0,
        "predicted_daily_avg_cases": round(sum(s["forecast_cases"] or 0 for s in future_only) / max(len(future_only), 1), 1),
    }

    return {"series": trimmed, "summary": summary, "method": "ai_forecast"}


async def _forecast_linreg():
    """Fallback: Python linear regression from Lakebase data."""
    from datetime import date as date_cls, timedelta

    # Use 70-95 range to match analyst workload (95+ are auto-blocked)
    rows = await db.execute(f"""
        SELECT
            DATE(transaction_date) AS day,
            COUNT(*) AS case_count,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS exposure
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND fraud_score::DOUBLE PRECISION < 95
          AND transaction_date >= CURRENT_DATE - INTERVAL '60 days'
        GROUP BY DATE(transaction_date)
        ORDER BY day
    """)

    series = []
    for r in rows:
        day = r["day"] if isinstance(r["day"], str) else r["day"].isoformat()
        series.append({
            "day": day,
            "actual_cases": int(r["case_count"]),
            "forecast_cases": None,
            "forecast_cases_lower": None,
            "forecast_cases_upper": None,
        })

    if len(series) >= 7:
        n = len(series)
        xs = list(range(n))
        ys = [s["actual_cases"] for s in series]

        x_mean = sum(xs) / n
        y_mean = sum(ys) / n
        num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        den = sum((x - x_mean) ** 2 for x in xs)
        slope = num / den if den else 0
        intercept = y_mean - slope * x_mean
        residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
        std_err = (sum(r ** 2 for r in residuals) / max(n - 2, 1)) ** 0.5

        last_day_str = series[-1]["day"]
        last_date = date_cls.fromisoformat(last_day_str) if isinstance(last_day_str, str) else last_day_str

        # Bridge last actual to forecast
        series[-1]["forecast_cases"] = series[-1]["actual_cases"]

        for i in range(1, 31):
            fc = max(0, round(intercept + slope * (n - 1 + i)))
            series.append({
                "day": (last_date + timedelta(days=i)).isoformat(),
                "actual_cases": None,
                "forecast_cases": fc,
                "forecast_cases_lower": max(0, round(fc - 1.96 * std_err)),
                "forecast_cases_upper": round(fc + 1.96 * std_err),
            })

    actuals = [s for s in series if s["actual_cases"] is not None]
    forecasts = [s for s in series if s["actual_cases"] is None]
    trimmed = actuals[-30:] + forecasts

    future_only = [s for s in trimmed if s["actual_cases"] is None and s["forecast_cases"] is not None]
    summary = {
        "predicted_cases_30d": round(sum(s["forecast_cases"] or 0 for s in future_only)),
        "predicted_exposure_30d": 0,
        "predicted_daily_avg_cases": round(sum(s["forecast_cases"] or 0 for s in future_only) / max(len(future_only), 1), 1),
    }

    return {"series": trimmed, "summary": summary}


def _to_num(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── Monthly Trend with Exposure ───────────────────────────────

@router.get("/monthly-trend")
async def monthly_trend():
    """Monthly case volume, exposure, and resolution rates for 12 months."""
    rows = await db.execute(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', transaction_date), 'YYYY-MM') AS month,
            COUNT(*) AS total_cases,
            SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS closed,
            SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
            ROUND(
                100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1
            ) AS resolution_rate,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(SUM(CASE WHEN review_status = 'pending_review' THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS open_exposure
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY DATE_TRUNC('month', transaction_date)
        ORDER BY month
    """)
    return {
        "months": [
            {
                "month": r["month"],
                "total": int(r["total_cases"]),
                "closed": int(r["closed"]),
                "pending": int(r["pending"]),
                "escalated": int(r["escalated"]),
                "resolution_rate": float(r["resolution_rate"] or 0),
                "exposure": float(r["total_exposure"] or 0),
                "open_exposure": float(r["open_exposure"] or 0),
            }
            for r in rows
        ]
    }


# ── Environmental Risk Factors ────────────────────────────────

@router.get("/environmental-factors")
async def environmental_factors():
    """Environmental patterns that correlate with fraud risk.

    Analyzes fraud by day-of-week, transaction type, region, and
    risk reason to surface environmental drivers.
    """
    # Day of week patterns
    dow_rows = await db.execute(f"""
        SELECT
            TO_CHAR(transaction_date, 'Dy') AS day_name,
            EXTRACT(DOW FROM transaction_date) AS day_num,
            COUNT(*) AS total_cases,
            ROUND(AVG(fraud_score::NUMERIC), 1) AS avg_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(
                100.0 * SUM(CASE WHEN fraud_score::DOUBLE PRECISION >= 90 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1
            ) AS critical_pct
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY TO_CHAR(transaction_date, 'Dy'), EXTRACT(DOW FROM transaction_date)
        ORDER BY day_num
    """)

    # Transaction type risk profile
    type_rows = await db.execute(f"""
        SELECT
            transaction_type,
            COUNT(*) AS total_cases,
            ROUND(AVG(fraud_score::NUMERIC), 1) AS avg_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(AVG(case_exposure_usd::NUMERIC), 2) AS avg_exposure,
            ROUND(
                100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1
            ) AS escalation_rate
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY transaction_type
        ORDER BY total_exposure DESC
    """)

    # Top risk reasons (drivers)
    reason_rows = await db.execute(f"""
        SELECT
            risk_reason_engine,
            COUNT(*) AS total_cases,
            ROUND(AVG(fraud_score::NUMERIC), 1) AS avg_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(
                100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1
            ) AS escalation_rate
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND risk_reason_engine IS NOT NULL AND risk_reason_engine != ''
        GROUP BY risk_reason_engine
        ORDER BY total_cases DESC
        LIMIT 10
    """)

    # Regional risk concentration
    region_rows = await db.execute(f"""
        SELECT
            transaction_region,
            COUNT(*) AS total_cases,
            ROUND(AVG(fraud_score::NUMERIC), 1) AS avg_score,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            SUM(CASE WHEN fraud_score::DOUBLE PRECISION >= 90 THEN 1 ELSE 0 END) AS critical_cases,
            ROUND(
                100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1
            ) AS escalation_rate
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        GROUP BY transaction_region
        ORDER BY total_exposure DESC
    """)

    return {
        "day_of_week": [
            {
                "day": r["day_name"].strip(),
                "cases": int(r["total_cases"]),
                "avg_score": float(r["avg_score"] or 0),
                "exposure": float(r["total_exposure"] or 0),
                "critical_pct": float(r["critical_pct"] or 0),
            }
            for r in dow_rows
        ],
        "transaction_types": [
            {
                "type": r["transaction_type"],
                "cases": int(r["total_cases"]),
                "avg_score": float(r["avg_score"] or 0),
                "total_exposure": float(r["total_exposure"] or 0),
                "avg_exposure": float(r["avg_exposure"] or 0),
                "escalation_rate": float(r["escalation_rate"] or 0),
            }
            for r in type_rows
        ],
        "risk_reasons": [
            {
                "reason": r["risk_reason_engine"],
                "cases": int(r["total_cases"]),
                "avg_score": float(r["avg_score"] or 0),
                "exposure": float(r["total_exposure"] or 0),
                "escalation_rate": float(r["escalation_rate"] or 0),
            }
            for r in reason_rows
        ],
        "regions": [
            {
                "region": r["transaction_region"],
                "cases": int(r["total_cases"]),
                "avg_score": float(r["avg_score"] or 0),
                "exposure": float(r["total_exposure"] or 0),
                "critical_cases": int(r["critical_cases"]),
                "escalation_rate": float(r["escalation_rate"] or 0),
            }
            for r in region_rows
        ],
    }


# ── Mitigation Effectiveness ──────────────────────────────────

@router.get("/mitigation-effectiveness")
async def mitigation_effectiveness():
    """Which mitigation steps are most effective and their cost impact.

    Compares each mitigation action by:
    - Volume (how often used)
    - Resolution rate (cases successfully closed vs escalated)
    - Average exposure per case (cost profile)
    - Total exposure managed
    - Estimated cost savings (exposure on resolved cases)
    """
    # Join analyst_review (actual analyst decisions) with transactions_synced
    # (exposure/score data) to get ground-truth mitigation effectiveness.
    rows = await db.execute(f"""
        SELECT
            ar.mitigation_steps,
            COUNT(*) AS total_cases,
            SUM(CASE WHEN ar.review_status = 'reviewed' THEN 1 ELSE 0 END) AS resolved,
            SUM(CASE WHEN ar.review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
            SUM(CASE WHEN ar.review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending,
            ROUND(
                100.0 * SUM(CASE WHEN ar.review_status = 'reviewed' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN ar.review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1
            ) AS success_rate,
            ROUND(AVG(t.case_exposure_usd::NUMERIC), 2) AS avg_exposure,
            ROUND(SUM(t.case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(
                SUM(CASE WHEN ar.review_status = 'reviewed' THEN t.case_exposure_usd::NUMERIC ELSE 0 END), 2
            ) AS exposure_saved,
            ROUND(AVG(t.fraud_score::NUMERIC), 1) AS avg_score,
            SUM(CASE WHEN ar.is_fp THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN ar.is_fn THEN 1 ELSE 0 END) AS false_negatives
        FROM {PGSCHEMA}.analyst_review ar
        JOIN {PGSCHEMA}.transactions_synced t ON ar.transaction_id = t.transaction_id
        WHERE t.fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND ar.mitigation_steps IS NOT NULL AND ar.mitigation_steps != ''
          AND ar.mitigation_steps IN (
              'Account locked and customer notified.',
              'Awaiting SME response before final action.',
              'Block lifted and customer apologised.',
              'Case closed as legitimate; customer reassured.',
              'Charge reversed and SIM reissued.',
              'Credentials reset and MFA enforced.',
              'Pending external review; monitoring account activity.',
              'Risk flags cleared after verification call.'
          )
        GROUP BY ar.mitigation_steps
        ORDER BY total_cases DESC
    """)
    return {
        "mitigations": [
            {
                "step": MITIGATION_LABELS.get(r["mitigation_steps"], r["mitigation_steps"]),
                "step_full": r["mitigation_steps"],
                "total_cases": int(r["total_cases"]),
                "resolved": int(r["resolved"]),
                "escalated": int(r["escalated"]),
                "pending": int(r["pending"]),
                "success_rate": float(r["success_rate"] or 0),
                "avg_exposure": float(r["avg_exposure"] or 0),
                "total_exposure": float(r["total_exposure"] or 0),
                "exposure_saved": float(r["exposure_saved"] or 0),
                "avg_score": float(r["avg_score"] or 0),
                "false_positives": int(r["false_positives"] or 0),
                "false_negatives": int(r["false_negatives"] or 0),
            }
            for r in rows
        ]
    }


# ── Team Performance vs Benchmarks ────────────────────────────

@router.get("/team-performance")
async def team_performance(period: str = "month"):
    """Per-analyst metrics with dollar exposure and benchmarks."""
    allowed = {"day", "week", "month", "quarter"}
    if period not in allowed:
        period = "month"
    trunc_map = {"day": "day", "week": "week", "month": "month", "quarter": "quarter"}
    trunc = trunc_map[period]

    rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('{trunc}', MAX(transaction_date)) AS ref_month
            FROM {PGSCHEMA}.transactions_synced
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT
            assigned_analyst,
            COUNT(*) AS total_cases,
            SUM(CASE WHEN review_status = 'reviewed' THEN 1 ELSE 0 END) AS reviewed,
            SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) AS escalated,
            SUM(CASE WHEN review_status = 'pending_review' THEN 1 ELSE 0 END) AS pending,
            ROUND(100.0 * SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS resolution_rate,
            ROUND(100.0 * SUM(CASE WHEN review_status = 'escalated' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END), 0), 1) AS escalation_rate,
            ROUND(SUM(case_exposure_usd::NUMERIC), 2) AS total_exposure,
            ROUND(SUM(CASE WHEN review_status = 'reviewed' THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS exposure_resolved,
            ROUND(SUM(CASE WHEN review_status = 'escalated' THEN case_exposure_usd::NUMERIC ELSE 0 END), 2) AS exposure_escalated
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND fraud_score::DOUBLE PRECISION < 95
          AND transaction_date >= latest.ref_month
        GROUP BY assigned_analyst
        ORDER BY total_cases DESC
    """)
    # Data-driven benchmarks derived from 26-month historical actuals:
    #   Avg cases/analyst/month: 2.5 (P75 = 3) → target at P75
    #   Avg resolution rate: 69.5% (best month: 74.3%) → target at top-quartile
    #   Avg escalation rate: 35.5% (best month: 31.3%) → target at top-quartile
    return {
        "benchmarks": {
            "target_resolution_rate": 72.0,
            "target_escalation_rate": 33.0,
            "target_cases_per_month": 3,
        },
        "analysts": [
            {
                "analyst": r["assigned_analyst"],
                "total": int(r["total_cases"]),
                "reviewed": int(r["reviewed"]),
                "escalated": int(r["escalated"]),
                "pending": int(r["pending"]),
                "resolution_rate": float(r["resolution_rate"] or 0),
                "escalation_rate": float(r["escalation_rate"] or 0),
                "total_exposure": float(r["total_exposure"] or 0),
                "exposure_resolved": float(r["exposure_resolved"] or 0),
                "exposure_escalated": float(r["exposure_escalated"] or 0),
            }
            for r in rows
        ],
    }


# ── SLA Tracking ─────────────────────────────────────────────

@router.get("/sla-tracking")
async def sla_tracking():
    """SLA metrics: aging cases, resolution times, backlog trend."""

    # Pending case aging buckets for the latest month
    aging_rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref,
                   MAX(transaction_date) AS max_date
            FROM {PGSCHEMA}.transactions_synced
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
                  AND transaction_date < latest.max_date - INTERVAL '1 day'
            ) AS pending_over_24h,
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
                  AND transaction_date < latest.max_date - INTERVAL '3 days'
            ) AS pending_over_72h,
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
                  AND transaction_date < latest.max_date - INTERVAL '7 days'
            ) AS pending_over_7d,
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
            ) AS total_pending,
            ROUND(SUM(
                CASE WHEN review_status = 'pending_review'
                     THEN case_exposure_usd::NUMERIC ELSE 0 END
            ), 2) AS total_pending_exposure
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref
    """)

    aging = aging_rows[0] if aging_rows else {}

    # Average resolution time from decision_audit_log
    res_rows = await db.execute(f"""
        SELECT
            ROUND(AVG(
                EXTRACT(EPOCH FROM (d.decision_timestamp - t.transaction_date)) / 3600.0
            )::NUMERIC, 1) AS avg_resolution_hours
        FROM {PGSCHEMA}.decision_audit_log d
        JOIN {PGSCHEMA}.transactions_synced t ON d.transaction_id = t.transaction_id
        WHERE t.review_status IN ('reviewed', 'escalated')
    """)

    avg_resolution_hours = _to_num(
        res_rows[0]["avg_resolution_hours"] if res_rows else None
    )

    # Daily new vs resolved for last 14 days (backlog trend)
    trend_rows = await db.execute(f"""
        SELECT DATE(transaction_date) AS day,
            COUNT(*) AS new_cases,
            SUM(CASE WHEN review_status IN ('reviewed','escalated') THEN 1 ELSE 0 END) AS resolved
        FROM {PGSCHEMA}.transactions_synced
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY DATE(transaction_date)
        ORDER BY day
    """)

    backlog_trend = [
        {
            "day": r["day"] if isinstance(r["day"], str) else r["day"].isoformat(),
            "new_cases": int(r["new_cases"] or 0),
            "resolved": int(r["resolved"] or 0),
        }
        for r in trend_rows
    ]

    return {
        "pending_over_24h": int(aging.get("pending_over_24h") or 0),
        "pending_over_72h": int(aging.get("pending_over_72h") or 0),
        "pending_over_7d": int(aging.get("pending_over_7d") or 0),
        "total_pending": int(aging.get("total_pending") or 0),
        "total_pending_exposure": float(aging.get("total_pending_exposure") or 0),
        "avg_resolution_hours": avg_resolution_hours if avg_resolution_hours is not None else 0,
        "backlog_trend": backlog_trend,
    }


# ── Similar Cases ─────────────────────────────────────────────

@router.get("/similar-cases/{transaction_id}")
async def similar_cases(
    transaction_id: str = Path(..., max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$"),
):
    """Find resolved cases similar to a given transaction."""

    # Top 5 similar resolved cases (same risk reason, score within 10)
    similar_rows = await db.execute(f"""
        WITH target AS (
            SELECT risk_reason_engine, transaction_region, transaction_type,
                   fraud_score::DOUBLE PRECISION AS score
            FROM {PGSCHEMA}.transactions_synced
            WHERE transaction_id = $1
        )
        SELECT t.transaction_id, t.customer_name, t.fraud_score,
               t.case_exposure_usd, t.review_status, t.risk_reason_engine,
               t.assigned_analyst, t.mitigation_steps
        FROM {PGSCHEMA}.transactions_synced t, target tgt
        WHERE t.transaction_id != $1
          AND t.risk_reason_engine = tgt.risk_reason_engine
          AND t.fraud_score::DOUBLE PRECISION BETWEEN tgt.score - 10 AND tgt.score + 10
          AND t.review_status IN ('reviewed', 'escalated')
        ORDER BY ABS(t.fraud_score::DOUBLE PRECISION - tgt.score),
                 t.transaction_date DESC
        LIMIT 5
    """, transaction_id)

    cases = [
        {
            "transaction_id": r["transaction_id"],
            "customer_name": r["customer_name"],
            "fraud_score": _to_num(r["fraud_score"]),
            "case_exposure_usd": _to_num(r["case_exposure_usd"]),
            "review_status": r["review_status"],
            "risk_reason_engine": r["risk_reason_engine"],
            "assigned_analyst": r["assigned_analyst"],
            "mitigation_steps": r["mitigation_steps"],
        }
        for r in similar_rows
    ]

    # Distribution of resolved similar cases
    dist_rows = await db.execute(f"""
        WITH target AS (
            SELECT risk_reason_engine,
                   fraud_score::DOUBLE PRECISION AS score
            FROM {PGSCHEMA}.transactions_synced
            WHERE transaction_id = $1
        )
        SELECT review_status, COUNT(*) AS cnt
        FROM {PGSCHEMA}.transactions_synced t, target tgt
        WHERE t.risk_reason_engine = tgt.risk_reason_engine
          AND t.fraud_score::DOUBLE PRECISION BETWEEN tgt.score - 10 AND tgt.score + 10
          AND t.review_status IN ('reviewed', 'escalated')
        GROUP BY review_status
    """, transaction_id)

    distribution = {r["review_status"]: int(r["cnt"]) for r in dist_rows}
    reviewed_cnt = distribution.get("reviewed", 0)
    escalated_cnt = distribution.get("escalated", 0)
    total = reviewed_cnt + escalated_cnt

    if total > 0 and reviewed_cnt / total > 0.6:
        recommendation = "Most similar cases were cleared"
    elif total > 0 and escalated_cnt / total > 0.6:
        recommendation = "Most similar cases were escalated"
    else:
        recommendation = "Similar cases have mixed outcomes"

    return {
        "similar_cases": cases,
        "distribution": distribution,
        "recommendation": recommendation,
    }


# ── Alerts ────────────────────────────────────────────────────

@router.get("/alerts")
async def alerts():
    """Active alerts based on data-driven thresholds."""

    alert_list = []

    # Gather all alert metrics in one query for the latest month
    metrics_rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref,
                   MAX(transaction_date) AS max_date
            FROM {PGSCHEMA}.transactions_synced
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
            ) AS total_pending,
            COUNT(*) FILTER (
                WHERE review_status = 'pending_review'
                  AND transaction_date < latest.max_date - INTERVAL '3 days'
            ) AS pending_over_72h,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE is_fp = TRUE)
                / NULLIF(COUNT(*) FILTER (WHERE risk_status_engine = 'blocked'), 0),
                1
            ) AS fp_rate
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref
    """)

    m = metrics_rows[0] if metrics_rows else {}
    total_pending = int(m.get("total_pending") or 0)
    pending_over_72h = int(m.get("pending_over_72h") or 0)
    fp_rate = _to_num(m.get("fp_rate"))

    # HIGH EXPOSURE: pending cases with analyst info
    high_exp_rows = await db.execute(f"""
        WITH latest AS (
            SELECT DATE_TRUNC('month', MAX(transaction_date)) AS ref
            FROM {PGSCHEMA}.transactions_synced
            WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
        )
        SELECT transaction_id, customer_name, assigned_analyst,
               fraud_score, case_exposure_usd
        FROM {PGSCHEMA}.transactions_synced, latest
        WHERE fraud_score::DOUBLE PRECISION >= {FLAGGED_THRESHOLD}
          AND transaction_date >= latest.ref
          AND review_status = 'pending_review'
          AND case_exposure_usd::NUMERIC > 1500
        ORDER BY case_exposure_usd::NUMERIC DESC
        LIMIT 10
    """)

    for r in high_exp_rows:
        exposure = _to_num(r["case_exposure_usd"])
        analyst = r.get("assigned_analyst") or "Unassigned"
        alert_list.append({
            "severity": "critical",
            "title": "High Exposure",
            "message": f"{analyst} → {r['customer_name']} — ${exposure:,.0f}",
            "metric": exposure,
            "transaction_id": r["transaction_id"],
        })

    # BACKLOG SPIKE
    if total_pending > 250:
        alert_list.append({
            "severity": "critical",
            "title": "Backlog Spike",
            "message": f"Backlog exceeds 250 cases ({total_pending} pending)",
            "metric": total_pending,
        })

    # FP SPIKE
    if fp_rate is not None and fp_rate > 20:
        alert_list.append({
            "severity": "warning",
            "title": "False Positive Rate Elevated",
            "message": f"False positive rate is {fp_rate:.1f}% (threshold: 20%)",
            "metric": fp_rate,
        })

    # AGING CASES
    if pending_over_72h > 10:
        alert_list.append({
            "severity": "warning",
            "title": "Aging Cases",
            "message": (
                f"{pending_over_72h} cases have been pending for over 72 hours"
            ),
            "metric": pending_over_72h,
        })

    return {"alerts": alert_list}
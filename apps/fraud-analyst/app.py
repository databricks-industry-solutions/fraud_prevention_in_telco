"""
v1.1 — Fraud Analyst Workbench (Databricks App)

Full REST API + React frontend + AI assistant for case management,
backed by Lakebase via direct Postgres (asyncpg) and Foundation Model API.

Endpoints:
  GET  /api/health              — Lakebase connectivity check
  GET  /api/cases               — Paginated case queue with filters
  GET  /api/cases/filters       — Distinct values for filter dropdowns
  GET  /api/cases/{id}          — Case detail + device profile
  POST /api/cases/{id}/action   — Submit analyst decision (write-back)
  GET  /api/dashboard/stats     — Aggregate statistics
  GET  /api/dashboard/trends    — Daily time-series
  GET  /api/dashboard/geo       — Region-level aggregation
  GET  /api/dashboard/top-risk  — Top pending high-risk cases
  POST /api/chat                — AI fraud analyst assistant
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
import logging
import os

from server.db import db, PGSCHEMA
from server.routes.cases import router as cases_router
from server.routes.dashboard import router as dashboard_router
from server.routes.chat import router as chat_router


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Startup / shutdown lifecycle hook."""
    yield
    # Graceful shutdown: close asyncpg pool and aiohttp sessions
    if db._pool:
        await db._pool.close()
    if db._http and not db._http.closed:
        await db._http.close()


logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="Fraud Analyst Workbench",
    version="1.1",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],  # Same-origin only; no cross-origin allowed
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# ── Register API routers ─────────────────────────────────────

app.include_router(cases_router)
app.include_router(dashboard_router)
app.include_router(chat_router)


# ── Health & status ──────────────────────────────────────────

@app.get("/api/health")
async def health():
    """Verify Lakebase connectivity and return table stats."""
    try:
        await db.fetchval(
            f"SELECT 1 FROM {PGSCHEMA}.transaction_risk LIMIT 1"
        )
        return {"status": "ok"}
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={"status": "error"},
        )


# ── Serve React frontend (built static files) ───────────────

frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dir):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_dir, "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dir, "index.html"))
else:

    @app.get("/")
    async def root():
        return {"status": "ok", "version": "1.1", "app": "Fraud Analyst Workbench"}

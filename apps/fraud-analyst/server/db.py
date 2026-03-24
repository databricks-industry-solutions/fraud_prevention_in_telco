"""Lakebase direct Postgres connection — asyncpg for sub-200ms queries.

Uses asyncpg wire protocol for all read queries against Lakebase-synced tables.
Uses SQL Statement API only for write-back to the Delta catalog (since
Lakebase synced tables are read-only).

Pool auto-reconnects on failure and refreshes credentials periodically.
"""

import os
import time
import logging
import aiohttp
import asyncpg
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from .config import get_oauth_token, get_workspace_host, IS_DATABRICKS_APP

logger = logging.getLogger(__name__)

# Lakebase Postgres connection details
PGHOST = os.environ.get("PGHOST", "")
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGSCHEMA = os.environ.get("PGSCHEMA", "fraud_data")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "")
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "")

# SQL Statement API for Delta write-back
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")

# Token / credential refresh interval (45 min)
_CREDENTIAL_TTL = 45 * 60


async def _fetch_pg_password_async(session: aiohttp.ClientSession) -> str:
    """Fetch PG credential via the Database Credential API (async)."""
    host = get_workspace_host()
    token = get_oauth_token()
    async with session.post(
        f"{host}/api/2.0/database/credentials",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={
            "request_id": f"app-{int(time.time())}",
            "instance_names": [LAKEBASE_INSTANCE],
        },
    ) as resp:
        data = await resp.json()
    return data["token"]


def _fetch_secret(scope: str, key: str) -> str:
    """Fetch a secret from Databricks Secrets at runtime."""
    from .config import get_workspace_client
    client = get_workspace_client()
    import base64
    resp = client.secrets.get_secret(scope=scope, key=key)
    return base64.b64decode(resp.value).decode("utf-8")


class LakebasePool:
    """Direct asyncpg connection pool to Lakebase Postgres.

    Features:
    - Parameterised queries to prevent SQL injection
    - Auto-reconnect on pool/connection failure
    - Credential refresh every 45 minutes (local dev)
    """

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_created_at: float = 0
        self._http: Optional[aiohttp.ClientSession] = None

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession()
        return self._http

    async def _resolve_credentials(self) -> tuple[str, str]:
        """Return (user, password) for the current environment."""
        if IS_DATABRICKS_APP:
            user = PGUSER
            # 1. Try Databricks Secrets (secure, preferred for native PG login)
            secret_scope = os.environ.get("PGPASSWORD_SECRET_SCOPE", "")
            secret_key = os.environ.get("PGPASSWORD_SECRET_KEY", "")
            if secret_scope and secret_key:
                try:
                    password = _fetch_secret(secret_scope, secret_key)
                    logger.info("Using secret scope for Lakebase auth (user=%s)", user)
                    return user, password
                except Exception as e:
                    logger.warning("Secret scope failed (%s), trying other methods", e)
            # 2. Fallback: PGPASSWORD env var (for local testing only)
            pg_password = os.environ.get("PGPASSWORD", "")
            if pg_password:
                logger.info("Using PGPASSWORD env var for Lakebase auth (user=%s)", user)
                return user, pg_password
            # 3. Fallback: SP OAuth token
            try:
                password = get_oauth_token()
                logger.info("Using SP OAuth token for Lakebase auth")
            except Exception as e:
                logger.warning("OAuth token failed (%s), trying Credential API", e)
                session = await self._get_http()
                try:
                    password = await _fetch_pg_password_async(session)
                except Exception:
                    raise RuntimeError("All Lakebase auth methods failed. Configure PGPASSWORD_SECRET_SCOPE/KEY in app.yaml.")
            return user, password

        # Local dev — use Credential API (token valid ~1 hour)
        from .config import get_workspace_client
        client = get_workspace_client()
        user = client.current_user.me().user_name
        session = await self._get_http()
        password = await _fetch_pg_password_async(session)
        return user, password

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Create or refresh the connection pool."""
        now = time.time()
        needs_refresh = (
            self._pool is None
            or (now - self._pool_created_at) > _CREDENTIAL_TTL
        )

        if needs_refresh:
            if self._pool:
                try:
                    await self._pool.close()
                except Exception:
                    pass
                self._pool = None

            user, password = await self._resolve_credentials()
            self._pool = await asyncpg.create_pool(
                host=PGHOST,
                port=PGPORT,
                database=PGDATABASE,
                user=user,
                password=password,
                ssl="require",
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            self._pool_created_at = now

        return self._pool

    async def _reset_pool(self):
        """Destroy the pool so the next call rebuilds it."""
        if self._pool:
            try:
                await self._pool.close()
            except Exception:
                pass
        self._pool = None

    @staticmethod
    def _serialize_row(row: asyncpg.Record) -> dict:
        """Convert asyncpg Record to a JSON-safe dict."""
        result = {}
        for key, val in row.items():
            if val is None:
                result[key] = None
            elif isinstance(val, (datetime, date)):
                result[key] = val.isoformat()
            elif isinstance(val, Decimal):
                result[key] = str(val)
            elif isinstance(val, (int, float, bool, str)):
                result[key] = val
            else:
                result[key] = str(val)
        return result

    async def execute(self, sql: str, *args) -> list[dict]:
        """Execute a parameterised read query and return rows as dicts.

        Use $1, $2, … placeholders and pass values as positional args:
            await db.execute("SELECT * FROM t WHERE id = $1", some_id)
        """
        try:
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *args)
                return [self._serialize_row(row) for row in rows]
        except (asyncpg.PostgresConnectionError, OSError) as exc:
            logger.warning("Lakebase connection error, resetting pool: %s", exc)
            await self._reset_pool()
            # Retry once with a fresh pool
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *args)
                return [self._serialize_row(row) for row in rows]

    async def execute_write(self, sql: str, *args) -> str:
        """Execute a parameterised write query (INSERT/UPDATE) via Lakebase.

        Returns the command tag (e.g. 'INSERT 0 1', 'UPDATE 1').
        """
        try:
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                return await conn.execute(sql, *args)
        except (asyncpg.PostgresConnectionError, OSError) as exc:
            logger.warning("Lakebase connection error on write, resetting pool: %s", exc)
            await self._reset_pool()
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                return await conn.execute(sql, *args)

    async def fetchval(self, sql: str, *args) -> Optional[str]:
        """Execute parameterised query and return first value of first row."""
        try:
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                val = await conn.fetchval(sql, *args)
                return str(val) if val is not None else None
        except (asyncpg.PostgresConnectionError, OSError) as exc:
            logger.warning("Lakebase connection error, resetting pool: %s", exc)
            await self._reset_pool()
            pool = await self._ensure_pool()
            async with pool.acquire() as conn:
                val = await conn.fetchval(sql, *args)
                return str(val) if val is not None else None


class DeltaWriter:
    """SQL Statement API client for write-back to Delta catalog."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def execute(
        self, sql: str, catalog: str, schema: str, user_token: Optional[str] = None,
    ) -> list[dict]:
        """Execute a query via SQL Statement API.

        Args:
            user_token: If provided, use this token instead of the SP token.
                        Enables user-passthrough for catalogs the SP can't access.
        """
        host = get_workspace_host()
        token = user_token or get_oauth_token()
        session = await self._get_session()

        async with session.post(
            f"{host}/api/2.0/sql/statements",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "warehouse_id": WAREHOUSE_ID,
                "catalog": catalog,
                "schema": schema,
                "statement": sql,
                "wait_timeout": "45s",
            },
        ) as resp:
            data = await resp.json()

        status = data.get("status", {})
        if status.get("state") != "SUCCEEDED":
            error_msg = status.get("error", {}).get("message", "Unknown error")
            raise RuntimeError(f"SQL Statement API error: {error_msg}")

        columns = [
            col["name"]
            for col in data.get("manifest", {}).get("schema", {}).get("columns", [])
        ]
        result_rows = data.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row)) for row in result_rows]


# Singleton instances
db = LakebasePool()
delta = DeltaWriter()

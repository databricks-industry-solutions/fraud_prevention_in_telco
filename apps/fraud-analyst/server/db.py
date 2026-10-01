"""Lakebase direct Postgres connection — asyncpg for sub-200ms queries.

Uses asyncpg wire protocol for all read queries against Lakebase-synced tables.
Uses SQL Statement API only for write-back to the Delta catalog (since
Lakebase synced tables are read-only).

Pool auto-reconnects on failure and refreshes credentials periodically.
"""

import os
import asyncio
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
LAKEBASE_PROJECT_ID = os.environ.get("LAKEBASE_PROJECT_ID", "")
LAKEBASE_BRANCH_ID = os.environ.get("LAKEBASE_BRANCH_ID", "production")
LAKEBASE_ENDPOINT_ID = os.environ.get("LAKEBASE_ENDPOINT_ID", "primary")
LAKEBASE_PG_VERSION = int(os.environ.get("LAKEBASE_PG_VERSION", "17"))

# Default compute sizing for auto-created Lakebase endpoints (autoscaling CU range).
# These values should generally match your project defaults; override via env if needed later.
LAKEBASE_AUTOSCALING_MIN_CU = float(os.environ.get("LAKEBASE_AUTOSCALING_MIN_CU", "2"))
LAKEBASE_AUTOSCALING_MAX_CU = float(os.environ.get("LAKEBASE_AUTOSCALING_MAX_CU", "4"))

# SQL Statement API for Delta write-back
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")

# Token / credential refresh interval (45 min)
_CREDENTIAL_TTL = 45 * 60


_lakebase_bootstrap_lock = asyncio.Lock()
_lakebase_bootstrapped = False


def _target_endpoint_resource_name() -> str:
    """
    Return the endpoint resource name expected by:
      - credential API: instance_names=[...]
    """
    if LAKEBASE_INSTANCE.startswith("projects/"):
        return LAKEBASE_INSTANCE
    if LAKEBASE_PROJECT_ID:
        return f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH_ID}/endpoints/{LAKEBASE_ENDPOINT_ID}"
    return ""


def _parse_endpoint_resource_name(endpoint: str) -> tuple[str, str, str]:
    # Expected format:
    # projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}
    parts = endpoint.strip("/").split("/")
    if len(parts) != 6 or parts[0] != "projects" or parts[2] != "branches" or parts[4] != "endpoints":
        raise ValueError(f"Unexpected endpoint resource name: {endpoint}")
    project_id = parts[1]
    branch_id = parts[3]
    endpoint_id = parts[5]
    return project_id, branch_id, endpoint_id


def _ensure_lakebase_sync(endpoint_resource_name: str) -> tuple[str, str]:
    """
    Synchronous Lakebase autoscaling bootstrap.

    Ensures:
      - project exists
      - branch exists
      - endpoint exists

    Returns:
      (endpoint_host, endpoint_resource_name)
    """
    # Local import to avoid pulling Lakebase SDK types during module import in local dev.
    from databricks.sdk.service.postgres import (
        Branch,
        BranchSpec,
        Endpoint,
        EndpointSpec,
        EndpointType,
        Project,
        ProjectDefaultEndpointSettings,
        ProjectSpec,
    )

    from .config import get_workspace_client

    w = get_workspace_client()
    project_id, branch_id, endpoint_id = _parse_endpoint_resource_name(endpoint_resource_name)
    project_name = f"projects/{project_id}"
    branch_name = f"projects/{project_id}/branches/{branch_id}"

    # 1) Ensure project exists
    try:
        w.postgres.get_project(project_name)
    except Exception:
        # When project doesn't exist, create a new one with default settings.
        proj_spec = ProjectSpec(
            display_name=project_id,
            pg_version=LAKEBASE_PG_VERSION,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=LAKEBASE_AUTOSCALING_MIN_CU,
                autoscaling_limit_max_cu=LAKEBASE_AUTOSCALING_MAX_CU,
            ),
        )
        project = Project(spec=proj_spec)
        operation = w.postgres.create_project(project=project, project_id=project_id)
        operation.wait()

    # 2) Ensure branch exists
    try:
        w.postgres.get_branch(branch_name)
    except Exception:
        branch_spec = BranchSpec(no_expiry=True)
        operation = w.postgres.create_branch(
            parent=project_name,
            branch=Branch(spec=branch_spec),
            branch_id=branch_id,
        )
        operation.wait()

    # 3) Ensure endpoint exists
    try:
        endpoint = w.postgres.get_endpoint(endpoint_resource_name)
    except Exception:
        endpoint_spec = EndpointSpec(
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=LAKEBASE_AUTOSCALING_MIN_CU,
            autoscaling_limit_max_cu=LAKEBASE_AUTOSCALING_MAX_CU,
        )
        operation = w.postgres.create_endpoint(
            parent=branch_name,
            endpoint=Endpoint(spec=endpoint_spec),
            endpoint_id=endpoint_id,
        )
        operation.wait()
        endpoint = w.postgres.get_endpoint(endpoint_resource_name)

    # 4) Wait for endpoint to become connectable
    # Endpoint transitions can take a bit; we poll briefly to reduce first-request failures.
    for _ in range(30):
        endpoint = w.postgres.get_endpoint(endpoint_resource_name)
        state = None
        if getattr(endpoint, "status", None) and getattr(endpoint.status, "current_state", None):
            state = endpoint.status.current_state
        if state in ("ACTIVE", "IDLE"):
            break
        time.sleep(2)

    endpoint_host = None
    hosts = getattr(getattr(endpoint, "status", None), "hosts", None)
    if hosts is not None:
        # `hosts` may be a single host object or a list of them depending on SDK version.
        if isinstance(hosts, (list, tuple)):
            endpoint_host = getattr(hosts[0], "host", None) if hosts else None
        else:
            endpoint_host = getattr(hosts, "host", None)
    if not endpoint_host:
        raise RuntimeError(f"Lakebase endpoint host not found for {endpoint_resource_name}")

    return endpoint_host, endpoint_resource_name


async def ensure_lakebase_postgres() -> None:
    """
    Ensure Lakebase autoscaling project/endpoint exists and derive connection info.

    This is intended to remove the “manual” Lakebase instance prerequisite:
    if the target project/endpoint is missing, the app will create it at startup.
    """
    global PGHOST, LAKEBASE_INSTANCE, _lakebase_bootstrapped

    if not IS_DATABRICKS_APP:
        # Local dev uses the credential API directly; do not attempt to create infrastructure.
        return

    endpoint_resource_name = _target_endpoint_resource_name()
    if not endpoint_resource_name:
        logger.warning(
            "Lakebase auto-bootstrap skipped: set LAKEBASE_INSTANCE (projects/...) or LAKEBASE_PROJECT_ID."
        )
        return

    async with _lakebase_bootstrap_lock:
        if _lakebase_bootstrapped:
            return

        endpoint_host, resolved_endpoint_resource_name = await asyncio.to_thread(
            _ensure_lakebase_sync, endpoint_resource_name
        )

        PGHOST = endpoint_host
        LAKEBASE_INSTANCE = resolved_endpoint_resource_name
        _lakebase_bootstrapped = True


async def _fetch_pg_password_async(session: aiohttp.ClientSession) -> str:
    """
    Fetch PG credential (password) token for asyncpg.

    Preferred: Databricks SDK Lakebase credential generator (stable shape).
    Fallback: `/api/2.0/database/credentials` + token field detection.
    """

    endpoint_resource_name = _target_endpoint_resource_name() or LAKEBASE_INSTANCE
    if not endpoint_resource_name:
        # Provisioned Lakebase via Apps database resource: use instance name from app resource
        endpoint_resource_name = os.environ.get("LAKEBASE_DB_INSTANCE_NAME", "fraud-analyst-db")

    # 1) SDK path (preferred)
    try:
        from .config import get_workspace_client

        client = get_workspace_client()
        cred = client.postgres.generate_database_credential(endpoint_resource_name)
        token = getattr(cred, "token", None)
        if token:
            return token
        if isinstance(cred, dict):
            token = cred.get("token")
            if token:
                return token
    except Exception as e:
        logger.warning("Lakebase credential generation via SDK failed, falling back to REST: %s", e)

    # 2) REST fallback
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
            "instance_names": [endpoint_resource_name],
        },
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            text = await resp.text()
            raise RuntimeError(f"Lakebase credential API returned non-JSON: status={resp.status}, body={text[:500]}")

    if resp.status != 200:
        def _redact(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    if "token" in k.lower():
                        out[k] = "<redacted>"
                    else:
                        out[k] = _redact(v)
                return out
            if isinstance(obj, list):
                return [_redact(x) for x in obj]
            return obj

        raise RuntimeError(
            f"Lakebase credential API failed: status={resp.status}, body={_redact(data) if isinstance(data, (dict, list)) else str(data)[:500]}"
        )

    def _find_token(obj):
        if isinstance(obj, dict):
            if obj.get("token"):
                return obj["token"]
            for v in obj.values():
                t = _find_token(v)
                if t:
                    return t
        elif isinstance(obj, list):
            for item in obj:
                t = _find_token(item)
                if t:
                    return t
        return None

    resolved = _find_token(data)
    if not resolved:
        keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
        raise RuntimeError(f"Lakebase credential API response missing token. keys={keys}")

    return resolved


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

            # 1. Databricks Secrets scope (if configured) — native PG login.
            #    Preferred when you manage the Lakebase password yourself.
            secret_scope = os.environ.get("PGPASSWORD_SECRET_SCOPE", "")
            secret_key = os.environ.get("PGPASSWORD_SECRET_KEY", "")
            if secret_scope and secret_key:
                if user:
                    try:
                        password = _fetch_secret(secret_scope, secret_key)
                        logger.info("Using secret scope for Lakebase auth (user=%s)", user)
                        return user, password
                    except Exception as e:
                        logger.warning("Secret scope auth failed (%s), trying credential API", e)
                else:
                    logger.warning(
                        "PGPASSWORD_SECRET_SCOPE/KEY set but PGUSER is empty; "
                        "skipping secret-scope auth and using the Credential API"
                    )

            # 2. PGPASSWORD env var (mainly for local/testing).
            pg_password = os.environ.get("PGPASSWORD", "")
            if pg_password and user:
                logger.info("Using PGPASSWORD env var for Lakebase auth (user=%s)", user)
                return user, pg_password

            # 3. Lakebase Database Credential API (SDK -> REST fallback).
            #    The returned token is the password AND carries the Postgres
            #    username in its `sub` claim, so PGUSER need not be set manually.
            session = await self._get_http()
            try:
                password = await _fetch_pg_password_async(session)
            except Exception as e:
                # Last-ditch: SP OAuth token as the PG password (legacy path).
                logger.warning("Credential API failed (%s); falling back to SP OAuth token", e)
                password = get_oauth_token()
            try:
                import base64
                import json

                # JWT format: header.payload.signature
                payload_b64 = password.split(".")[1]
                # Pad base64 if needed
                payload_b64 += "=" * (-len(payload_b64) % 4)
                payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
                user = payload.get("sub") or user
            except Exception:
                pass

            if not user:
                raise RuntimeError(
                    "Lakebase auth failed: could not determine PGUSER. Set PGUSER, "
                    "or configure PGPASSWORD_SECRET_SCOPE/PGPASSWORD_SECRET_KEY."
                )
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
        # If PGHOST/LAKEBASE_INSTANCE are blank, try to bootstrap Lakebase on demand.
        if (not PGHOST or not LAKEBASE_INSTANCE) and IS_DATABRICKS_APP:
            await ensure_lakebase_postgres()

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
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        async with session.post(
            f"{host}/api/2.0/sql/statements",
            headers=headers,
            json={
                "warehouse_id": WAREHOUSE_ID,
                "catalog": catalog,
                "schema": schema,
                "statement": sql,
                "wait_timeout": "50s",
            },
        ) as resp:
            http_status = resp.status
            data = await resp.json()

        if http_status != 200:
            logger.error(
                "SQL Statement API POST %s (warehouse=%s catalog=%s): %s",
                http_status, WAREHOUSE_ID, catalog, str(data)[:800],
            )
            raise RuntimeError(f"SQL Statement API HTTP {http_status}: {str(data)[:400]}")

        status = data.get("status", {})
        state = status.get("state")

        # Poll while the statement is still running (a cold serverless warehouse
        # can take well over the initial wait window, e.g. ai_forecast). The
        # first response returns a statement_id we can poll until it terminates.
        statement_id = data.get("statement_id")
        poll_deadline = time.time() + 180  # cap total wait at 3 min
        while state in ("PENDING", "RUNNING") and statement_id and time.time() < poll_deadline:
            await asyncio.sleep(2)
            async with session.get(
                f"{host}/api/2.0/sql/statements/{statement_id}",
                headers=headers,
            ) as poll_resp:
                data = await poll_resp.json()
            status = data.get("status", {})
            state = status.get("state")

        if state != "SUCCEEDED":
            error_msg = status.get("error", {}).get("message", state or "Unknown error")
            logger.error("SQL Statement API non-success (state=%s): %s", state, str(data)[:800])
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
